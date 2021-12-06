package replicator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/goydb/replicator/client"
	"github.com/goydb/replicator/logger"
)

var (
	ErrAbort                = errors.New("abort replication")
	ErrReplicationCompleted = errors.New("replication completed")
)

// Replicator implements the couchdb replication protocol:
// https://docs.couchdb.org/en/stable/replication/protocol.html
type Replicator struct {
	// name used to generate the replication id
	name string

	job    *Job
	source *client.Client
	target *client.Client

	sourceInfo, targetInfo *client.Info

	replicationID string

	sourceLastSeq string
	diffResp      client.DiffResponse

	sourceRepLog, targetRepLog *client.ReplicationLog
	currentHistory             *client.History

	logger logger.Logger
}

func NewReplicator(name string, job *Job) (*Replicator, error) {
	source, err := client.NewClient(job.Source)
	if err != nil {
		return nil, err
	}

	target, err := client.NewClient(job.Target)
	if err != nil {
		return nil, err
	}

	return &Replicator{
		name:   name,
		job:    job,
		logger: new(logger.Noop),
		source: source,
		target: target,
	}, nil
}

func (r *Replicator) SetLogger(logger logger.Logger) {
	r.logger = logger
	r.source.SetLogger(logger)
	r.target.SetLogger(logger)
}

func (t *Replicator) logErrf(format string, args ...interface{}) error {
	e := fmt.Errorf(format, args...)
	t.logger.Error(e.Error())
	return e
}

func (r *Replicator) Run(ctx context.Context) error {
	r.logger.Debug("VerifyPeers")
	err := r.VerifyPeers(ctx)
	if err != nil {
		return r.logErrf("verify peers failed: %w", err)
	}

	r.logger.Debug("GetPeersInformation")
	err = r.GetPeersInformation(ctx)
	if err != nil {
		return r.logErrf("get peers information failed: %w", err)
	}

	r.logger.Debug("FindCommonAncestry")
	err = r.FindCommonAncestry(ctx)
	if err != nil {
		return r.logErrf("find common ancestry failed: %w", err)
	}
	r.logger.Debugf("Replication will start since: %s", r.sourceLastSeq)

	r.logger.Debug("LocateChangedDocuments")
	lastSeq, err := r.LocateChangedDocuments(ctx)
	if err != nil {
		return r.logErrf("locate changed documents failed: %w", err)
	}

	r.logger.Debugf("ReplicateChanges (lastSeq: %q)", lastSeq)
	err = r.ReplicateChanges(ctx, lastSeq)
	if err != nil {
		return r.logErrf("replicate changes failed: %w", err)
	}

	return nil
}

// VerifyPeers
// https://docs.couchdb.org/en/stable/replication/protocol.html#verify-peers
func (r *Replicator) VerifyPeers(ctx context.Context) error {
	// Check Source Existence
	err := r.source.Check(ctx)
	if err != nil {
		return err
	}

	// Check Target Existence
	err = r.target.Check(ctx)
	if err == nil { // 200 OK
		return nil
	}

	// only 404 Not Found can be handled
	if !errors.Is(err, client.ErrNotFound) {
		return err
	}

	// Create Target?
	if !r.job.CreateTarget {
		return err
	}

	// Create Target
	return r.target.Create(ctx)
}

// GetPeersInformation
// https://docs.couchdb.org/en/stable/replication/protocol.html#get-peers-information
func (r *Replicator) GetPeersInformation(ctx context.Context) error {
	var err error

	//  Get Source Information
	r.sourceInfo, err = r.source.Info(ctx)
	if err != nil {
		return err
	}

	// Get Target Information
	r.targetInfo, err = r.target.Info(ctx)
	if err != nil {
		return err
	}

	return nil
}

// FindCommonAncestry
// https://docs.couchdb.org/en/stable/replication/protocol.html#find-common-ancestry
func (r *Replicator) FindCommonAncestry(ctx context.Context) error {
	// Generate Replication ID
	id := r.job.GenerateReplicationID(r.name)
	r.logger.Debugf("Replication ID %q", id)
	r.replicationID = id

	// Get Replication Log from Source
	sourceRepLog, err := r.source.GetReplicationLog(ctx, id)
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		return err
	}
	if sourceRepLog == nil {
		sourceRepLog = new(client.ReplicationLog)
	}

	// Get Replication Log from Target
	targetRepLog, err := r.target.GetReplicationLog(ctx, id)
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		return err
	}
	if targetRepLog == nil {
		targetRepLog = new(client.ReplicationLog)
	}

	// Compare Replication Logs
	err = r.CompareReplicationLogs(ctx, sourceRepLog, targetRepLog)
	if err != nil {
		return err
	}

	r.sourceRepLog = sourceRepLog
	r.targetRepLog = targetRepLog
	r.currentHistory = &client.History{
		StartTime:    client.Time(time.Now()),
		StartLastSeq: r.sourceLastSeq,
		SessionID:    id,
	}

	return nil
}

// Locate Changed Documents
// https://docs.couchdb.org/en/stable/replication/protocol.html#locate-changed-documents
func (r *Replicator) LocateChangedDocuments(ctx context.Context) (string, error) {
start:
	time.Sleep(time.Second)

	// Listen to Changes Feed
	changes, err := r.source.Changes(ctx, client.ChangeOptions{
		Since:     r.sourceLastSeq,
		Heartbeat: r.job.HeartbeatOrFallback(),
	})
	if err != nil {
		return "", err
	}

	// No more changes
	r.logger.Debugf("Changes: %d", len(changes.Results))
	if len(changes.Results) == 0 {
		if r.job.Continuous {
			goto start
		} else {
			return "", ErrReplicationCompleted // Replication Completed
		}
	}

	// Read Batch of Changes
	diff := make(client.RevDiffRequest)
	for _, change := range changes.Results {
		for _, rev := range change.Changes {
			diff[change.ID] = append(diff[change.ID], rev.Rev)
		}
	}
	r.currentHistory.MissingFound += len(diff)

	// Compare Documents Revisions
	diffResp, err := r.target.RevDiff(ctx, diff)
	if err != nil {
		return "", err
	}
	r.currentHistory.MissingChecked += len(diffResp)

	// Any Differences Found?
	r.logger.Debugf("Differences: %d", len(diffResp))
	if len(diffResp) == 0 { // No
		goto start
	}

	r.diffResp = diffResp
	return changes.LastSeq, nil
}

// MB10 10 MB
const MB10 = 10 * (1024 ^ 2)

// ReplicateChanges
// https://docs.couchdb.org/en/stable/replication/protocol.html#replicate-changes
func (r *Replicator) ReplicateChanges(ctx context.Context, lastSeq string) error {
	var stack client.Stack

	for docID, diff := range r.diffResp {
		// Fetch Next Changed Document
		doc, err := r.source.GetDocumentComplete(ctx, docID, diff)
		if err != nil {
			return err
		}
		r.currentHistory.DocsRead++
		r.logger.Debugf("Document size: %d has attachments: %v revision: %q", doc.Size(), doc.HasChangedAttachments(), doc.Data["_rev"])

		// Document Has Changed Attachments?
		if doc.HasChangedAttachments() {
			// Are They Big Enough?
			if doc.Size() > MB10 {
				// Update Document on Target
				err := r.target.UploadDocumentWithAttachments(ctx, doc)
				if err != nil {
					r.currentHistory.DocWriteFailures++
					return err
				}
				r.currentHistory.DocsWritten++
			} else {
				err := doc.InlineAttachments()
				if err != nil {
					return err
				}

				// Put Document Into the Stack
				stack = append(stack, doc)
			}
		}

		// Stack is Full?
		if stack.Size() > MB10 {
			err := r.replicateChangesBulk(ctx, stack)
			if err != nil {
				return err
			}
		}
	}

	// stack too small but changes available? push rest
	if len(stack) > 0 {
		err := r.replicateChangesBulk(ctx, stack)
		if err != nil {
			return err
		}
	}

	r.currentHistory.SessionID = r.replicationID
	r.currentHistory.EndLastSeq = lastSeq
	r.currentHistory.EndTime = client.Time(time.Now())

	if r.currentHistory.DocsWritten > 0 {
		err := r.recordReplicationCheckpoint(ctx, r.sourceRepLog, lastSeq)
		if err != nil {
			return err
		}
		err = r.recordReplicationCheckpoint(ctx, r.targetRepLog, lastSeq)
		if err != nil {
			return err
		}
	}

	r.currentHistory = nil

	return nil
}

func (r *Replicator) replicateChangesBulk(ctx context.Context, stack client.Stack) error {
	// Upload Stack of Documents to Target
	err := r.target.BulkDocs(ctx, &stack)
	if err != nil {
		r.currentHistory.DocWriteFailures += len(stack)
		return err
	}
	r.currentHistory.DocsWritten += len(stack)

	// Ensure in Commit
	err = r.target.EnsureFullCommit(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (r *Replicator) recordReplicationCheckpoint(ctx context.Context, repLog *client.ReplicationLog, lastSeq string) error {
	repLog.ID = r.replicationID
	repLog.ReplicationIDVersion = 3
	repLog.SessionID = r.replicationID
	repLog.SourceLastSeq = lastSeq
	repLog.History = append(r.targetRepLog.History, r.currentHistory)

	// Record Replication Checkpoint
	err := r.source.RecordReplicationCheckpoint(ctx, repLog, r.replicationID)
	if err != nil {
		return err
	}

	return nil
}

const NoVersion = "0"

// 2.4.2.3.3. Compare Replication Logs
func (r *Replicator) CompareReplicationLogs(ctx context.Context, source, target *client.ReplicationLog) error {
	// 	If the Replication Logs are successfully retrieved from both Source and Target then the Replicator MUST determine their common ancestry by following the next algorithm:
	if source == nil || target == nil {
		r.sourceLastSeq = NoVersion
		return nil
	}

	//     Compare session_id values for the chronological last session - if they match both Source and Target have a common Replication history and it seems to be valid. Use 	source_last_seq value for the startup Checkpoint
	if source.SessionID == target.SessionID && source.SourceLastSeq != "" {
		r.sourceLastSeq = source.SourceLastSeq
		return nil
	}

	//     In case of mismatch, iterate over the history collection to search for the latest (chronologically) common session_id for Source and Target. Use value of recorded_seq field as startup Checkpoint
	for _, sl := range source.History {
		for _, tl := range target.History {
			if sl.SessionID == tl.SessionID {
				r.sourceLastSeq = sl.RecordedSeq
				return nil
			}
		}
	}

	// If Source and Target has no common ancestry, the Replicator MUST run Full Replication.
	r.sourceLastSeq = NoVersion

	return nil
}
