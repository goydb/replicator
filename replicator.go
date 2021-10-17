package replicator

import (
	"context"
	"errors"
	"fmt"

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

	sourceLastSeq string
	diffResp      client.DiffResponse

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
	err = r.LocateChangedDocuments(ctx)
	if err != nil {
		return r.logErrf("locate changed documents failed: %w", err)
	}

	r.logger.Debug("ReplicateChanges")
	err = r.ReplicateChanges(ctx)
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

	// Get Replication Log from Source
	sourceRepLog, err := r.source.GetReplicationLog(ctx, id)
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		return err
	}

	// Get Replication Log from Target
	targetRepLog, err := r.target.GetReplicationLog(ctx, id)
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		return err
	}

	// Compare Replication Logs
	err = r.CompareReplicationLogs(ctx, sourceRepLog, targetRepLog)
	if err != nil {
		return err
	}

	return nil
}

// Locate Changed Documents
// https://docs.couchdb.org/en/stable/replication/protocol.html#locate-changed-documents
func (r *Replicator) LocateChangedDocuments(ctx context.Context) error {
start:
	// Listen to Changes Feed
	changes, err := r.source.Changes(ctx, client.ChangeOptions{
		Since:     r.sourceLastSeq,
		Heartbeat: r.job.HeartbeatOrFallback(),
	})
	if err != nil {
		return err
	}

	// No more changes
	r.logger.Debugf("Changes: %d", len(changes.Results))
	if len(changes.Results) == 0 {
		if r.job.Continuous {
			goto start
		} else {
			return ErrReplicationCompleted // Replication Completed
		}
	}

	// Read Batch of Changes
	diff := make(client.RevDiffRequest)
	for _, change := range changes.Results {
		for _, rev := range change.Changes {
			diff[change.ID] = append(diff[change.ID], rev.Rev)
		}
	}

	// Compare Documents Revisions
	diffResp, err := r.target.RevDiff(ctx, diff)
	if err != nil {
		return err
	}

	// Any Differences Found?
	r.logger.Debugf("Differences: %d", len(diffResp))
	if len(diffResp) == 0 { // No
		goto start
	}

	r.diffResp = diffResp
	return nil
}

// ReplicateChanges
// https://docs.couchdb.org/en/stable/replication/protocol.html#replicate-changes
func (r *Replicator) ReplicateChanges(ctx context.Context) error {
	for docid := range r.diffResp {
		// Fetch Next Changed Document
		err := r.source.GetDocumentComplete(ctx, docid)
		if err != nil {
			return err
		}

		// Document Has Changed Attachments?
		// WIP point
	}

	return nil
}

// 2.4.2.3.3. Compare Replication Logs
func (r *Replicator) CompareReplicationLogs(ctx context.Context, source, target *client.ReplicationLog) error {

	// 	If the Replication Logs are successfully retrieved from both Source and Target then the Replicator MUST determine their common ancestry by following the next algorithm:
	if source == nil || target == nil {
		r.sourceLastSeq = "0"
		return nil
	}

	//     Compare session_id values for the chronological last session - if they match both Source and Target have a common Replication history and it seems to be valid. Use 	source_last_seq value for the startup Checkpoint
	if source.SessionID == target.SessionID {
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
	r.sourceLastSeq = "0"
	return nil
}
