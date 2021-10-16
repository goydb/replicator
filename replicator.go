package replicator

import (
	"context"
	"errors"
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
	source *Client
	target *Client

	sourceInfo, targetInfo *Info

	sourceLastSeq string
	diffResp      DiffResponse
}

func NewReplicator(name string, job *Job) *Replicator {
	return &Replicator{
		name: name,
		job:  job,
	}
}

func (r *Replicator) Run(ctx context.Context) error {
	err := r.VerifyPeers(ctx)
	if err != nil {
		return err
	}

	err = r.GetPeersInformation(ctx)
	if err != nil {
		return err
	}

	err = r.FindCommonAncestry(ctx)
	if err != nil {
		return err
	}

	err = r.LocateChangedDocuments(ctx)
	if err != nil {
		return err
	}

	err = r.ReplicateChanges(ctx)
	if err != nil {
		return err
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
	if !errors.Is(err, ErrNotFound) {
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

	// Get Replication Log from Source
	sourceRepLog, err := r.source.GetReplicationLog(ctx, id)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}

	// Get Replication Log from Target
	targetRepLog, err := r.target.GetReplicationLog(ctx, id)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}

	// Compare Replication Logs
	r.CompareReplicationLogs(ctx, sourceRepLog, targetRepLog)

	return nil
}

// Locate Changed Documents
// https://docs.couchdb.org/en/stable/replication/protocol.html#locate-changed-documents
func (r *Replicator) LocateChangedDocuments(ctx context.Context) error {
start:
	// Listen to Changes Feed
	changes, err := r.source.Changes(ctx, ChangeOptions{
		since:     r.sourceLastSeq,
		Heartbeat: r.job.HeartbeatOrFallback(),
	})
	if err != nil {
		return err
	}

	// No more changes
	if len(changes.Results) == 0 {
		if r.job.Continuous {
			goto start
		} else {
			return ErrReplicationCompleted // Replication Completed
		}
	}

	// Read Batch of Changes
	diff := make(RevDiffRequest)
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
func (r *Replicator) CompareReplicationLogs(ctx context.Context, source, target *ReplicationLog) error {

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
