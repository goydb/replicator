package replicator

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound = errors.New("not found")
)

type Client struct {
}

func (c *Client) Check(ctx context.Context) error {

	// HEAD /source HTTP/1.1
	// Host: localhost:5984
	// User-Agent: CouchDB

	// Response:

	// HTTP/1.1 200 OK
	// Cache-Control: must-revalidate
	// Content-Type: application/json
	// Date: Sat, 05 Oct 2013 08:50:39 GMT
	// Server: CouchDB (Erlang/OTP)

	return nil
}

func (c *Client) Create(ctx context.Context) error {
	// 	PUT /target HTTP/1.1
	// Accept: application/json
	// Host: localhost:5984
	// User-Agent: CouchDB

	// Response:

	// HTTP/1.1 201 Created
	// Content-Length: 12
	// Content-Type: application/json
	// Date: Sat, 05 Oct 2013 08:58:41 GMT
	// Server: CouchDB (Erlang/OTP)

	// {
	//     "ok": true
	// }

	// However, the Replicator’s PUT request MAY NOT succeeded due to insufficient privileges (which are granted by the provided credential) and so receive a 401 Unauthorized or a 403 Forbidden error. Such errors SHOULD be expected and well handled:

	// HTTP/1.1 500 Internal Server Error
	// Cache-Control: must-revalidate
	// Content-Length: 108
	// Content-Type: application/json
	// Date: Fri, 09 May 2014 13:50:32 GMT
	// Server: CouchDB (Erlang OTP)

	// {
	//     "error": "unauthorized",
	//     "reason": "unauthorized to access or create database http://localhost:5984/target"
	// }

	return nil
}

func (c *Client) Info(ctx context.Context) (*Info, error) {

	// Request:

	// GET /source HTTP/1.1
	// Accept: application/json
	// Host: localhost:5984
	// User-Agent: CouchDB

	// Response:

	// HTTP/1.1 200 OK
	// Cache-Control: must-revalidate
	// Content-Length: 256
	// Content-Type: application/json
	// Date: Tue, 08 Oct 2013 07:53:08 GMT
	// Server: CouchDB (Erlang OTP)

	// {
	//     "committed_update_seq": 61772,
	//     "compact_running": false,
	//     "db_name": "source",
	//     "disk_format_version": 6,
	//     "doc_count": 41961,
	//     "doc_del_count": 3807,
	//     "instance_start_time": "0",
	//     "purge_seq": 0,
	//     "sizes": {
	//       "active": 70781613961,
	//       "disk": 79132913799,
	//       "external": 72345632950
	//     },
	//     "update_seq": 61772
	// }
	return nil, nil
}

type Info struct {
	CommittedUpdateSeq int    `json:"committed_update_seq"`
	CompactRunning     bool   `json:"compact_running"`
	DbName             string `json:"db_name"`
	DiskFormatVersion  int    `json:"disk_format_version"`
	DocCount           int    `json:"doc_count"`
	DocDelCount        int    `json:"doc_del_count"`
	InstanceStartTime  string `json:"instance_start_time"`
	PurgeSeq           int    `json:"purge_seq"`
	Sizes              Sizes  `json:"sizes"`
	UpdateSeq          int    `json:"update_seq"`
}

type Sizes struct {
	Active   int64 `json:"active"`
	Disk     int64 `json:"disk"`
	External int64 `json:"external"`
}

func (c *Client) GetReplicationLog(ctx context.Context, id string) (*ReplicationLog, error) {
	return nil, nil
}

type ReplicationLog struct {
	ID                   string    `json:"_id"`
	Rev                  string    `json:"_rev"`
	History              []History `json:"history"`
	ReplicationIDVersion int       `json:"replication_id_version"` // Replication protocol version. Defines Replication ID calculation algorithm, HTTP API calls and the others routines. Required
	SessionID            string    `json:"session_id"`             // Unique ID of the last session. Shortcut to the session_id field of the latest history object. Required
	SourceLastSeq        string    `json:"source_last_seq"`        // Last processed Checkpoint. Shortcut to the recorded_seq field of the latest history object. Required
}

type History struct {
	DocWriteFailures int    `json:"doc_write_failures"` // Number of failed writes
	DocsRead         int    `json:"docs_read"`          // Number of read documents
	DocsWritten      int    `json:"docs_written"`       // Number of written documents
	EndLastSeq       int    `json:"end_last_seq"`       //  Last processed Update Sequence ID
	EndTime          string `json:"end_time"`           // Replication completion timestamp in RFC 5322 format
	MissingChecked   int    `json:"missing_checked"`    // Number of checked revisions on Source
	MissingFound     int    `json:"missing_found"`      // Number of missing revisions found on Target
	RecordedSeq      string `json:"recorded_seq"`       // Recorded intermediate Checkpoint. Required
	SessionID        string `json:"session_id"`         // Unique session ID. Commonly, a random UUID value is used. Required
	StartLastSeq     int    `json:"start_last_seq"`     // Start update Sequence ID
	StartTime        string `json:"start_time"`         //  Replication start timestamp in RFC 5322 format
}

func (c *Client) Changes(ctx context.Context, opts ChangeOptions) (*ChangesResponse, error) {
	// /source/_changes?feed=normal&style=all_docs&heartbeat=10000

	return nil, nil
}

type ChangeOptions struct {
	Heartbeat time.Duration
	since     string
}

type ChangesResponse struct {
	Results []Results `json:"results"`
	LastSeq int       `json:"last_seq"`
}
type Changes struct {
	Rev string `json:"rev"`
}
type Results struct {
	Seq     int       `json:"seq"`
	ID      string    `json:"id"`
	Changes []Changes `json:"changes"`
	Deleted bool      `json:"deleted,omitempty"`
}

func (c *Client) RevDiff(ctx context.Context, r RevDiffRequest) (DiffResponse, error) {
	return nil, nil
}

type RevDiffRequest map[string][]string

type DiffResponse map[string]Diff

type Diff struct {
	// Missing contains missing revisions
	Missing []string `json:"missing"`
}

// GetDocumentComplete
// 2.4.2.5.1. Fetch Changed Documents
func (c *Client) GetDocumentComplete(ctx context.Context, docid string) error {
	return nil
}
