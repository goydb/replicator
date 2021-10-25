package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/goydb/replicator/logger"
)

var (
	ErrNotFound = errors.New("not found")
	ErrFailed   = errors.New("operation failed")
)

type Client struct {
	remote *Remote
	client *http.Client
	logger logger.Logger
	base   *url.URL
}

func NewClient(r *Remote) (*Client, error) {
	base, err := url.Parse(r.URL)
	if err != nil {
		return nil, err
	}

	return &Client{
		remote: r,
		client: http.DefaultClient,
		logger: new(logger.Noop),
		base:   base,
	}, nil
}

func (c *Client) SetLogger(logger logger.Logger) {
	c.logger = logger
}

func (c *Client) request(req *http.Request) (*http.Response, error) {
	for key, value := range c.remote.Headers {
		req.Header.Add(key, value)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		c.logger.Debugf("HTTP [%s] %s -> %s", req.Method, req.URL, err)
	} else {
		c.logger.Debugf("HTTP [%s] %s -> %d", req.Method, req.URL, resp.StatusCode)
	}

	return resp, err
}

func (c *Client) Check(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.remote.URL, nil)
	if err != nil {
		return err
	}

	resp, err := c.request(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() // nolint: errcheck

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	return fmt.Errorf("check request failed: %s", resp.Status)
}

func (c *Client) Create(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.remote.URL, nil)
	if err != nil {
		return err
	}

	resp, err := c.request(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() // nolint: errcheck

	var info struct {
		Error       string `json:"error"`
		ErrorReason string `json:"reason"`
		OK          bool   `json:"ok"`
	}
	err = json.NewDecoder(resp.Body).Decode(&info)
	if err != nil {
		return err
	}

	if !info.OK {
		return fmt.Errorf("%w: %s: %s", ErrFailed, info.Error, info.ErrorReason)
	}

	return nil
}

func (c *Client) Info(ctx context.Context) (*Info, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.remote.URL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.request(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() // nolint: errcheck

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("info request failed: %s", resp.Status)
	}

	var i Info
	err = json.NewDecoder(resp.Body).Decode(&i)
	if err != nil {
		return nil, err
	}

	return &i, nil
}

type Info struct {
	CommittedUpdateSeq int    `json:"committed_update_seq"`
	CompactRunning     bool   `json:"compact_running"`
	DbName             string `json:"db_name"`
	DiskFormatVersion  int    `json:"disk_format_version"`
	DocCount           int    `json:"doc_count"`
	DocDelCount        int    `json:"doc_del_count"`
	InstanceStartTime  string `json:"instance_start_time"`
	PurgeSeq           string `json:"purge_seq"`
	Sizes              Sizes  `json:"sizes"`
	UpdateSeq          string `json:"update_seq"`
}

type Sizes struct {
	Active   int64 `json:"active"`
	Disk     int64 `json:"disk"`
	External int64 `json:"external"`
}

func urlJoin(parts ...string) string {
	parts[0] = strings.TrimRight(parts[0], "/")
	return strings.Join(parts, "/")
}

func (c *Client) GetReplicationLog(ctx context.Context, id string) (*ReplicationLog, error) {
	u := urlJoin(c.remote.URL, "_local", id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.request(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() // nolint: errcheck

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("replication log request failed: %s", resp.Status)
	}

	var rl ReplicationLog
	err = json.NewDecoder(resp.Body).Decode(&rl)
	if err != nil {
		return nil, err
	}

	return &rl, nil
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
	path := fmt.Sprintf("_changes?feed=normal&style=all_docs&heartbeat=%d&since=%s",
		opts.Heartbeat.Milliseconds(), opts.Since)
	u := urlJoin(c.remote.URL, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.request(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() // nolint: errcheck

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("replication log request failed: %s", resp.Status)
	}

	var changes ChangesResponse
	err = json.NewDecoder(resp.Body).Decode(&changes)
	if err != nil {
		return nil, err
	}

	return &changes, nil
}

type ChangeOptions struct {
	Heartbeat time.Duration
	Since     string
}

type ChangesResponse struct {
	Results []Results `json:"results"`
	LastSeq string    `json:"last_seq"`
}
type Changes struct {
	Rev string `json:"rev"`
}
type Results struct {
	Seq     string    `json:"seq"`
	ID      string    `json:"id"`
	Changes []Changes `json:"changes"`
	Deleted bool      `json:"deleted,omitempty"`
}

func (c *Client) RevDiff(ctx context.Context, r RevDiffRequest) (DiffResponse, error) {
	var buf bytes.Buffer

	err := json.NewEncoder(&buf).Encode(r)
	if err != nil {
		return nil, err
	}

	u := urlJoin(c.remote.URL, "_revs_diff")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, &buf)
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")

	if err != nil {
		return nil, err
	}

	resp, err := c.request(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() // nolint: errcheck

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rev diff request failed: %s", resp.Status)
	}

	var diffResp DiffResponse
	err = json.NewDecoder(resp.Body).Decode(&diffResp)
	if err != nil {
		return nil, err
	}

	return diffResp, nil
}

type RevDiffRequest map[string][]string

type DiffResponse map[string]*Diff

type Diff struct {
	// Missing contains missing revisions
	Missing []string `json:"missing"`
}

// GetDocumentComplete
// 2.4.2.5.1. Fetch Changed Documents
func (c *Client) GetDocumentComplete(ctx context.Context, docid string, diff *Diff) (*CompleteDoc, error) {
	for i, rev := range diff.Missing {
		diff.Missing[i] = "%22" + rev + "%22"
	}

	u := urlJoin(c.remote.URL, docid+"?revs=true&latest=true&open_revs=[")
	u += strings.Join(diff.Missing, ",") + "]"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	req.Header.Add("Accept", "multipart/mixed")
	if err != nil {
		return nil, err
	}

	resp, err := c.request(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() // nolint: errcheck

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rev diff request failed: %s", resp.Status)
	}

	contentType := resp.Header.Get("Content-Type")
	matches := boundaryMixedRegexp.FindStringSubmatch(contentType)

	if len(matches) != 2 {
		return nil, fmt.Errorf("no multipart mixed")
	}

	reader := multipart.NewReader(resp.Body, matches[1])
	for {
		c.logger.Debug("Next part")
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		c.logger.Debugf("Next 1 part ## %#v", part.Header)

		contentType := part.Header.Get("Content-Type")
		matches := boundaryRelatedRegexp.FindStringSubmatch(contentType)

		if len(matches) != 2 {
			return nil, fmt.Errorf("no multipart related")
		}

		mr := multipart.NewReader(part, matches[1])
		for {
			c.logger.Debug("Next part")
			part1, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}

			c.logger.Debugf("Next 2 part ## %#v", part1.Header)
		}
	}

	return nil, nil
}
