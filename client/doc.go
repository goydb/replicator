package client

import (
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"regexp"
	"strings"
)

var boundaryMixedRegexp = regexp.MustCompile(`multipart/mixed; boundary="([^"]+)"`)

var boundaryRelatedRegexp = regexp.MustCompile(`multipart/related; boundary="([^"]+)"`)

type CompleteDoc struct {
	ID          string
	Data        map[string]interface{}
	resp        *http.Response
	attachments []*multipart.Part
	size        sizeWriter
}

type sizeWriter int

func (sw *sizeWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	*sw = sizeWriter(int(*sw) + n)
	return
}

func NewCompleteDoc(docid string, resp *http.Response) (*CompleteDoc, error) {
	d := &CompleteDoc{
		ID:   docid,
		resp: resp,
	}

	// FIXME: Attachments and a document can be very large.
	// A reader that would swap to disk after a certain size
	// will slow down the process but use less memory.

	r := io.TeeReader(d.resp.Body, &d.size)
	mr, err := getMultipart(boundaryMixedRegexp, r, d.resp.Header)
	if err != nil {
		return nil, err
	}
	err = d.parseStageOne(mr)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func (d *CompleteDoc) HasChangedAttachments() bool {
	return len(d.attachments) > 0
}

func (d *CompleteDoc) Close() error {
	return d.resp.Body.Close()
}

func (d *CompleteDoc) Size() int64 {
	return int64(d.size)
}

func (d *CompleteDoc) parseStageOne(reader *multipart.Reader) error {
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		contentType := part.Header.Get("Content-Type")
		switch {
		case contentType == "application/json":
			// single document without attachments
			err = d.parseDocument(part)
			if err != nil {
				return err
			}
		case strings.HasPrefix(contentType, "multipart/related"):
			// mutlipart attachments and document
			mr, err := getMultipart(boundaryRelatedRegexp, part, http.Header(part.Header))
			if err != nil {
				return err
			}
			err = d.parseStageTwo(mr)
			if err != nil {
				return err
			}
		default:
			// unknown type
			return fmt.Errorf("invalid content type: %q", contentType)
		}
	}

	return nil
}

func (d *CompleteDoc) parseStageTwo(reader *multipart.Reader) error {
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		contentDisposition := part.Header.Get("Content-Disposition")
		switch {
		case contentDisposition == "":
			// main document
			err = d.parseDocument(part)
			if err != nil {
				return err
			}
		case strings.HasPrefix(contentDisposition, "attachment"):
			// mutlipart attachments
			d.attachments = append(d.attachments, part)
		default:
			// unknown type
			return fmt.Errorf("invalid content disposition: %q", contentDisposition)
		}
	}

	return nil
}

func (d *CompleteDoc) parseDocument(r io.ReadCloser) error {
	defer r.Close() // nolint: errcheck

	return json.NewDecoder(r).Decode(&d.Data)
}

func getMultipart(re *regexp.Regexp, r io.Reader, header http.Header) (*multipart.Reader, error) {
	contentType := header.Get("Content-Type")
	matches := re.FindStringSubmatch(contentType)

	if len(matches) != 2 {
		return nil, fmt.Errorf("no multipart related")
	}

	mr := multipart.NewReader(r, matches[1])
	return mr, nil
}

// InlineAttachments
// inlines the attachments using the base64 encoding.
func (d *CompleteDoc) InlineAttachments() error {
	// TODO implement
	return nil
}
