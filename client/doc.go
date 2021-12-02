package client

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"regexp"
	"strings"
)

var boundaryMixedRegexp = regexp.MustCompile(`multipart/mixed; boundary="([^"]+)"`)

var boundaryRelatedRegexp = regexp.MustCompile(`multipart/related; boundary="([^"]+)"`)

var dispositionFilename = regexp.MustCompile(`attachment; filename="([^"]+)"`)

type CompleteDoc struct {
	ID          string
	Data        map[string]interface{}
	resp        *http.Response
	attachments []attachmentMultipartData
	size        sizeWriter
}

type attachmentMultipartData struct {
	Part *multipart.Part
	Data []byte
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
			data, err := io.ReadAll(part)
			if err != nil {
				return fmt.Errorf("failed to read %s", contentDisposition)
			}

			d.attachments = append(d.attachments, attachmentMultipartData{
				Part: part,
				Data: data,
			})
		default:
			// unknown type
			return fmt.Errorf("invalid content disposition: %q", contentDisposition)
		}
	}

	return nil
}

func (d *CompleteDoc) parseDocument(r io.ReadCloser) error {
	defer r.Close() // nolint: errcheck

	err := json.NewDecoder(r).Decode(&d.Data)
	if err != nil {
		return err
	}

	return nil
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
// inline the attachments using the base64 encoding.
func (d *CompleteDoc) InlineAttachments() error {
	for _, attachment := range d.attachments {
		disposition := attachment.Part.Header.Get("Content-Disposition")
		matches := dispositionFilename.FindStringSubmatch(disposition)

		if len(matches) != 2 {
			return fmt.Errorf("invalid attachment, filename missing")
		}
		filename := matches[1]

		// get to attachments
		attrsObj, ok := d.Data["_attachments"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid attachments data in json for %q", filename)
		}

		// get to single attachment
		attObj, ok := attrsObj[filename].(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid attachment data in json for %q", filename)
		}

		// if encoded via gzip, decode
		if attObj["encoding"] == "gzip" {
			r := io.Reader(bytes.NewReader(attachment.Data))
			r, err := gzip.NewReader(r)
			if err != nil {
				return fmt.Errorf("unable to create attachment from gzip: %w", err)
			}
			data, err := io.ReadAll(r)
			if err != nil {
				return fmt.Errorf("unable to decompress attachment from gzip: %w", err)
			}
			attachment.Data = data
			delete(attObj, "encoding")
			delete(attObj, "encoded_length")
		}

		// inline attachment
		data := base64.StdEncoding.EncodeToString(attachment.Data)
		attObj["data"] = data

		delete(attObj, "stub")
		delete(attObj, "digest")
		delete(attObj, "length")
		delete(attObj, "follows")
	}

	return nil
}

// Reader returns a multipart mime representation of the complete doc
func (d *CompleteDoc) Reader() (io.ReadCloser, string, error) {
	r, w := io.Pipe()
	mr := multipart.NewWriter(w)

	go func() {
		// write document json
		dw, err := mr.CreatePart(textproto.MIMEHeader{
			"Content-Type": []string{"application/json"},
		})
		if err != nil {
			w.CloseWithError(err)
		}

		err = json.NewEncoder(dw).Encode(d.Data)
		if err != nil {
			w.CloseWithError(err)
		}

		// write attachments
		for _, attachment := range d.attachments {
			aw, err := mr.CreatePart(attachment.Part.Header)
			if err != nil {
				w.CloseWithError(err)
			}

			_, err = aw.Write(attachment.Data)
			if err != nil {
				w.CloseWithError(err)
			}
		}

		// close multipart writer and pipe
		mr.Close()
		w.Close()
	}()

	return r, mr.Boundary(), nil
}
