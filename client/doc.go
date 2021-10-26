package client

import "regexp"

type CompleteDoc struct {
	ID   string
	Data map[string]interface{}
}

func (d *CompleteDoc) HasChangedAttachments() bool {
	return true
}

func (d *CompleteDoc) Close() error {
	return nil
}

func (d *CompleteDoc) Size() int64 {
	return 0
}

func (d *CompleteDoc) InlineAttachments() error {
	return nil
}

var boundaryMixedRegexp = regexp.MustCompile(`multipart/mixed; boundary="([^"]+)"`)

var boundaryRelatedRegexp = regexp.MustCompile(`multipart/related; boundary="([^"]+)"`)
