package client

import (
	"encoding/json"
	"io"
)

type Stack []*CompleteDoc

// Size returns the total size of all the documents in the stack in bytes
func (s Stack) Size() int64 {
	var size int64

	for _, doc := range s {
		size += doc.Size()
	}

	return size
}

// Reader generates a reader that serializes the stacks data to json
func (s Stack) Reader() (io.ReadCloser, error) {
	r, w := io.Pipe()

	go func() {
		// body
		var body struct {
			Docs     []map[string]interface{} `json:"docs"`
			NewEdits bool                     `json:"new_edits"`
		}
		body.NewEdits = false

		// add all document data
		for _, attachment := range s {
			body.Docs = append(body.Docs, attachment.Data)
		}

		// serialize the data
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		err := enc.Encode(body)
		if err != nil {
			w.CloseWithError(err)
		}

		w.Close()
	}()

	return r, nil
}
