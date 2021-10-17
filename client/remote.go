package client

import (
	"bufio"
	"sort"
)

type Remote struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

func (r Remote) GenerateReplicationID(b *bufio.Writer) {
	_, err := b.WriteString(r.URL)
	if err != nil {
		panic(err)
	}
	_, err = b.WriteString("|")
	if err != nil {
		panic(err)
	}

	var keys []string
	for key := range r.Headers {
		keys = append(keys, key)
	}

	sort.Stable(sort.StringSlice(keys))
	for _, key := range keys {
		value := r.Headers[key]
		_, err = b.WriteString(key)
		if err != nil {
			panic(err)
		}
		_, err = b.WriteString("|")
		if err != nil {
			panic(err)
		}
		_, err = b.WriteString(value)
		if err != nil {
			panic(err)
		}
		_, err = b.WriteString("|")
		if err != nil {
			panic(err)
		}
	}
}
