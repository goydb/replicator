package replicator

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"time"
)

type Job struct {
	ID           string  `json:"_id"`
	Rev          string  `json:"_rev"`
	UserCtx      UserCtx `json:"user_ctx"`
	Source       Remote  `json:"source"`
	Target       Remote  `json:"target"`
	CreateTarget bool    `json:"create_target"`
	Continuous   bool    `json:"continuous"`
	Owner        string  `json:"owner"`

	Config
}

type Config struct {
	// Heartbeat For Continuous Replication the heartbeat parameter defines the heartbeat period in milliseconds. The RECOMMENDED value by default is 10000 (10 seconds).
	Heartbeat time.Duration
}

func (c Config) HeartbeatOrFallback() time.Duration {
	if c.Heartbeat == 0 {
		return time.Second * 10
	}
	return c.Heartbeat
}

// GenerateReplicationID generates a replication id
// using the given name, name could be a hostame.
// https://docs.couchdb.org/en/stable/replication/protocol.html#generate-replication-id
func (j *Job) GenerateReplicationID(name string) string {
	hash := sha256.New()

	b := bufio.NewWriter(hash)
	b.WriteString(name)
	b.WriteString("|")
	j.Source.GenerateReplicationID(b)
	b.WriteString("|")
	j.Target.GenerateReplicationID(b)
	b.WriteString("|")

	if j.CreateTarget {
		b.WriteString("T")
	} else {
		b.WriteString("F")
	}

	if j.Continuous {
		b.WriteString("T")
	} else {
		b.WriteString("F")
	}

	b.Flush()

	final := hash.Sum(nil)
	return hex.EncodeToString(final)
}

type UserCtx struct {
	Name  string   `json:"name"`
	Roles []string `json:"roles"`
}

type Remote struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

func (r Remote) GenerateReplicationID(b *bufio.Writer) {
	b.WriteString(r.URL)
	b.WriteString("|")

	var keys []string
	for key := range r.Headers {
		keys = append(keys, key)
	}

	sort.Stable(sort.StringSlice(keys))
	for _, key := range keys {
		value := r.Headers[key]
		b.WriteString(key)
		b.WriteString("|")
		b.WriteString(value)
		b.WriteString("|")
	}
}
