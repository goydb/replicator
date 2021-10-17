package replicator

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/goydb/replicator/client"
)

type Job struct {
	ID           string         `json:"_id"`
	Rev          string         `json:"_rev"`
	UserCtx      UserCtx        `json:"user_ctx"`
	Source       *client.Remote `json:"source"`
	Target       *client.Remote `json:"target"`
	CreateTarget bool           `json:"create_target"`
	Continuous   bool           `json:"continuous"`
	Owner        string         `json:"owner"`

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
	_, err := b.WriteString(name)
	if err != nil {
		panic(err)
	}
	_, err = b.WriteString("|")
	if err != nil {
		panic(err)
	}
	j.Source.GenerateReplicationID(b)
	_, err = b.WriteString("|")
	if err != nil {
		panic(err)
	}
	j.Target.GenerateReplicationID(b)
	_, err = b.WriteString("|")
	if err != nil {
		panic(err)
	}

	if j.CreateTarget {
		_, err = b.WriteString("T")
		if err != nil {
			panic(err)
		}
	} else {
		_, err = b.WriteString("F")
		if err != nil {
			panic(err)
		}
	}

	if j.Continuous {
		_, err = b.WriteString("T")
		if err != nil {
			panic(err)
		}
	} else {
		_, err = b.WriteString("F")
		if err != nil {
			panic(err)
		}
	}

	b.Flush()

	final := hash.Sum(nil)
	return hex.EncodeToString(final)
}

type UserCtx struct {
	Name  string   `json:"name"`
	Roles []string `json:"roles"`
}
