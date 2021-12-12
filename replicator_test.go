package replicator_test

import (
	"context"
	"testing"

	"github.com/goydb/replicator"
	"github.com/goydb/replicator/client"
	"github.com/goydb/replicator/logger"
	"github.com/stretchr/testify/assert"
)

func TestReplication(t *testing.T) {
	auth := map[string]string{"Authorization": "Basic YWRtaW46c2VjcmV0"}

	r, err := replicator.NewReplicator("test", &replicator.Job{
		Source: &client.Remote{
			URL:     "http://localhost:5984/source/",
			Headers: auth,
		},
		Target: &client.Remote{
			URL:     "http://localhost:5984/target/",
			Headers: auth,
		},
		CreateTarget: true,
	})
	assert.NoError(t, err)
	r.SetLogger(new(logger.Stdout))

	// err = r.Reset(context.Background())
	// assert.NoError(t, err)

	err = r.Run(context.Background())
	assert.NoError(t, err)
}
