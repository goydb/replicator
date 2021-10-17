# replicator [![Go Reference](https://pkg.go.dev/badge/github.com/goydb/replicator.svg)](https://pkg.go.dev/github.com/goydb/replicator)

Go implementation of https://docs.couchdb.org/en/stable/replication/protocol.html

Goal is to have a 100% compatible implemntation of the replicator.

## Ideas

Create a kubernetes deployable replicator. Requirements:

* Configuration via environment
* Chart example
* Docker container
* Logging
* Termination log

## Couchdb 

Launch via podman for local testing.

    make couchdb
