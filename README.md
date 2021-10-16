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

    mkdir -p tmp/couchdbdata tmp/couchdbconf
    podman run --volume $PWD/tmp/couchdbdata:/home/couchdb/data \
        --volume $PWD/tmp/couchdbconf:/opt/couchdb/etc/local.d \
        -p 5984:5984 -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=secret \
        docker.io/library/couchdb:latest
