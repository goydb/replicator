# replicator
Go implementation of https://docs.couchdb.org/en/stable/replication/protocol.html


## Couchdb 

Launch via podman

mkdir -p tmp/couchdbdata tmp/couchdbconf
podman run --volume $PWD/tmp/couchdbdata:/home/couchdb/data \
    --volume $PWD/tmp/couchdbconf:/opt/couchdb/etc/local.d \
    -p 5984:5984 -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=secret \
    docker.io/library/couchdb:latest
