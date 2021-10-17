.PHONY: lint test couchdb

GO:=go
GO_TEST_FLAGS:=-count=1 -v -cover -race

lint:
	$(GO) run github.com/golangci/golangci-lint/cmd/golangci-lint run --timeout 2m

test:
	$(GO) test $(GO_TEST_FLAGS) -short ./...

couchdb:
	mkdir -p tmp/couchdbdata tmp/couchdbconf
	podman run --volume $(PWD)/tmp/couchdbdata:/opt/couchdb/data \
		--volume $(PWD)/tmp/couchdbconf:/opt/couchdb/etc/local.d \
		-p 5984:5984 -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=secret \
		docker.io/library/couchdb:latest
