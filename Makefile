.PHONY: build
build: bin/querydigest

.PHONY: bin/querydigest
bin/querydigest: vendor
	go build -o bin/querydigest -mod vendor cmd/querydigest/main.go

# .PHONY: generate
# generate:

.PHONY: test
test: vendor
	go test ./... -cover -count=1 -v

vendor: go.mod go.sum
	go mod vendor

