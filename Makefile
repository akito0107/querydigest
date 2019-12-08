.PHONY: build
build: bin/querydigest

.PHONY: bin/astprinter
bin/querydigest:
	go build -o bin/querydigest cmd/querydigest/main.go

# .PHONY: generate
# generate:

.PHONY: test
test: vendor
	go test ./... -cover -count=1 -v

