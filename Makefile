.PHONY: build
build: bin/querydigest

.PHONY: bin/astprinter
bin/querydigest: vendor main.go
	go build -o bin/querydigest main.go

# .PHONY: generate
# generate:
# 	go generate ./...
 
vendor: Gopkg.toml Gopkg.lock
	dep ensure

.PHONY: test
test: vendor
	go test ./... -cover -count=1 -v

