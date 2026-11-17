.PHONY: build test check clean

build:
	go build -trimpath -o bin/print-queue .

test:
	go test ./...

check: test build

clean:
	rm -rf bin
