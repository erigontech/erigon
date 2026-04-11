#!/bin/bash

# ruleguard runs as a separate `go vet -vettool` step. Running it via gocritic
# inside golangci-lint re-creates a ruleguard engine for every package
# (~50s of fixed overhead on this repo); as a vet tool it hits Go's vet cache
# and re-analyzes only changed packages.
go build -o ./build/bin/ruleguard ./tools/ruleguard
go vet -vettool=./build/bin/ruleguard ./...

go tool golangci-lint run --config ./.golangci.yml --fast-only
go tool golangci-lint run --config ./.golangci.yml
