#!/bin/bash

COMMIT_SHA="$1"

sed "s/\$COMMIT_SHA/$COMMIT_SHA/" go.mod.template > go.mod

rm -f go.sum
go mod tidy

go run main.go
