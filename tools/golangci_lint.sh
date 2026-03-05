#!/bin/bash

go tool golangci-lint run --config ./.golangci.yml --fast-only
go tool golangci-lint run --config ./.golangci.yml
