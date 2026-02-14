#!/bin/bash

GOEXPERIMENT=synctest go tool golangci-lint run --config ./.golangci.yml --fast-only
GOEXPERIMENT=synctest go tool golangci-lint run --config ./.golangci.yml
