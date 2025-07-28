#!/bin/bash

scriptDir=$(dirname "${BASH_SOURCE[0]}")
scriptName=$(basename "${BASH_SOURCE[0]}")
version="v2.3.0"

install_dir="$(go env GOBIN)"
if [ -z "$install_dir" ]; then
  install_dir="$(go env GOPATH)/bin"
fi

bin_path="$install_dir/golangci-lint"

if [ ! -x "$bin_path" ]; then
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b "$install_dir" "$version"
fi

"$bin_path" run --config ./.golangci.yml
