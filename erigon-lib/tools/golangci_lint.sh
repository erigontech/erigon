#!/bin/bash

scriptDir=$(dirname "${BASH_SOURCE[0]}")
scriptName=$(basename "${BASH_SOURCE[0]}")
version="v2.2.0"

if [[ "$1" == "--install-deps" ]]
then
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b "$(go env GOPATH)/bin" "$version"
	exit
fi

bin_path="$install_dir/golangci-lint"

if [ ! -x "$bin_path" ]; then
  echo "golangci-lint tool is not found in $install_dir, install it with:"
  echo "    make lint-deps"
  echo "or follow https://golangci-lint.run/usage/install/"
  exit 2
fi

"$bin_path" run --config ./.golangci.yml
