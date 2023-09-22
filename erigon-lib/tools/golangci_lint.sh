#!/bin/bash

scriptDir=$(dirname "${BASH_SOURCE[0]}")
scriptName=$(basename "${BASH_SOURCE[0]}")
version="v1.54.2"

if [[ "$1" == "--install-deps" ]]
then
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b "$(go env GOPATH)/bin" "$version"
	exit
fi

if ! which golangci-lint > /dev/null
then
	echo "golangci-lint tool is not found, install it with:"
	echo "    $scriptName --install-deps"
	echo "or follow https://golangci-lint.run/usage/install/"
	exit
fi

golangci-lint run --config ./.golangci.yml
