#!/bin/bash

scriptDir=$(dirname "${BASH_SOURCE[0]}")
scriptName=$(basename "${BASH_SOURCE[0]}")
version="v2.2.2"

install_dir="./../build/bin"
bin_path="$install_dir/golangci-lint"

if [ -e $bin_path ]
then
  echo "installed"
else
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b "$install_dir" "$version"
fi

"$bin_path" run --config ./.golangci.yml
