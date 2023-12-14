#!/bin/bash

set -e
set -u
set -o pipefail

script_dir=$(dirname "${BASH_SOURCE[0]}")
project_dir=$(realpath "$script_dir/../..")
go_cmd=go

function os_name {
	value=$(uname -s)
	case $value in
		Linux)
			echo linux;;
		Darwin)
			echo macos;;
		*)
			echo "unsupported OS: $value"
			exit 1;;
	esac
}

function arch_name {
	value=$(uname -m)
	case $value in
		arm64)
			echo arm64;;
		aarch64)
			echo arm64;;
		x86_64)
			echo x64;;
		*)
			echo "unsupported CPU architecture: $value"
			exit 1;;
	esac
}

function lib_file_ext {
    value=$(os_name)
    case $value in
        linux)
            echo so;;
        macos)
            echo dylib;;
		*)
			echo "unsupported OS: $value"
			exit 1;;
    esac
}

function silkworm_go_version {
    grep "silkworm-go" "$project_dir/go.mod" | awk '{ print $2 }'
}

function libsilkworm_path {
    echo "$($go_cmd env GOMODCACHE)/github.com/erigontech/silkworm-go@$(silkworm_go_version)/lib/$(os_name)_$(arch_name)/libsilkworm_capi.$(lib_file_ext)"
}

libsilkworm_path
