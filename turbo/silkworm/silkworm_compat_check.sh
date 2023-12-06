#!/bin/bash

set -e
set -u
set -o pipefail

OS_RELEASE_PATH=/etc/os-release

function glibc_version {
    cmd="ldd --version"
    $cmd | head -1 | awk '{ print $NF }'
}

function version_major {
    IFS='.' read -a components <<< "$1"
    echo "${components[0]}"
}

function version_minor {
    IFS='.' read -a components <<< "$1"
    echo "${components[1]}"
}

case $(uname -s) in
	Linux)
		if [[ ! -f "$OS_RELEASE_PATH" ]]
		then
			echo "not supported Linux without $OS_RELEASE_PATH"
			exit 2
		fi

		source "$OS_RELEASE_PATH"

        if [[ -n "$ID" ]] && [[ -n "$VERSION_ID" ]]
        then
            version=$(version_major "$VERSION_ID")
            case "$ID" in
                "debian")
                    if (( version < 12 ))
                    then
                        echo "not supported Linux version: $ID $VERSION_ID"
                        exit 3
                    fi
                    ;;
                "ubuntu")
                    if (( version < 22 ))
                    then
                        echo "not supported Linux version: $ID $VERSION_ID"
                        exit 3
                    fi
                    ;;
            esac
        fi

        version=$(version_minor "$(glibc_version)")
        if (( version < 34 ))
        then
            echo "not supported glibc version: $version"
            exit 4
        fi

		;;
	Darwin)
		;;
	*)
		echo "unsupported OS"
		exit 1
		;;
esac
