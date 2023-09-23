#!/bin/bash

scriptDir=$(dirname "${BASH_SOURCE[0]}")
scriptName=$(basename "${BASH_SOURCE[0]}")
projectDir="$scriptDir/.."
goLicensesVersion="v1.6.0"

if [[ "$1" == "--install-deps" ]]
then
	go install "github.com/google/go-licenses@$goLicensesVersion"
	exit
fi

if ! which go-licenses > /dev/null
then
	echo "go-licenses tool is not found, install it with:"
	echo "    go install github.com/google/go-licenses@$goLicensesVersion"
	exit 2
fi

# enable build tags to cover maximum .go files
export GOFLAGS="-tags=gorules,linux,tools"

output=$(find "$projectDir" -type 'd' -maxdepth 1 \
    -not -name ".*" \
    -not -name tools \
    -not -name build \
    | xargs go-licenses report 2>&1 \
    `# exceptions` \
    | grep -v "erigon-lib has empty version"        `# self` \
    | grep -v "golang.org/x/"                       `# a part of Go` \
    | grep -v "crawshaw.io/sqlite"                  `# ISC` \
    | grep -v "erigon-lib/sais"                     `# MIT` \
    | grep -v "github.com/anacrolix/go-libutp"      `# MIT` \
    | grep -v "github.com/anacrolix/mmsg"           `# MPL-2.0` \
    | grep -v "github.com/anacrolix/multiless"      `# MPL-2.0` \
    | grep -v "github.com/anacrolix/sync"           `# MPL-2.0` \
    | grep -v "github.com/anacrolix/upnp"           `# MPL-2.0` \
    | grep -v "github.com/go-llsqlite/adapter"      `# MPL-2.0` \
    | grep -v "github.com/go-llsqlite/crawshaw"     `# ISC` \
    | grep -v "github.com/consensys/gnark-crypto"   `# Apache-2.0` \
    | grep -v "github.com/erigontech/mdbx-go"       `# Apache-2.0` \
    | grep -v "github.com/ledgerwatch/secp256k1"    `# BSD-3-Clause` \
    | grep -v "github.com/RoaringBitmap/roaring"    `# Apache-2.0` \
    | grep -v "github.com/!roaring!bitmap/roaring"  `# Apache-2.0` \
    | grep -v "pedersen_hash"                       `# Apache-2.0` \
    `# approved licenses` \
    | grep -Ev "Apache-2.0$" \
    | grep -Ev "BSD-2-Clause$" \
    | grep -Ev "BSD-3-Clause$" \
    | grep -Ev "ISC$" \
    | grep -Ev "MIT$" \
    | grep -Ev "MPL-2.0$" \
)

if [[ -n "$output" ]]
then
	echo "ERROR: $scriptName has found issues!" 1>&2
	echo "ERROR: If it is a false positive, add it to the exceptions list in the script:" 1>&2
	echo "$output" 1>&2
	exit 1
fi
