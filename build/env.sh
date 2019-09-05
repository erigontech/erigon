#!/bin/sh

set -e

if [ ! -f "build/env.sh" ]; then
    echo "$0 must be run from the root of the repository."
    exit 2
fi

# Create fake Go workspace if it doesn't exist yet.
workspace="$GOPATH"
root="$PWD"
ethdir="$workspace/src/github.com/ledgerwatch"
if [ ! -L "$ethdir/turbo-geth" ]; then
    mkdir -p "$ethdir"
fi

# Run the command inside the workspace.
cd "$ethdir/turbo-geth"
PWD="$ethdir/turbo-geth"

# Launch the arguments with the configured environment.
GO111MODULE=on exec "$@"
