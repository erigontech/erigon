#!/bin/bash

# Lint only files changed since the branch diverged from origin/main.
# Override: GOLANGCI_LINT_NEW_FROM_REV=<commit> or ="" for full scan.
# CI uses `make lintci` which runs a full scan directly.
NEW_FROM_REV="${GOLANGCI_LINT_NEW_FROM_REV:-$(git merge-base origin/main HEAD 2>/dev/null)}"

if [ -n "$NEW_FROM_REV" ]; then
	go tool golangci-lint run --config ./.golangci.yml --new-from-rev="$NEW_FROM_REV"
else
	go tool golangci-lint run --config ./.golangci.yml
fi
