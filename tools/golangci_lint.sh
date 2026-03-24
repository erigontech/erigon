#!/bin/bash

# Lint only files changed since the branch diverged from its base.
# Override: GOLANGCI_LINT_NEW_FROM_REV=<commit> or set to "" for full scan.
if [ -z "${GOLANGCI_LINT_NEW_FROM_REV+set}" ]; then
	BEST_TS=0
	for base in main $(git branch -r --list 'origin/release/*' 2>/dev/null | sed 's|origin/||;s/^[[:space:]]*//'); do
		MB=$(git merge-base HEAD "origin/$base" 2>/dev/null) || continue
		TS=$(git log --format=%ct -1 "$MB")
		if [ "$TS" -gt "$BEST_TS" ]; then
			BEST_TS=$TS
			GOLANGCI_LINT_NEW_FROM_REV=$MB
			BASE_BRANCH=$base
		fi
	done
fi

if [ -n "$GOLANGCI_LINT_NEW_FROM_REV" ]; then
	echo "lint: checking files changed since ${BASE_BRANCH:-$GOLANGCI_LINT_NEW_FROM_REV}"
	go tool golangci-lint run --config ./.golangci.yml --new-from-rev="$GOLANGCI_LINT_NEW_FROM_REV"
else
	go tool golangci-lint run --config ./.golangci.yml
fi
