#!/bin/bash

go mod tidy

if ! git diff --exit-code -- go.mod go.sum
then
	echo "ERROR: go.mod/sum is not tidy. Run 'go mod tidy' in $PWD and commit the changes."
	exit 1
fi
