#!/usr/bin/env bash

# running the job for 5 hours
let SLEEP_TIME=5*60*60

# GOFLAGS=-modcacherw is required for our CI
# to be able to remove go modules cache
GOFLAGS=-modcacherw make geth

echo "running geth..."
./build/bin/geth > tgeth.log 2>&1 &

GETH_PID=$!

echo "sleeping for $SLEEP_TIME seconds"

sleep $SLEEP_TIME

echo "killing GETH (pid=$GETH_PID)"
kill $GETH_PID
echo "boom"

wait $GETH_PID

GETH_STATUS=$?
echo "The exit status of the process was $GETH_STATUS"

exit $GETH_STATUS
