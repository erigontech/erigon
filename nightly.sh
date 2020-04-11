#!/usr/bin/env bash

SLEEP_TIME=60

make geth
./build/bin/geth > tgeth.log 2>&1 &

GETH_PID=$!

echo "sleeping for $SLEEP_TIME"

sleep $SLEEP_TIME

echo "killing GETH (pid=$GETH_PID)"
kill $GETH_PID
echo "boom"

wait $GETH_PID

GETH_STATUS=$?
echo "The exit status of the process was $GETH_STATUS"

exit $GETH_STATUS
