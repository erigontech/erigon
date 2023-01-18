#!/bin/bash

set -e

TARGET_RESPONSE=$(curl -s -X POST  -H "Content-Type: application/json"  --data '{"jsonrpc":"2.0","method":"admin_nodeInfo","params":[],"id":1}' "127.0.0.1:8545" )
echo ${TARGET_RESPONSE}| jq -r '.result.enode' | sed 's/127.0.0.1/erigon/g' | sed 's/?discport=0//g'