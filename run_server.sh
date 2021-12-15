#!/bin/sh

BASE=$(pwd) # absolute path to erigon
BIN_DIR=$BASE/build/bin


EXECUTABLE=tx_server
ENTRY_FOLDER=$BASE/tx_analysis/server

go build -o $BIN_DIR/$EXECUTABLE $ENTRY_FOLDER/...

$BIN_DIR/$EXECUTABLE


# ./build/bin/erigon --datadir=/mnt/mx500_0/goerli --chain goerli --private.api.addr=localhost:9090
# ./build/bin/rpcdaemon --private.api.addr=localhost:9090 --http.api=eth,erigon,web3,net,debug,trace,txpool --verbosity=4 --datadir /mnt/mx500_0/goerli --ws