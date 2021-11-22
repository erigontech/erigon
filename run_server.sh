#!/bin/sh

BASE=$(pwd) # absolute path to erigon
BIN_DIR=$BASE/build/bin


EXECUTABLE=tx_server
ENTRY_FOLDER=$BASE/tx_analysis/server

go build -o $BIN_DIR/$EXECUTABLE $ENTRY_FOLDER/...

$BIN_DIR/$EXECUTABLE