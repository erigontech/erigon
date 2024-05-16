#!/usr/bin/bash

DATADIR=erigon_data

rm -rf $DATADIR

./build/bin/erigon init --datadir $DATADIR genesis.json

./build/bin/erigon --datadir $DATADIR \
    --chain=interop \
    --log.console.verbosity=4 \
    --exeternal \