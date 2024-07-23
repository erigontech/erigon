#!/usr/bin/env bash

# cmake -S . -B build -DEVMONE_TESTING=OFF
cmake --build build --parallel

sudo cp ./build/lib/libevmon* /usr/local/lib

CGO_CPPFLAGS_ALLOW='-std.*'

make erigon
# go run ./core/evmone-go/...