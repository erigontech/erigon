#!/usr/bin/env bash

# cmake -S . -B build -DEVMONE_TESTING=OFF
cmake --build build --parallel


mkdir -p ./core/evmone-go/lib
cp ./build/lib/libevmon* ./core/evmone-go/lib

CGO_CPPFLAGS_ALLOW='-std.*'

make erigon