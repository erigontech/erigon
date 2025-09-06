#!/bin/bash

# Quick fix for Go toolchain issues
# This script provides immediate solutions

echo "🚀 Quick fix for Go toolchain issues..."

# Method 1: Set GOTOOLCHAIN=local
echo "🔧 Setting GOTOOLCHAIN=local..."
export GOTOOLCHAIN=local
export GOFLAGS="-buildvcs=false"

echo "📋 Go version after fix:"
go version

# Try building immediately
echo "🔨 Building with fixed Go settings..."
make clean 2>/dev/null || true
make erigon

if [ -f "build/bin/erigon" ]; then
    echo "✅ Build successful!"
    echo ""
    echo "🎯 Olym3 Testnet Season 3 is ready!"
    echo "   To run: ./build/bin/erigon --chain=olym3-testnet-s3 --datadir=olym3-data --http.api=eth,erigon,web3,net,debug,trace,txpool"
    exit 0
fi

# Method 2: Simple go build
echo "🔨 Trying simple go build..."
cd cmd/erigon
go build -o ../../build/bin/erigon
cd ../..

if [ -f "build/bin/erigon" ]; then
    echo "✅ Simple build successful!"
    echo ""
    echo "🎯 Olym3 Testnet Season 3 is ready!"
    echo "   To run: ./build/bin/erigon --chain=olym3-testnet-s3 --datadir=olym3-data --http.api=eth,erigon,web3,net,debug,trace,txpool"
    exit 0
fi

echo "❌ Quick fix failed. Try:"
echo "   ./fix_go_toolchain.sh"
echo "   or"
echo "   ./docker_build.sh"

exit 1
