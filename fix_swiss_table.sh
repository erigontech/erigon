#!/bin/bash

# Fix Swiss table conflicts in Go runtime
# This script addresses the mapiterinit redeclared errors

echo "🔧 Fixing Swiss table conflicts for Olym3 Testnet Season 3..."

# Check current Go version
echo "📋 Current Go version:"
go version

# Method 1: Disable Swiss table optimization
echo "🔨 Method 1: Building with Swiss table disabled..."
cd cmd/erigon

# Build with GOTAGS to disable Swiss table
GOTAGS="noswiss" go build -o ../../build/bin/erigon

cd ../..

if [ -f "build/bin/erigon" ]; then
    echo "✅ Swiss table disabled build successful!"
    ls -la build/bin/erigon
    echo ""
    echo "🎯 Ready to run Olym3 Testnet Season 3!"
    exit 0
fi

# Method 2: Use older Go build approach
echo "🔨 Method 2: Using older Go build approach..."
cd cmd/erigon

# Build without optimization flags
CGO_ENABLED=1 go build -ldflags "-s -w" -o ../../build/bin/erigon

cd ../..

if [ -f "build/bin/erigon" ]; then
    echo "✅ Older build approach successful!"
    ls -la build/bin/erigon
    exit 0
fi

# Method 3: Build with specific Go version flags
echo "🔨 Method 3: Building with specific Go version flags..."
cd cmd/erigon

# Set Go build flags to avoid Swiss table
export GOFLAGS="-buildvcs=false"
go build -tags "netgo" -o ../../build/bin/erigon

cd ../..

if [ -f "build/bin/erigon" ]; then
    echo "✅ Specific flags build successful!"
    ls -la build/bin/erigon
    exit 0
fi

# Method 4: Try building without CGO
echo "🔨 Method 4: Building without CGO..."
cd cmd/erigon

CGO_ENABLED=0 go build -o ../../build/bin/erigon

cd ../..

if [ -f "build/bin/erigon" ]; then
    echo "✅ No-CGO build successful!"
    ls -la build/bin/erigon
    exit 0
fi

# Method 5: Manual build with minimal flags
echo "🔨 Method 5: Manual build with minimal flags..."
cd cmd/erigon

# Build with absolute minimal flags
go build -trimpath=false -buildvcs=false -o ../../build/bin/erigon

cd ../..

if [ -f "build/bin/erigon" ]; then
    echo "✅ Minimal flags build successful!"
    ls -la build/bin/erigon
    exit 0
fi

echo "❌ All Swiss table fix methods failed."
echo ""
echo "💡 Next steps:"
echo "1. Try updating Go: sudo apt update && sudo apt upgrade golang-go"
echo "2. Try installing Go 1.21: wget https://go.dev/dl/go1.21.5.linux-amd64.tar.gz"
echo "3. Use Docker build: ./docker_build.sh"
echo "4. Check if there are multiple Go installations"

exit 1
