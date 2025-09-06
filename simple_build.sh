#!/bin/bash

# Simple build script for Olym3 Testnet Season 3
# This script avoids complex build flags that cause conflicts

echo "🔧 Simple build for Olym3 Testnet Season 3..."

# Check if we're in the right directory
if [ ! -f "go.mod" ]; then
    echo "❌ Error: Not in Erigon root directory"
    echo "Please run: cd /home/erigon/erigon"
    exit 1
fi

# Check Go version
echo "📋 Go version:"
go version

# Clean everything first
echo "🧹 Cleaning previous builds..."
rm -rf build/bin/erigon 2>/dev/null || true
go clean -cache 2>/dev/null || true

# Create build directory
mkdir -p build/bin

# Method 1: Try simple go build
echo "🔨 Method 1: Simple go build..."
cd cmd/erigon
go build -o ../../build/bin/erigon
cd ../..

if [ -f "build/bin/erigon" ]; then
    echo "✅ Simple build successful!"
    ls -la build/bin/erigon
    echo ""
    echo "🚀 Testing binary..."
    ./build/bin/erigon --help | head -10
    echo ""
    echo "🎯 Ready to run Olym3 Testnet Season 3!"
    exit 0
fi

# Method 2: Try with minimal flags
echo "🔨 Method 2: Build with minimal flags..."
cd cmd/erigon
CGO_ENABLED=1 go build -ldflags "-s -w" -o ../../build/bin/erigon
cd ../..

if [ -f "build/bin/erigon" ]; then
    echo "✅ Minimal flags build successful!"
    ls -la build/bin/erigon
    exit 0
fi

# Method 3: Try without CGO
echo "🔨 Method 3: Build without CGO..."
cd cmd/erigon
CGO_ENABLED=0 go build -o ../../build/bin/erigon
cd ../..

if [ -f "build/bin/erigon" ]; then
    echo "✅ No-CGO build successful!"
    ls -la build/bin/erigon
    exit 0
fi

# Method 4: Try with specific Go version
echo "🔨 Method 4: Try with specific Go build tags..."
cd cmd/erigon
go build -tags "netgo" -o ../../build/bin/erigon
cd ../..

if [ -f "build/bin/erigon" ]; then
    echo "✅ Netgo build successful!"
    ls -la build/bin/erigon
    exit 0
fi

echo "❌ All build methods failed."
echo ""
echo "💡 Troubleshooting steps:"
echo "1. Check Go installation: go version"
echo "2. Update Go: sudo apt update && sudo apt upgrade golang-go"
echo "3. Check dependencies: sudo apt install build-essential"
echo "4. Try Docker build: ./docker_build.sh"
echo "5. Check disk space: df -h"
echo "6. Check memory: free -h"

exit 1
