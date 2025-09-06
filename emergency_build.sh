#!/bin/bash

# Emergency build script for Olym3 Testnet Season 3
# This script tries every possible method to build Erigon

echo "🚨 Emergency build for Olym3 Testnet Season 3..."

# Check if we're in the right directory
if [ ! -f "go.mod" ]; then
    echo "❌ Error: Not in Erigon root directory"
    echo "Please run: cd /home/erigon/erigon"
    exit 1
fi

# Clean everything
echo "🧹 Cleaning everything..."
make clean 2>/dev/null || true
go clean -cache 2>/dev/null || true
go clean -modcache 2>/dev/null || true
rm -rf build/bin/erigon 2>/dev/null || true

# Create build directory
mkdir -p build/bin

# Method 1: Try with Go 1.21
echo "🔨 Method 1: Try with Go 1.21..."
if command -v go &> /dev/null; then
    go version
    cd cmd/erigon
    GOTAGS="noswiss" go build -o ../../build/bin/erigon
    cd ../..
    
    if [ -f "build/bin/erigon" ]; then
        echo "✅ Go 1.21 build successful!"
        ls -la build/bin/erigon
        exit 0
    fi
fi

# Method 2: Try with minimal CGO
echo "🔨 Method 2: Try with minimal CGO..."
cd cmd/erigon
CGO_ENABLED=1 go build -ldflags "-s -w" -o ../../build/bin/erigon
cd ../..

if [ -f "build/bin/erigon" ]; then
    echo "✅ Minimal CGO build successful!"
    ls -la build/bin/erigon
    exit 0
fi

# Method 3: Try without CGO
echo "🔨 Method 3: Try without CGO..."
cd cmd/erigon
CGO_ENABLED=0 go build -o ../../build/bin/erigon
cd ../..

if [ -f "build/bin/erigon" ]; then
    echo "✅ No-CGO build successful!"
    ls -la build/bin/erigon
    exit 0
fi

# Method 4: Try with specific build tags
echo "🔨 Method 4: Try with specific build tags..."
cd cmd/erigon
go build -tags "netgo,osusergo" -o ../../build/bin/erigon
cd ../..

if [ -f "build/bin/erigon" ]; then
    echo "✅ Specific tags build successful!"
    ls -la build/bin/erigon
    exit 0
fi

# Method 5: Try with older build approach
echo "🔨 Method 5: Try with older build approach..."
cd cmd/erigon
go build -trimpath=false -buildvcs=false -o ../../build/bin/erigon
cd ../..

if [ -f "build/bin/erigon" ]; then
    echo "✅ Older build approach successful!"
    ls -la build/bin/erigon
    exit 0
fi

# Method 6: Try with gccgo
echo "🔨 Method 6: Try with gccgo..."
if command -v gccgo &> /dev/null; then
    cd cmd/erigon
    CC=gccgo go build -o ../../build/bin/erigon
    cd ../..
    
    if [ -f "build/bin/erigon" ]; then
        echo "✅ GCCGO build successful!"
        ls -la build/bin/erigon
        exit 0
    fi
fi

# Method 7: Try with static build
echo "🔨 Method 7: Try with static build..."
cd cmd/erigon
CGO_ENABLED=0 go build -ldflags "-s -w -extldflags '-static'" -o ../../build/bin/erigon
cd ../..

if [ -f "build/bin/erigon" ]; then
    echo "✅ Static build successful!"
    ls -la build/bin/erigon
    exit 0
fi

echo "❌ All emergency build methods failed."
echo ""
echo "🔍 Debug information:"
echo "Go version: $(go version 2>/dev/null || echo 'Go not found')"
echo "Go env: $(go env 2>/dev/null || echo 'Go not found')"
echo "Disk space: $(df -h /)"
echo "Memory: $(free -h)"
echo "Architecture: $(uname -m)"
echo "OS: $(uname -a)"
echo ""
echo "💡 Suggestions:"
echo "1. Install Go 1.21: ./install_go121.sh"
echo "2. Use Docker: ./docker_build.sh"
echo "3. Check if there are multiple Go installations"
echo "4. Try on a different machine/VM"

exit 1
