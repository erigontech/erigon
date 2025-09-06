#!/bin/bash

# Fix Go toolchain issues for Olym3 Testnet Season 3
# This script fixes Go version and toolchain problems

echo "🔧 Fixing Go toolchain for Olym3 Testnet Season 3..."

# Check current Go version
echo "📋 Current Go version:"
go version

# Check Go environment
echo "📋 Go environment:"
go env GOROOT
go env GOPATH
go env GOTOOLCHAIN

# Method 1: Set Go toolchain to local
echo "🔨 Method 1: Setting Go toolchain to local..."
export GOTOOLCHAIN=local
go version

# Try building with local toolchain
echo "🔨 Building with local toolchain..."
make clean 2>/dev/null || true
make erigon

if [ -f "build/bin/erigon" ]; then
    echo "✅ Build successful with local toolchain!"
    exit 0
fi

# Method 2: Install Go 1.21 manually
echo "🔨 Method 2: Installing Go 1.21 manually..."
cd /tmp

# Download Go 1.21.5
wget https://go.dev/dl/go1.21.5.linux-amd64.tar.gz

if [ -f "go1.21.5.linux-amd64.tar.gz" ]; then
    # Remove old Go
    rm -rf /usr/local/go
    
    # Install new Go
    tar -C /usr/local -xzf go1.21.5.linux-amd64.tar.gz
    
    # Set environment
    export PATH=/usr/local/go/bin:$PATH
    export GOTOOLCHAIN=local
    
    # Add to bashrc
    echo 'export PATH=/usr/local/go/bin:$PATH' >> ~/.bashrc
    echo 'export GOTOOLCHAIN=local' >> ~/.bashrc
    
    echo "✅ Go 1.21.5 installed successfully!"
    go version
    
    # Clean up
    rm -f go1.21.5.linux-amd64.tar.gz
    
    # Try building again
    cd /home/erigon/erigon
    make clean 2>/dev/null || true
    make erigon
    
    if [ -f "build/bin/erigon" ]; then
        echo "✅ Build successful with Go 1.21.5!"
        exit 0
    fi
fi

# Method 3: Use system Go with specific flags
echo "🔨 Method 3: Using system Go with specific flags..."
cd /home/erigon/erigon

# Set Go flags to avoid toolchain issues
export GOTOOLCHAIN=local
export GOFLAGS="-buildvcs=false"

# Clean and build
make clean 2>/dev/null || true
cd cmd/erigon
go build -o ../../build/bin/erigon
cd ../..

if [ -f "build/bin/erigon" ]; then
    echo "✅ Build successful with system Go!"
    exit 0
fi

# Method 4: Use Docker build
echo "🔨 Method 4: Using Docker build..."
./docker_build.sh

if [ -f "build/bin/erigon" ]; then
    echo "✅ Docker build successful!"
    exit 0
fi

echo "❌ All Go toolchain fix methods failed."
echo ""
echo "💡 Manual suggestions:"
echo "1. Update system: sudo apt update && sudo apt upgrade"
echo "2. Install Go manually: wget https://go.dev/dl/go1.21.5.linux-amd64.tar.gz"
echo "3. Use Docker: ./docker_build.sh"
echo "4. Check disk space: df -h"
echo "5. Check memory: free -h"

exit 1
