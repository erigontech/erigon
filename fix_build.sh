#!/bin/bash

# Fix for Go runtime Swiss table conflicts
# This script addresses the mapiterinit redeclared errors

echo "🔧 Fixing Go build issues for Olym3 Testnet Season 3..."

# Check Go version
echo "📋 Checking Go version..."
go version

# Clean previous builds
echo "🧹 Cleaning previous builds..."
make clean 2>/dev/null || true
rm -rf build/bin/erigon 2>/dev/null || true

# Set environment variables to avoid Swiss table conflicts
export GOFLAGS="-buildvcs=false"
export CGO_ENABLED=1

# Try building with specific Go build tags
echo "🔨 Building Erigon with Olym3 Testnet Season 3 support..."

# Method 1: Build without Swiss table optimization
GOFLAGS="-buildvcs=false" make erigon

# If that fails, try Method 2: Build with specific tags
if [ ! -f "build/bin/erigon" ]; then
    echo "⚠️  First build attempt failed, trying alternative method..."
    
    # Clean again
    make clean 2>/dev/null || true
    
    # Build with specific build tags to avoid Swiss table conflicts
    cd cmd/erigon
    GOARCH=amd64 GOAMD64=v2 \
    CGO_CFLAGS="-O2 -g -DMDBX_FORCE_ASSERTIONS=0 -DMDBX_DISABLE_VALIDATION=0 -DMDBX_ENV_CHECKPID=0 -D__BLST_PORTABLE__ -Wno-unknown-warning-option -Wno-enum-int-mismatch -Wno-strict-prototypes -Wno-unused-but-set-variable -O3" \
    CGO_LDFLAGS="-O2 -g -O3 -g" \
    GOPRIVATE="github.com/erigontech/silkworm-go" \
    go build -trimpath -buildvcs=false \
    -ldflags "-X github.com/erigontech/erigon/db/version.GitCommit=$(git rev-parse HEAD) -X github.com/erigontech/erigon/db/version.GitBranch=$(git branch --show-current) -X github.com/erigontech/erigon/db/version.GitTag=" \
    -tags "noswiss" \
    -o ../../build/bin/erigon
    
    cd ../..
fi

# Check if build was successful
if [ -f "build/bin/erigon" ]; then
    echo "✅ Build successful! Erigon binary created."
    echo "📊 Binary info:"
    ls -la build/bin/erigon
    echo ""
    echo "🚀 Testing Olym3 Testnet Season 3..."
    
    # Test the binary
    ./build/bin/erigon --help | head -20
    
    echo ""
    echo "🎯 To run Olym3 Testnet Season 3:"
    echo "   ./build/bin/erigon --chain=olym3-testnet-s3 --datadir=olym3-data --http.api=eth,erigon,web3,net,debug,trace,txpool"
    
else
    echo "❌ Build failed. Trying alternative approach..."
    
    # Method 3: Use go build directly with minimal flags
    echo "🔨 Attempting minimal build..."
    
    cd cmd/erigon
    go build -o ../../build/bin/erigon
    cd ../..
    
    if [ -f "build/bin/erigon" ]; then
        echo "✅ Minimal build successful!"
    else
        echo "❌ All build methods failed."
        echo "💡 Suggestions:"
        echo "   1. Update Go to version 1.21+ or 1.22+"
        echo "   2. Try: go clean -cache"
        echo "   3. Check if there are any Go module issues"
        echo "   4. Consider using Docker build instead"
    fi
fi

echo "🏁 Build process completed."
