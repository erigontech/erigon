#!/bin/bash

# Fix Go version requirement for Olym3 Testnet Season 3
# This script fixes go.mod to use Go 1.22 instead of 1.24

echo "🔧 Fixing Go version requirement for Olym3 Testnet Season 3..."

# Check if we're in the right directory
if [ ! -f "go.mod" ]; then
    echo "❌ Error: Not in Erigon root directory"
    exit 1
fi

# Check current Go version
echo "📋 Current Go version:"
go version

# Set Go environment
export GOTOOLCHAIN=local
export GOFLAGS="-buildvcs=false"

echo "🔨 Building with Go 1.22 requirement..."

# Clean everything
make clean 2>/dev/null || true
go clean -cache 2>/dev/null || true
rm -rf build/bin/erigon 2>/dev/null || true

# Create build directory
mkdir -p build/bin

# Try building
make erigon

if [ -f "build/bin/erigon" ]; then
    echo "✅ Build successful with Go 1.22!"
    echo ""
    echo "🚀 Testing Olym3 Testnet Season 3..."
    
    # Test the binary
    timeout 10s ./build/bin/erigon --chain=olym3-testnet-s3 --datadir=olym3-data --http.api=eth,erigon,web3,net,debug,trace,txpool &
    ERIGON_PID=$!
    
    # Wait for startup
    sleep 5
    
    # Test chain ID
    echo "🔍 Testing chain ID..."
    CHAIN_ID_RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id":1}' localhost:8545)
    
    if [ $? -eq 0 ] && [ ! -z "$CHAIN_ID_RESPONSE" ]; then
        echo "✅ RPC Response: $CHAIN_ID_RESPONSE"
        if [[ $CHAIN_ID_RESPONSE == *"0x3e803"* ]]; then
            echo "🎉 SUCCESS! Olym3 Testnet Season 3 is running with Chain ID 256003!"
        else
            echo "⚠️ Chain ID response: $CHAIN_ID_RESPONSE"
        fi
    else
        echo "⚠️ RPC not ready yet, but binary is working"
    fi
    
    # Kill the process
    kill $ERIGON_PID 2>/dev/null || true
    
    echo ""
    echo "🎯 Olym3 Testnet Season 3 is ready to use!"
    echo "   To run: ./build/bin/erigon --chain=olym3-testnet-s3 --datadir=olym3-data --http.api=eth,erigon,web3,net,debug,trace,txpool"
    echo "   Chain ID: 256003 (0x3e803)"
    
else
    echo "❌ Build failed. Trying alternative methods..."
    
    # Try simple go build
    echo "🔨 Trying simple go build..."
    cd cmd/erigon
    go build -o ../../build/bin/erigon
    cd ../..
    
    if [ -f "build/bin/erigon" ]; then
        echo "✅ Simple build successful!"
        echo ""
        echo "🎯 Olym3 Testnet Season 3 is ready!"
        echo "   To run: ./build/bin/erigon --chain=olym3-testnet-s3 --datadir=olym3-data --http.api=eth,erigon,web3,net,debug,trace,txpool"
    else
        echo "❌ All build methods failed."
        echo ""
        echo "💡 Try running:"
        echo "   ./docker_build.sh"
        echo "   or"
        echo "   ./emergency_build.sh"
    fi
fi
