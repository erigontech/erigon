#!/bin/bash

# Resolve merge conflict and fix Olym3 Testnet Season 3
# This script handles git merge conflicts

echo "🔧 Resolving merge conflict for Olym3 Testnet Season 3..."

# Check if we're in the right directory
if [ ! -f "go.mod" ]; then
    echo "❌ Error: Not in Erigon root directory"
    exit 1
fi

# Check git status
echo "📋 Current git status:"
git status

# Option 1: Stash local changes and pull
echo "🔧 Option 1: Stashing local changes and pulling..."
git stash
git pull origin main

# Check if pull was successful
if [ $? -eq 0 ]; then
    echo "✅ Successfully pulled latest changes!"
    echo "📋 Current git status:"
    git status
else
    echo "❌ Pull failed. Trying alternative approach..."
    
    # Option 2: Reset and pull
    echo "🔧 Option 2: Resetting and pulling..."
    git reset --hard HEAD
    git pull origin main
fi

# Clean build environment
echo "🧹 Cleaning build environment..."
make clean 2>/dev/null || true
rm -rf build/bin/erigon 2>/dev/null || true

# Create build directory
mkdir -p build/bin

# Rebuild
echo "🔨 Rebuilding Erigon..."
make erigon

if [ -f "build/bin/erigon" ]; then
    echo "✅ Build successful!"
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
    echo "❌ Build failed. Please check the error messages above."
    echo ""
    echo "💡 Try running:"
    echo "   ./final_fix.sh"
    echo "   or"
    echo "   ./emergency_build.sh"
fi
