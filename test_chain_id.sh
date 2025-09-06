#!/bin/bash

# Test Chain ID for Olym3 Testnet Season 3
# This script tests the correct chain ID

echo "🔍 Testing Olym3 Testnet Season 3 Chain ID..."

# Kill any existing Erigon processes
echo "🛑 Stopping any existing Erigon processes..."
pkill -f erigon 2>/dev/null || true
sleep 2

# Start Erigon in background
echo "🚀 Starting Olym3 Testnet Season 3..."
./build/bin/erigon --chain=olym3-testnet-s3 --datadir=olym3-data --http.api=eth,erigon,web3,net,debug,trace,txpool --torrent.port=42070 &
ERIGON_PID=$!

# Wait for startup
echo "⏳ Waiting for Erigon to start..."
sleep 10

# Test chain ID multiple times
echo "🔍 Testing Chain ID..."
for i in {1..5}; do
    echo "Attempt $i:"
    CHAIN_ID_RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id":1}' localhost:8545)
    echo "Response: $CHAIN_ID_RESPONSE"
    
    if [[ $CHAIN_ID_RESPONSE == *"0x3e803"* ]]; then
        echo "🎉 SUCCESS! Chain ID is correct: 256003 (0x3e803)"
        break
    elif [[ $CHAIN_ID_RESPONSE == *"0x1"* ]]; then
        echo "⚠️ Still showing mainnet chain ID (0x1). Waiting..."
        sleep 5
    else
        echo "❓ Unexpected response: $CHAIN_ID_RESPONSE"
        sleep 5
    fi
done

# Test other RPC methods
echo ""
echo "🔍 Testing other RPC methods..."
echo "Network version:"
curl -s -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "net_version", "params": [], "id":1}' localhost:8545

echo ""
echo "Block number:"
curl -s -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id":1}' localhost:8545

echo ""
echo "Gas price:"
curl -s -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_gasPrice", "params": [], "id":1}' localhost:8545

# Kill the process
echo ""
echo "🛑 Stopping Erigon..."
kill $ERIGON_PID 2>/dev/null || true
sleep 2

echo ""
echo "🎯 Olym3 Testnet Season 3 test completed!"
echo "   If chain ID shows 0x3e803, then it's working correctly!"
echo "   If it shows 0x1, there might be a configuration issue."
