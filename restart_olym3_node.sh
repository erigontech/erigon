#!/bin/bash

echo "🔄 Restarting Erigon with Olym3 Testnet Season 3..."

# Kill all existing Erigon processes
echo "🔪 Killing existing Erigon processes..."
pkill -f erigon
sleep 2

# Kill processes on specific ports
echo "🔪 Killing processes on ports 8545, 9090, 30303, 30304, 42069..."
lsof -ti:8545 | xargs -r kill -9
lsof -ti:9090 | xargs -r kill -9
lsof -ti:30303 | xargs -r kill -9
lsof -ti:30304 | xargs -r kill -9
lsof -ti:42069 | xargs -r kill -9
sleep 2

# Clean up any remaining processes
echo "🧹 Final cleanup..."
pkill -9 -f erigon
sleep 1

# Start Erigon with Olym3 Testnet Season 3
echo "🚀 Starting Erigon with Olym3 Testnet Season 3..."
echo "Chain: olym3-testnet-s3"
echo "Chain ID: 256003"
echo "Ports: HTTP=8545, Private API=9090, P2P=30303, Torrent=42070"

# Start in background
nohup ./build/bin/erigon \
    --chain=olym3-testnet-s3 \
    --datadir=olym3-data \
    --http.api=eth,erigon,web3,net,debug,trace,txpool \
    --torrent.port=42070 \
    --private.api.addr=127.0.0.1:9090 \
    --port=30303 \
    --http.port=8545 \
    > erigon.log 2>&1 &

echo "⏳ Waiting for Erigon to start..."
sleep 10

# Test the node
echo "🧪 Testing Olym3 Testnet Season 3..."
echo "Testing Chain ID..."
curl -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id":1}' \
    localhost:8545

echo -e "\nTesting Network Version..."
curl -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc": "2.0", "method": "net_version", "params": [], "id":1}' \
    localhost:8545

echo -e "\nTesting Block Number..."
curl -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id":1}' \
    localhost:8545

echo -e "\n✅ Olym3 Testnet Season 3 should now be running!"
echo "📋 Expected results:"
echo "   Chain ID: 0x3e803 (256003)"
echo "   Network Version: 256003"
echo "   Block Number: 0x0 (genesis block)"
