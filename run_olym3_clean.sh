#!/bin/bash

# Clean run script for Olym3 Testnet Season 3
# This script kills existing processes and runs with clean ports

echo "🧹 Clean run for Olym3 Testnet Season 3..."

# Kill any existing Erigon processes
echo "🛑 Stopping any existing Erigon processes..."
pkill -f erigon 2>/dev/null || true
pkill -f rpcdaemon 2>/dev/null || true
sleep 3

# Kill processes using specific ports
echo "🔌 Freeing up ports..."
lsof -ti:8545 | xargs kill -9 2>/dev/null || true
lsof -ti:30303 | xargs kill -9 2>/dev/null || true
lsof -ti:30304 | xargs kill -9 2>/dev/null || true
lsof -ti:9090 | xargs kill -9 2>/dev/null || true
lsof -ti:42069 | xargs kill -9 2>/dev/null || true
lsof -ti:42070 | xargs kill -9 2>/dev/null || true

sleep 2

# Check if we're in the right directory
if [ ! -f "build/bin/erigon" ]; then
    echo "❌ Error: Erigon binary not found. Please build first."
    exit 1
fi

echo "🚀 Starting Olym3 Testnet Season 3 with clean ports..."

# Run Erigon with different ports to avoid conflicts
./build/bin/erigon \
    --chain=olym3-testnet-s3 \
    --datadir=olym3-data \
    --http.api=eth,erigon,web3,net,debug,trace,txpool \
    --http.port=8546 \
    --private.api.addr=127.0.0.1:9091 \
    --torrent.port=42071 \
    --port=30305 \
    --authrpc.port=30306 \
    --mine \
    --miner.etherbase=0x1234567890123456789012345678901234567890
