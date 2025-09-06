#!/bin/bash

# Test RPC for Olym3 Testnet Season 3
# This script tests various RPC endpoints

echo "🔍 Testing Olym3 Testnet Season 3 RPC endpoints..."

# Test Chain ID
echo "1. Testing Chain ID:"
CHAIN_ID=$(curl -s -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id":1}' localhost:8546)
echo "   Response: $CHAIN_ID"

# Test Network Version
echo "2. Testing Network Version:"
NET_VERSION=$(curl -s -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "net_version", "params": [], "id":1}' localhost:8546)
echo "   Response: $NET_VERSION"

# Test Block Number
echo "3. Testing Block Number:"
BLOCK_NUMBER=$(curl -s -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id":1}' localhost:8546)
echo "   Response: $BLOCK_NUMBER"

# Test Gas Price
echo "4. Testing Gas Price:"
GAS_PRICE=$(curl -s -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_gasPrice", "params": [], "id":1}' localhost:8546)
echo "   Response: $GAS_PRICE"

# Test Mining Status
echo "5. Testing Mining Status:"
MINING=$(curl -s -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_mining", "params": [], "id":1}' localhost:8546)
echo "   Response: $MINING"

# Test Account Balance
echo "6. Testing Account Balance:"
BALANCE=$(curl -s -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_getBalance", "params": ["0x1234567890123456789012345678901234567890", "latest"], "id":1}' localhost:8546)
echo "   Response: $BALANCE"

# Test Peer Count
echo "7. Testing Peer Count:"
PEER_COUNT=$(curl -s -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "net_peerCount", "params": [], "id":1}' localhost:8546)
echo "   Response: $PEER_COUNT"

echo ""
echo "🎯 Summary:"
if [[ $CHAIN_ID == *"0x3e803"* ]]; then
    echo "✅ Chain ID: CORRECT (256003)"
else
    echo "❌ Chain ID: INCORRECT (Expected: 0x3e803, Got: $CHAIN_ID)"
fi

if [[ $NET_VERSION == *"256003"* ]]; then
    echo "✅ Network Version: CORRECT (256003)"
else
    echo "❌ Network Version: INCORRECT (Expected: 256003, Got: $NET_VERSION)"
fi

echo "📊 Block Number: $BLOCK_NUMBER"
echo "⛽ Gas Price: $GAS_PRICE"
echo "⛏️ Mining: $MINING"
echo "💰 Balance: $BALANCE"
echo "👥 Peers: $PEER_COUNT"
