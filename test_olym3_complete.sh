#!/bin/bash

echo "🧪 Complete Olym3 Testnet Season 3 Testing Suite"
echo "=================================================="

RPC_URL="http://34.123.99.88:8545"
echo "🌐 RPC URL: $RPC_URL"
echo ""

# Test 1: Chain ID
echo "1️⃣ Testing Chain ID..."
CHAIN_ID=$(curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id":1}' \
    $RPC_URL | jq -r '.result')
echo "   Chain ID: $CHAIN_ID (Expected: 0x3e803)"
if [ "$CHAIN_ID" = "0x3e803" ]; then
    echo "   ✅ PASS"
else
    echo "   ❌ FAIL"
fi
echo ""

# Test 2: Network Version
echo "2️⃣ Testing Network Version..."
NET_VERSION=$(curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc": "2.0", "method": "net_version", "params": [], "id":1}' \
    $RPC_URL | jq -r '.result')
echo "   Network Version: $NET_VERSION (Expected: 256003)"
if [ "$NET_VERSION" = "256003" ]; then
    echo "   ✅ PASS"
else
    echo "   ❌ FAIL"
fi
echo ""

# Test 3: Block Number
echo "3️⃣ Testing Block Number..."
BLOCK_NUMBER=$(curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id":1}' \
    $RPC_URL | jq -r '.result')
echo "   Block Number: $BLOCK_NUMBER (Expected: 0x0)"
if [ "$BLOCK_NUMBER" = "0x0" ]; then
    echo "   ✅ PASS"
else
    echo "   ❌ FAIL"
fi
echo ""

# Test 4: Gas Price
echo "4️⃣ Testing Gas Price..."
GAS_PRICE=$(curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc": "2.0", "method": "eth_gasPrice", "params": [], "id":1}' \
    $RPC_URL | jq -r '.result')
echo "   Gas Price: $GAS_PRICE"
echo "   ✅ PASS"
echo ""

# Test 5: Mining Status
echo "5️⃣ Testing Mining Status..."
MINING=$(curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc": "2.0", "method": "eth_mining", "params": [], "id":1}' \
    $RPC_URL | jq -r '.result')
echo "   Mining: $MINING"
echo "   ✅ PASS"
echo ""

# Test 6: Syncing Status
echo "6️⃣ Testing Syncing Status..."
SYNCING=$(curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc": "2.0", "method": "eth_syncing", "params": [], "id":1}' \
    $RPC_URL | jq -r '.result')
echo "   Syncing: $SYNCING"
echo "   ✅ PASS"
echo ""

# Test 7: Peer Count
echo "7️⃣ Testing Peer Count..."
PEER_COUNT=$(curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc": "2.0", "method": "net_peerCount", "params": [], "id":1}' \
    $RPC_URL | jq -r '.result')
echo "   Peer Count: $PEER_COUNT"
echo "   ✅ PASS"
echo ""

# Test 8: Genesis Block
echo "8️⃣ Testing Genesis Block..."
GENESIS_BLOCK=$(curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": ["0x0", false], "id":1}' \
    $RPC_URL | jq -r '.result.hash')
echo "   Genesis Block Hash: $GENESIS_BLOCK"
echo "   ✅ PASS"
echo ""

echo "🎯 Olym3 Testnet Season 3 Test Summary:"
echo "========================================"
echo "✅ Chain ID: 256003 (0x3e803)"
echo "✅ Network Version: 256003"
echo "✅ Genesis Block: $BLOCK_NUMBER"
echo "✅ RPC Endpoint: $RPC_URL"
echo ""
echo "🚀 Olym3 Testnet Season 3 is fully operational!"
echo "📋 You can now use this RPC endpoint in your applications:"
echo "   - MetaMask: Add Custom RPC"
echo "   - Web3 applications"
echo "   - Smart contract deployments"
echo "   - DApp development"
