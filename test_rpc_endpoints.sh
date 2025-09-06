#!/bin/bash

echo "🧪 Testing RPC Endpoints for Olym3 Testnet Season 3"
echo "=================================================="

# Test 1: Direct GCP IP
echo "1️⃣ Testing Direct GCP IP (34.123.99.88:8545)..."
GCP_RPC="http://34.123.99.88:8545"
echo "   RPC URL: $GCP_RPC"

CHAIN_ID=$(curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id":1}' \
    $GCP_RPC | jq -r '.result' 2>/dev/null)

if [ "$CHAIN_ID" = "0x3e803" ]; then
    echo "   ✅ PASS - Chain ID: $CHAIN_ID (256003)"
else
    echo "   ❌ FAIL - Chain ID: $CHAIN_ID"
fi
echo ""

# Test 2: Domain RPC (if available)
echo "2️⃣ Testing Domain RPC (rpc3.olym3.xyz)..."
DOMAIN_RPC="https://rpc3.olym3.xyz"
echo "   RPC URL: $DOMAIN_RPC"

# Test with timeout
CHAIN_ID_DOMAIN=$(curl -s --connect-timeout 10 -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id":1}' \
    $DOMAIN_RPC | jq -r '.result' 2>/dev/null)

if [ "$CHAIN_ID_DOMAIN" = "0x3e803" ]; then
    echo "   ✅ PASS - Chain ID: $CHAIN_ID_DOMAIN (256003)"
elif [ -z "$CHAIN_ID_DOMAIN" ]; then
    echo "   ⚠️  WARNING - Domain not accessible or not configured"
else
    echo "   ❌ FAIL - Chain ID: $CHAIN_ID_DOMAIN"
fi
echo ""

# Test 3: Local RPC
echo "3️⃣ Testing Local RPC (localhost:8545)..."
LOCAL_RPC="http://localhost:8545"
echo "   RPC URL: $LOCAL_RPC"

CHAIN_ID_LOCAL=$(curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id":1}' \
    $LOCAL_RPC | jq -r '.result' 2>/dev/null)

if [ "$CHAIN_ID_LOCAL" = "0x3e803" ]; then
    echo "   ✅ PASS - Chain ID: $CHAIN_ID_LOCAL (256003)"
else
    echo "   ❌ FAIL - Chain ID: $CHAIN_ID_LOCAL"
fi
echo ""

echo "📋 RPC Endpoint Summary:"
echo "========================"
echo "✅ Working RPC: $GCP_RPC"
echo "⚠️  Domain RPC: $DOMAIN_RPC (needs configuration)"
echo "🔧 Local RPC: $LOCAL_RPC (for local development)"
echo ""
echo "🚀 Recommended RPC for applications:"
echo "   $GCP_RPC"
echo ""
echo "💡 To fix domain RPC:"
echo "   1. Configure DNS for rpc3.olym3.xyz"
echo "   2. Setup SSL certificate"
echo "   3. Configure reverse proxy (nginx/apache)"
echo "   4. Point domain to 34.123.99.88:8545"
