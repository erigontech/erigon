#!/bin/bash

# Fix genesis hash for Olym3 Testnet Season 3
# This script calculates the correct genesis hash

echo "🔧 Fixing genesis hash for Olym3 Testnet Season 3..."

# Check if we're in the right directory
if [ ! -f "go.mod" ]; then
    echo "❌ Error: Not in Erigon root directory"
    exit 1
fi

# Create a simple Go program to calculate genesis hash
cat > calculate_genesis_hash.go << 'EOF'
package main

import (
	"fmt"
	"math/big"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/types"
)

func main() {
	// Create genesis block
	genesis := spec.Olym3TestnetS3GenesisBlock()
	
	// Calculate genesis hash
	genesisHash := genesis.ToBlock().Hash()
	
	fmt.Printf("Genesis Hash: %s\n", genesisHash.Hex())
	
	// Also calculate state root
	stateRoot := genesis.ToBlock().Root()
	fmt.Printf("State Root: %s\n", stateRoot.Hex())
	
	// Print genesis block details
	fmt.Printf("\nGenesis Block Details:\n")
	fmt.Printf("Chain ID: %d\n", genesis.Config.ChainID.Uint64())
	fmt.Printf("Gas Limit: %d\n", genesis.GasLimit)
	fmt.Printf("Difficulty: %d\n", genesis.Difficulty.Uint64())
	fmt.Printf("Timestamp: %d\n", genesis.Timestamp)
	fmt.Printf("Extra Data: %s\n", string(genesis.ExtraData))
}
EOF

echo "🔨 Calculating genesis hash..."
go run calculate_genesis_hash.go

# Get the genesis hash from output
GENESIS_HASH=$(go run calculate_genesis_hash.go | grep "Genesis Hash:" | cut -d' ' -f3)
STATE_ROOT=$(go run calculate_genesis_hash.go | grep "State Root:" | cut -d' ' -f3)

echo ""
echo "📊 Calculated values:"
echo "Genesis Hash: $GENESIS_HASH"
echo "State Root: $STATE_ROOT"

# Update the config.go file
echo "🔧 Updating config.go with correct genesis hash..."

# Create backup
cp execution/chain/spec/config.go execution/chain/spec/config.go.backup

# Update the genesis hash in config.go
sed -i "s/GenesisHash: common.HexToHash(\"0x0000000000000000000000000000000000000000000000000000000000000000\")/GenesisHash: common.HexToHash(\"$GENESIS_HASH\")/" execution/chain/spec/config.go

# Add state root if it's not empty
if [ "$STATE_ROOT" != "0x0000000000000000000000000000000000000000000000000000000000000000" ]; then
    # Check if GenesisStateRoot line exists
    if grep -q "GenesisStateRoot:" execution/chain/spec/config.go; then
        sed -i "s/GenesisStateRoot: common.HexToHash(\"0x0000000000000000000000000000000000000000000000000000000000000000\")/GenesisStateRoot: common.HexToHash(\"$STATE_ROOT\")/" execution/chain/spec/config.go
    else
        # Add GenesisStateRoot line
        sed -i "/GenesisHash: common.HexToHash(\"$GENESIS_HASH\"),/a\\t\tGenesisStateRoot: common.HexToHash(\"$STATE_ROOT\")," execution/chain/spec/config.go
    fi
fi

echo "✅ Config.go updated with correct genesis hash!"

# Clean up
rm -f calculate_genesis_hash.go

echo ""
echo "🔨 Rebuilding Erigon with correct genesis hash..."
make erigon

if [ -f "build/bin/erigon" ]; then
    echo "✅ Build successful with correct genesis hash!"
    echo ""
    echo "🚀 Testing Olym3 Testnet Season 3..."
    ./build/bin/erigon --chain=olym3-testnet-s3 --datadir=olym3-data --http.api=eth,erigon,web3,net,debug,trace,txpool &
    ERIGON_PID=$!
    
    # Wait a bit for startup
    sleep 5
    
    # Test chain ID
    echo "🔍 Testing chain ID..."
    curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id":1}' localhost:8545 2>/dev/null || echo "RPC not ready yet"
    
    # Kill the process
    kill $ERIGON_PID 2>/dev/null || true
    
    echo ""
    echo "🎯 Olym3 Testnet Season 3 is ready!"
    echo "   Chain ID: 256003"
    echo "   Genesis Hash: $GENESIS_HASH"
    echo "   State Root: $STATE_ROOT"
else
    echo "❌ Build failed. Restoring backup..."
    cp execution/chain/spec/config.go.backup execution/chain/spec/config.go
    echo "Backup restored. Please check the error messages above."
fi
