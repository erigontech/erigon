package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"

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
	fmt.Printf("Genesis Hash (without 0x): %s\n", genesisHash.Hex()[2:])
	
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
	
	// Save to file for easy copy
	output := fmt.Sprintf(`// Olym3 Testnet Season 3 Genesis Hash
GenesisHash: common.HexToHash("%s"),
GenesisStateRoot: common.HexToHash("%s"),`, genesisHash.Hex(), stateRoot.Hex())
	
	err := os.WriteFile("genesis_hash_output.txt", []byte(output), 0644)
	if err != nil {
		log.Printf("Error writing file: %v", err)
	} else {
		fmt.Printf("\nGenesis hash saved to genesis_hash_output.txt\n")
	}
}
