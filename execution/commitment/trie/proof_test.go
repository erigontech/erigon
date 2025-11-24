package trie

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"testing"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/stretchr/testify/require"
)

//go:embed proofResponse.json
var responseJson string

func TestPrintProof(t *testing.T) {

	var proof accounts.AccProofResult
	if err := json.Unmarshal([]byte(responseJson), &proof); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("AccountProof entries: %d\n", len(proof.AccountProof))
	for i, p := range proof.AccountProof {
		fmt.Printf("  [%d] %d bytes\n", i, len(p))
	}
	fmt.Printf("Balance: %s\n", (*proof.Balance).String()) // decimal
	fmt.Printf("Nonce: %d\n", uint64(proof.Nonce))
	fmt.Printf("CodeHash: %s\n", proof.CodeHash.Hex())
	fmt.Printf("StorageHash: %s\n", proof.StorageHash.Hex())
	fmt.Printf("Address: %s\n", proof.Address.Hex())
	hashedKey := crypto.Keccak256Hash(proof.Address[:])
	fmt.Printf("HashedKey: %s", hashedKey.Hex())

	fmt.Printf("AccountProof: \n")
	err := PrintProof(proof.AccountProof)
	require.NoError(t, err)

	if proof.StorageProof == nil {
		return
	}
	fmt.Println()
	fmt.Printf("StorageProof: \n")
	for i, storageProof := range proof.StorageProof {
		fmt.Printf("\t #%d key=%x, value=%v \n", i, storageProof.Key, storageProof.Value)
		err = PrintProof(storageProof.Proof)
	}
	require.NoError(t, err)
}
