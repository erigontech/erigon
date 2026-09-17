package trie

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

//go:embed proofResponse.json
var responseJson string

func TestPrintProof(t *testing.T) {
	var proof accounts.AccProofResult
	require.NoError(t, json.Unmarshal([]byte(responseJson), &proof))

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
		require.NoError(t, PrintProof(storageProof.Proof))
	}
}

func TestVerifyStorageProofRejectsValueForAbsentKey(t *testing.T) {
	tr := New(common.Hash{})
	present := crypto.Keccak256([]byte("present"))
	enc, err := rlp.EncodeToBytes([]byte{0x2a})
	require.NoError(t, err)
	tr.Update(present, enc)
	root := tr.Hash()

	prove := func(key []byte, value uint64) error {
		nodes, err := tr.Prove(key, 0, false)
		require.NoError(t, err)
		proof := accounts.StorProofResult{Value: (*hexutil.U256)(uint256.NewInt(value))}
		for _, n := range nodes {
			proof.Proof = append(proof.Proof, n)
		}
		return VerifyStorageProofByHash(root, common.BytesToHash(key), proof)
	}

	absent := crypto.Keccak256([]byte("absent"))
	require.NoError(t, prove(present, 0x2a))
	require.NoError(t, prove(absent, 0))
	require.Error(t, prove(absent, 0x2a))

	nodes, err := tr.Prove(absent, 0, false)
	require.NoError(t, err)
	var noValue accounts.StorProofResult
	for _, n := range nodes {
		noValue.Proof = append(noValue.Proof, n)
	}
	require.NoError(t, VerifyStorageProofByHash(root, common.BytesToHash(absent), noValue), "a missing value is zero")
}
