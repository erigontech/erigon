package trie

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
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

func TestProofFromNodesMatchesProve(t *testing.T) {
	for _, valueLen := range []int{1, 40} {
		tr := New(common.Hash{})
		keys, values := make([][]byte, 300), make([][]byte, 300)
		for i := range keys {
			keys[i] = crypto.Keccak256([]byte{byte(i), byte(i >> 8)})
			if i%3 != 0 {
				values[i] = bytes.Repeat([]byte{byte(i) | 1}, valueLen)
				tr.Update(keys[i], values[i])
			}
		}
		byHash, want := map[string][]byte{}, make([][][]byte, len(keys))
		for i, key := range keys {
			nodes, err := tr.Prove(key, 0, false)
			require.NoError(t, err)
			want[i] = nodes
			for _, n := range nodes {
				byHash[string(crypto.Keccak256(n))] = n
			}
		}
		root := tr.Hash()
		for i, key := range keys {
			got, value, err := ProofFromNodes(byHash, root[:], key)
			require.NoError(t, err)
			require.Equal(t, want[i], got, "key %d", i)
			var wantValue []byte
			if values[i] != nil { // a leaf holds its value as an RLP string, which is what an account proof decodes
				wantValue = make([]byte, rlp.StringLen(values[i]))
				rlp.EncodeStringToBuf(values[i], wantValue)
			}
			require.Equal(t, wantValue, value, "key %d", i)
		}
	}
}

func TestProofFromNodesEmptyTrie(t *testing.T) {
	proof, value, err := ProofFromNodes(map[string][]byte{}, EmptyRoot[:], crypto.Keccak256([]byte("any")))
	require.NoError(t, err)
	require.Empty(t, proof)
	require.Nil(t, value)
}
