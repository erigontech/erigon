package trie

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

//go:embed proofResponse.json
var responseJson string

func TestProofMapMainnetNodes(t *testing.T) {
	var proof accounts.AccProofResult
	require.NoError(t, json.Unmarshal([]byte(responseJson), &proof))
	nodes := proof.AccountProof
	for _, sp := range proof.StorageProof {
		nodes = append(nodes, sp.Proof...)
	}
	require.Len(t, nodes, 12)
	m, _, err := proofMap(nodes)
	require.NoError(t, err)
	require.Len(t, m, 12)
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
	proof, value, err := ProofFromNodes(map[string][]byte{}, empty.RootHash[:], crypto.Keccak256([]byte("any")))
	require.NoError(t, err)
	require.Empty(t, proof)
	require.Nil(t, value)
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

// A child whose encoding is under 32 bytes sits inside its parent, so it is not a proof element of
// its own. Short keys reach that shape; the 32-byte hashed keys eth_getProof uses do not.
func TestProofFromNodesSkipsInlineChildren(t *testing.T) {
	tr := New(common.Hash{})
	keys := make([][]byte, 8)
	for i := range keys {
		keys[i] = crypto.Keccak256([]byte{byte(i)})[:1]
		tr.Update(keys[i], []byte{byte(i) | 1})
	}
	root := tr.Hash()

	byHash := map[string][]byte{}
	for _, k := range keys {
		nodes, err := tr.Prove(k, 0, false)
		require.NoError(t, err)
		for _, n := range nodes {
			byHash[string(crypto.Keccak256(n))] = n
		}
	}

	for _, k := range keys {
		got, _, err := ProofFromNodes(byHash, root[:], k)
		require.NoError(t, err)
		for i, n := range got {
			require.True(t, i == 0 || len(n) >= length.Hash,
				"element %d of %d bytes is inline in its parent, so it must not be emitted", i, len(n))
		}
	}
}
