package trie

import (
	_ "embed"
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
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
