package trie

import (
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/stretchr/testify/require"
)

func TestProofRetainerConstruction(t *testing.T) {
	rl := NewRetainList(0)
	pr, err := NewProofRetainer(
		libcommon.Address{0x1},
		&accounts.Account{
			Initialised: true,
			Nonce:       2,
			Balance:     *uint256.NewInt(6e9),
			CodeHash:    libcommon.Hash{3},
			Incarnation: 3,
		},
		[]libcommon.Hash{{1}, {2}, {3}},
		rl,
	)
	require.NoError(t, err)
	require.Len(t, rl.hexes, 4)

	validKeys := [][]byte{
		{},
		pr.accHexKey[:15],
		pr.accHexKey[:],
		pr.storageHexKeys[0][:85],
		pr.storageHexKeys[0][:],
		pr.storageHexKeys[1][:90],
		pr.storageHexKeys[1][:],
		pr.storageHexKeys[2][:98],
		pr.storageHexKeys[2][:95],
		pr.storageHexKeys[2][:],
	}

	invalidKeys := [][]byte{
		pr.accHexKey[1:16],
		pr.storageHexKeys[0][12:80],
		pr.storageHexKeys[2][19:90],
	}

	for _, key := range validKeys {
		pe := pr.ProofElement(key)
		require.NotNil(t, pe)
		require.Equal(t, pe.hexKey, key)
		switch len(key) {
		case 64: // Account leaf key
			pe.storageRoot = libcommon.Hash{3}
		case 144: // Storage leaf key
			pe.storageValue = uint256.NewInt(5)
		}
		pe.proof.Write(key)
	}
	for _, key := range invalidKeys {
		pe := pr.ProofElement(key)
		require.Nil(t, pe)
	}
	require.Equal(t, len(validKeys), len(pr.proofs))

	accProof, err := pr.ProofResult()
	require.NoError(t, err)

	require.Len(t, accProof.AccountProof, 3)
	require.Equal(t, []byte(nil), []byte(accProof.AccountProof[0]))
	require.Equal(t, validKeys[1], []byte(accProof.AccountProof[1]))
	require.Equal(t, validKeys[2], []byte(accProof.AccountProof[2]))

	require.Len(t, accProof.StorageProof, 3)
	require.Equal(t, accProof.StorageProof[0].Key, libcommon.Hash{1})
	require.Len(t, accProof.StorageProof[0].Proof, 2)
	require.Equal(t, validKeys[3], []byte(accProof.StorageProof[0].Proof[0]))
	require.Equal(t, validKeys[4], []byte(accProof.StorageProof[0].Proof[1]))

	require.Equal(t, accProof.StorageProof[1].Key, libcommon.Hash{2})
	require.Len(t, accProof.StorageProof[1].Proof, 2)
	require.Equal(t, validKeys[5], []byte(accProof.StorageProof[1].Proof[0]))
	require.Equal(t, validKeys[6], []byte(accProof.StorageProof[1].Proof[1]))

	require.Equal(t, accProof.StorageProof[2].Key, libcommon.Hash{3})
	require.Len(t, accProof.StorageProof[2].Proof, 3)
	require.Equal(t, validKeys[7], []byte(accProof.StorageProof[2].Proof[0]))
	require.Equal(t, validKeys[8], []byte(accProof.StorageProof[2].Proof[1]))
	require.Equal(t, validKeys[9], []byte(accProof.StorageProof[2].Proof[2]))

	t.Run("missingStorageRoot", func(t *testing.T) {
		oldStorageHash := pr.proofs[2].storageRoot
		pr.proofs[2].storageRoot = libcommon.Hash{}
		defer func() { pr.proofs[2].storageRoot = oldStorageHash }()
		_, err := pr.ProofResult()
		require.Error(t, err, "did not find storage root in proof elements")
	})

	t.Run("missingStorageValue", func(t *testing.T) {
		oldKey := pr.proofs[4].storageValue
		pr.proofs[4].storageValue = nil
		defer func() { pr.proofs[4].storageValue = oldKey }()
		_, err := pr.ProofResult()
		require.Error(t, err, "no storage value for storage key 0x%x", validKeys[4])
	})
}
