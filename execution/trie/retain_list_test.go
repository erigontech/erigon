// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func FakePreimage(hash common.Hash) common.Hash {
	result := hash
	for i, b := range hash {
		result[i] = b ^ 1
	}
	return result
}

// NewManualProofRetainer is a way to allow external tests in this package to
// manually construct a ProofRetainer based on a set of keys.  This is
// especially useful for tests which want to manually manipulate the hash
// databases without worrying about generating and tracking pre-images.
func NewManualProofRetainer(t *testing.T, acc *accounts.Account, rl *RetainList, keys [][]byte) *DefaultProofRetainer {
	t.Helper()
	var accHexKey []byte
	var storageKeys []common.Hash
	var storageHexKeys [][]byte
	for _, key := range keys {
		switch len(key) {
		case 32:
			require.Nil(t, accHexKey, "only one account key may be provided")
			accHexKey = rl.AddKey(key)
		case 72:
			if accHexKey == nil {
				accHexKey = rl.AddKey(key[:32])
			}
			storageKeys = append(storageKeys, FakePreimage(common.BytesToHash(key[40:])))
			storageHexKeys = append(storageHexKeys, rl.AddKey(key))
			require.Equal(t, accHexKey, storageHexKeys[0][:64], "all storage keys must be for the same account")
		default:
			require.Fail(t, "unexpected key length", len(key))
		}
	}
	return &DefaultProofRetainer{
		rl:             rl,
		acc:            acc,
		accHexKey:      accHexKey,
		storageKeys:    storageKeys,
		storageHexKeys: storageHexKeys,
	}
}

func TestProofRetainerConstruction(t *testing.T) {
	rl := NewRetainList(0)
	pr, err := NewProofRetainer(
		common.Address{0x1},
		&accounts.Account{
			Initialised: true,
			Nonce:       2,
			Balance:     *uint256.NewInt(6e9),
			CodeHash:    common.Hash{3},
			Incarnation: 3,
		},
		[]common.Hash{{1}, {2}, {3}},
		rl,
	)
	require.NoError(t, err)
	require.Len(t, rl.hexes, 4)

	validKeys := [][]byte{
		pr.storageHexKeys[2][:],
		pr.storageHexKeys[2][:98],
		pr.storageHexKeys[2][:95],
		pr.storageHexKeys[1][:],
		pr.storageHexKeys[1][:90],
		pr.storageHexKeys[0][:],
		pr.storageHexKeys[0][:85],
		pr.accHexKey[:],
		pr.accHexKey[:15],
		{},
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
			pe.storageRoot = common.Hash{3}
			pe.storageRootKey = key
		case 144: // Storage leaf key
			pe.storageValue = uint256.NewInt(5)
			pe.storageKey = key[2*(32+8):]
		}
		pe.proof.Write(key)
	}
	for _, key := range invalidKeys {
		pe := pr.ProofElement(key)
		require.Nil(t, pe)
	}
	require.Len(t, pr.proofs, len(validKeys))

	accProof, err := pr.ProofResult()
	require.NoError(t, err)

	require.Len(t, accProof.AccountProof, 3)
	require.Equal(t, []byte(nil), []byte(accProof.AccountProof[0]))
	require.Equal(t, validKeys[8], []byte(accProof.AccountProof[1]))
	require.Equal(t, validKeys[7], []byte(accProof.AccountProof[2]))

	require.Len(t, accProof.StorageProof, 3)
	require.Equal(t, common.Hash{1}, common.HexToHash(accProof.StorageProof[0].Key))
	require.Len(t, accProof.StorageProof[0].Proof, 2)
	require.Equal(t, validKeys[6], []byte(accProof.StorageProof[0].Proof[0]))
	require.Equal(t, validKeys[5], []byte(accProof.StorageProof[0].Proof[1]))

	require.Equal(t, common.Hash{2}, common.HexToHash(accProof.StorageProof[1].Key))
	require.Len(t, accProof.StorageProof[1].Proof, 2)
	require.Equal(t, validKeys[4], []byte(accProof.StorageProof[1].Proof[0]))
	require.Equal(t, validKeys[3], []byte(accProof.StorageProof[1].Proof[1]))

	require.Equal(t, common.Hash{3}, common.HexToHash(accProof.StorageProof[2].Key))
	require.Len(t, accProof.StorageProof[2].Proof, 3)
	require.Equal(t, validKeys[2], []byte(accProof.StorageProof[2].Proof[0]))
	require.Equal(t, validKeys[1], []byte(accProof.StorageProof[2].Proof[1]))
	require.Equal(t, validKeys[0], []byte(accProof.StorageProof[2].Proof[2]))

	t.Run("missingStorageRoot", func(t *testing.T) {
		oldStorageHash := pr.proofs[2].storageRoot
		pr.proofs[2].storageRoot = common.Hash{}
		defer func() { pr.proofs[2].storageRoot = oldStorageHash }()
		_, err := pr.ProofResult()
		require.Error(t, err, "did not find storage root in proof elements")
	})

	t.Run("missingStorageValue", func(t *testing.T) {
		oldKey := pr.proofs[4].storageValue
		pr.proofs[4].storageValue = nil
		defer func() { pr.proofs[4].storageValue = oldKey }()
		accProof, err := pr.ProofResult()
		require.NoError(t, err)
		require.Equal(t, &hexutil.Big{}, accProof.StorageProof[0].Value)
	})
}
