// Copyright 2026 The Erigon Authors
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

package types

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/execution/chain"
)

func sampleBlobTx() *BlobTx {
	to := common.HexToAddress("0x0000000000000000000000000000000000000002")
	var bvh common.Hash
	bvh[0] = kzg.BlobCommitmentVersionKZG
	return &BlobTx{
		DynamicFeeTransaction: DynamicFeeTransaction{
			CommonTx:   CommonTx{Nonce: 0, GasLimit: 21_000, To: &to, Value: *uint256.NewInt(1)},
			ChainID:    *uint256.NewInt(1),
			TipCap:     *uint256.NewInt(1),
			FeeCap:     *uint256.NewInt(2),
			AccessList: AccessList{},
		},
		MaxFeePerBlobGas:    *uint256.NewInt(3),
		BlobVersionedHashes: []common.Hash{bvh},
	}
}

func TestBlobTx_SignRecoverAsMessage(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	addr := crypto.PubkeyToAddress(key.PublicKey)
	signer := LatestSignerForChainID(uint256.NewInt(1).ToBig())

	signed, err := SignTx(sampleBlobTx(), *signer, key)
	require.NoError(t, err)

	got, err := signed.Sender(*signer)
	require.NoError(t, err)
	require.Equal(t, addr, got.Value())

	msg, err := signed.AsMessage(*signer, uint256.NewInt(1), &chain.Rules{IsCancun: true})
	require.NoError(t, err)
	require.Equal(t, addr, msg.From().Value())
	require.Len(t, msg.BlobHashes(), 1)
}

func TestBlobTx_AsMessage_Errors(t *testing.T) {
	t.Parallel()
	signer := LatestSignerForChainID(uint256.NewInt(1).ToBig())

	// Pre-Cancun.
	_, err := sampleBlobTx().AsMessage(*signer, nil, &chain.Rules{})
	require.ErrorContains(t, err, "Cancun")

	// Nil recipient.
	noTo := sampleBlobTx()
	noTo.To = nil
	_, err = noTo.AsMessage(*signer, nil, &chain.Rules{IsCancun: true})
	require.ErrorIs(t, err, ErrNilToFieldTx)

	// No blob hashes.
	noBlobs := sampleBlobTx()
	noBlobs.BlobVersionedHashes = nil
	_, err = noBlobs.AsMessage(*signer, nil, &chain.Rules{IsCancun: true})
	require.ErrorIs(t, err, ErrBlobTxnEmptyBlobs)

	// Wrong version byte.
	badVer := sampleBlobTx()
	badVer.BlobVersionedHashes = []common.Hash{{0xff}}
	_, err = badVer.AsMessage(*signer, nil, &chain.Rules{IsCancun: true})
	require.ErrorIs(t, err, ErrBlobTxnInvalidVersionedHash)
}

func TestBlobTx_Copy(t *testing.T) {
	t.Parallel()
	tx := sampleBlobTx()
	cp := tx.copy()
	require.Len(t, cp.BlobVersionedHashes, 1)

	tx.BlobVersionedHashes[0][1] = 0xab
	require.NotEqual(t, tx.BlobVersionedHashes[0], cp.BlobVersionedHashes[0])
}
