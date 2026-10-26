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

package txpool

import (
	"bytes"
	"slices"
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	libkzg "github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestValidateBlobTxn(t *testing.T) {
	cancun := makeBlobTxn()
	cancun.SenderID = 1
	osaka := cancun
	osaka.BlobBundles = slices.Clone(cancun.BlobBundles)
	for i := range osaka.BlobBundles {
		bundle := &osaka.BlobBundles[i]
		require.Len(t, bundle.Blob, params.BlobSize)
		_, proofs, err := libkzg.Ctx().ComputeCellsAndKZGProofs((*goethkzg.Blob)(bundle.Blob), 1)
		require.NoError(t, err)
		bundle.Proofs = slices.Clone(proofs[:])
		require.Len(t, bundle.Proofs, int(params.CellsPerExtBlob))
	}

	for _, tc := range []struct {
		fork string
		slot TxnSlot
	}{
		{"Cancun", cancun},
		{"Osaka", osaka},
	} {
		t.Run(tc.fork, func(t *testing.T) {
			pool, err := New(t.Context(), nil, nil, nil, txpoolcfg.DefaultConfig, nil,
				testforks.Forks[tc.fork], nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
			require.NoError(t, err)
			pool.lock.Lock()
			defer pool.lock.Unlock()
			slot := tc.slot
			badProof := slot
			badProof.BlobBundles = slices.Clone(slot.BlobBundles)
			last := &badProof.BlobBundles[len(badProof.BlobBundles)-1]
			last.Proofs = slices.Clone(last.Proofs)
			last.Proofs[len(last.Proofs)-1][0] ^= 0xff

			t.Run("valid sidecar", func(t *testing.T) {
				require.Equal(t, txpoolcfg.Success, pool.validateBlobTxn(&slot, false))
			})
			t.Run("changed blob", func(t *testing.T) {
				bad := slot
				bad.BlobBundles = slices.Clone(slot.BlobBundles)
				bad.BlobBundles[0].Blob = bytes.Clone(slot.BlobBundles[0].Blob)
				bad.BlobBundles[0].Blob[31] ^= 1
				require.Equal(t, txpoolcfg.UnmatchedBlobTxExt, pool.validateBlobTxn(&bad, false))
			})
			t.Run("changed last proof", func(t *testing.T) {
				require.Equal(t, txpoolcfg.UnmatchedBlobTxExt, pool.validateBlobTxn(&badProof, false))
			})
			for _, size := range []struct {
				name   string
				length int
			}{
				{"short blob", params.BlobSize - 1},
				{"long blob", params.BlobSize + 1},
			} {
				t.Run(size.name, func(t *testing.T) {
					bad := slot
					bad.BlobBundles = slices.Clone(slot.BlobBundles)
					last := &bad.BlobBundles[len(bad.BlobBundles)-1]
					last.Blob = make([]byte, size.length)
					copy(last.Blob, slot.BlobBundles[len(slot.BlobBundles)-1].Blob)
					var reason txpoolcfg.DiscardReason
					require.NotPanics(t, func() { reason = pool.validateBlobTxn(&bad, false) })
					require.Equal(t, txpoolcfg.UnmatchedBlobTxExt, reason)
				})
			}
			t.Run("changed blob boundaries", func(t *testing.T) {
				require.Equal(t, txpoolcfg.Success, pool.validateBlobTxn(&slot, false))
				bad := slot
				bad.BlobBundles = slices.Clone(slot.BlobBundles)
				txn := types.CopyTxs(types.Transactions{slot.Txn})[0].(*types.BlobTx)
				bad.Txn = txn
				bad.BlobBundles[0].Blob = append(bytes.Clone(slot.BlobBundles[0].Blob), slot.BlobBundles[0].Commitment[:]...)
				copy(bad.BlobBundles[0].Commitment[:], slot.BlobBundles[1].Blob[:48])
				bad.BlobBundles[1].Blob = slot.BlobBundles[1].Blob[48:]
				txn.BlobVersionedHashes[0] = common.Hash(libkzg.KZGToVersionedHash(bad.BlobBundles[0].Commitment))
				var reason txpoolcfg.DiscardReason
				require.NotPanics(t, func() { reason = pool.validateBlobTxn(&bad, false) })
				require.Equal(t, txpoolcfg.UnmatchedBlobTxExt, reason)
			})
			if tc.fork == "Osaka" {
				t.Run("missing cell proof", func(t *testing.T) {
					bad := slot
					bad.BlobBundles = slices.Clone(slot.BlobBundles)
					last := &bad.BlobBundles[len(bad.BlobBundles)-1]
					last.Proofs = last.Proofs[:len(last.Proofs)-1]
					require.Equal(t, txpoolcfg.UnmatchedBlobTxExt, pool.validateBlobTxn(&bad, false))
				})
			}
			t.Run("full pool before proof verification", func(t *testing.T) {
				pool.totalBlobsInPool.Store(pool.cfg.TotalBlobPoolLimit)
				defer pool.totalBlobsInPool.Store(0)
				require.Equal(t, txpoolcfg.BlobPoolOverflow, pool.validateBlobTxn(&badProof, false))
			})
			t.Run("sender limit before proof verification", func(t *testing.T) {
				pool.all.senderIDBlobCount[slot.SenderID] = pool.cfg.BlobSlots
				defer delete(pool.all.senderIDBlobCount, slot.SenderID)
				require.Equal(t, txpoolcfg.Spammer, pool.validateBlobTxn(&badProof, false))
				require.Equal(t, txpoolcfg.UnmatchedBlobTxExt, pool.validateBlobTxn(&badProof, true))
			})
		})
	}
}
