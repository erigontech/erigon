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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

func TestAggregatorBuildKeepsBackfilledReceiptVisible(t *testing.T) {
	const stepSize = uint64(10)
	db, agg := testDbAndAggregatorv3(t, stepSize)
	ctx := t.Context()
	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 3}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 3}})
	require.NoError(t, agg.OpenFolder(db))

	key, value := []byte{1}, []byte{7}
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		at := agg.BeginFilesRo()
		defer at.Close()
		write := func(domain kv.Domain, txNums ...uint64) {
			w := at.d[domain].NewWriter(db)
			defer w.Close()
			for _, txNum := range txNums {
				require.NoError(t, w.PutWithPrev([]byte{byte(txNum)}, value, txNum, nil))
			}
			require.NoError(t, w.Flush(ctx, tx))
		}
		write(kv.ReceiptDomain, 1)
		write(kv.AccountsDomain, 3*stepSize, 4*stepSize)
		return nil
	}))
	requireReceipt := func() {
		t.Helper()
		require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
			at := agg.BeginFilesRo()
			defer at.Close()
			got, _, found, err := at.d[kv.ReceiptDomain].GetLatest(key, tx)
			require.NoError(t, err)
			require.True(t, found, "the backfilled receipt must remain readable")
			require.Equal(t, value, got)
			return nil
		}))
	}

	requireReceipt()
	done, started := agg.buildFilesInBackground(asTemporalRoDB(db), 4*stepSize, false, unboundedFinalityCtx)
	require.True(t, started)
	<-done
	requireReceipt()
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		at := agg.BeginFilesRo()
		defer at.Close()
		require.Equal(t, 4*stepSize, at.d[kv.AccountsDomain].files.EndTxNum())
		_, err := at.prune(ctx, tx, 0, true, nil)
		return err
	}))
	requireReceipt()
}
