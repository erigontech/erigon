// Copyright 2021 The Erigon Authors
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

package rawdbv3

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
)

func TestTxNum(t *testing.T) {
	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	db := mdbx.New(dbcfg.ChainDB, log.New()).InMem(t, dirs.Chaindata).MustOpen()
	t.Cleanup(db.Close)

	err := db.Update(t.Context(), func(tx kv.RwTx) error {
		require.NoError(TxNums.Append(tx, 0, 3))
		require.NoError(TxNums.Append(tx, 1, 99))
		require.NoError(TxNums.Append(tx, 2, 100))

		n, _, err := TxNums.FindBlockNum(t.Context(), tx, 10)
		require.NoError(err)
		require.Equal(1, int(n))

		n, _, err = TxNums.FindBlockNum(t.Context(), tx, 0)
		require.NoError(err)
		require.Equal(0, int(n))

		n, _, err = TxNums.FindBlockNum(t.Context(), tx, 3)
		require.NoError(err)
		require.Equal(0, int(n))
		n, _, err = TxNums.FindBlockNum(t.Context(), tx, 4)
		require.NoError(err)
		require.Equal(1, int(n))

		n, _, err = TxNums.FindBlockNum(t.Context(), tx, 99)
		require.NoError(err)
		require.Equal(1, int(n))

		n, _, err = TxNums.FindBlockNum(t.Context(), tx, 100)
		require.NoError(err)
		require.Equal(2, int(n))

		_, ok, err := TxNums.FindBlockNum(t.Context(), tx, 101)
		require.NoError(err)
		require.False(ok)
		return nil
	})
	require.NoError(err)
}

// BenchmarkMapTxNum2BlockNumIter measures the cursor open/close cost for Min+Max
// on each block change inside the TxNums2BlockNums iterator.
//
// CurrentCursorPerCall: baseline — 2 new cursors per block change (Min + Max)
// SharedCursorForMinMax: optimized — 1 cursor reused for the iterator's lifetime
//
// Run with:
//
//	go test -bench=BenchmarkMapTxNum2BlockNumIter -benchmem -count=5 ./db/kv/rawdbv3/...
func BenchmarkMapTxNum2BlockNumIter(b *testing.B) {
	const numBlocks = 10_000
	const txPerBlock = 5

	dirs := datadir.New(b.TempDir())
	db := mdbx.New(dbcfg.ChainDB, log.New()).InMem(b, dirs.Chaindata).MustOpen()
	b.Cleanup(db.Close)
	ctx := context.Background()

	// Populate MaxTxNum table
	err := db.Update(ctx, func(tx kv.RwTx) error {
		var maxTxNum uint64
		for blockNum := uint64(0); blockNum < numBlocks; blockNum++ {
			maxTxNum += txPerBlock
			if err := TxNums.Append(tx, blockNum, maxTxNum); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(b, err)

	// Worst case: one txNum per block — every Next() changes block — maximum cursor opens
	txNumsPerBlock := make([]uint64, numBlocks)
	err = db.View(ctx, func(tx kv.Tx) error {
		for blockNum := uint64(0); blockNum < numBlocks; blockNum++ {
			min, err := TxNums.Min(ctx, tx, blockNum)
			if err != nil {
				return err
			}
			txNumsPerBlock[blockNum] = min
		}
		return nil
	})
	require.NoError(b, err)

	tx, err := db.BeginRo(ctx)
	require.NoError(b, err)
	defer tx.Rollback()
	b.Cleanup(tx.Rollback)

	// Benchmark 1: baseline
	// Min() and Max() each open their own kv.MaxTxNum cursor on every block change.
	b.Run("CurrentCursorPerCall", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			it := TxNums2BlockNums(ctx, tx, TxNums, stream.Array(txNumsPerBlock), order.Asc)
			for it.HasNext() {
				if _, _, _, _, _, err := it.Next(); err != nil {
					b.Fatal(err)
				}
			}
			it.Close()
		}
	})

	// Benchmark 2: optimized path
	// A single cursor is opened once and passed to each MaxTxNum call
	// (equivalent to Min/Max with a shared cursor).
	// FindBlockNum still opens its own cursor for binary search (not optimized here).
	b.Run("SharedCursorForMinMax", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			c, err := tx.Cursor(kv.MaxTxNum) //nolint:gocritic // in bench loop; closed explicitly at end of each iteration
			if err != nil {
				b.Fatal(err)
			}
			var maxTxNumInBlock uint64
			for _, txNum := range txNumsPerBlock {
				if txNum > maxTxNumInBlock {
					blockNum, _, err := TxNums.FindBlockNum(ctx, tx, txNum)
					if err != nil {
						b.Fatal(err)
					}
					// Min: MaxTxNum(blockNum-1)+1 — shared cursor
					if blockNum > 0 {
						if _, _, err := DefaultTxBlockIndexInstance.MaxTxNum(ctx, tx, c, blockNum-1); err != nil {
							b.Fatal(err)
						}
					}
					// Max: MaxTxNum(blockNum) — shared cursor
					max, _, err := DefaultTxBlockIndexInstance.MaxTxNum(ctx, tx, c, blockNum)
					if err != nil {
						b.Fatal(err)
					}
					maxTxNumInBlock = max
				}
			}
			c.Close()
		}
	})
}
