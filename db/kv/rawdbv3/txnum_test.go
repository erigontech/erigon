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
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
)

func TestTxNum(t *testing.T) {
	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	db := mdbxtest.InMem(t, mdbx.New(dbcfg.ChainDB, log.New()), dirs.Chaindata).MustOpen()
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

func TestMaxTxNumNilCursor(t *testing.T) {
	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	db := mdbxtest.InMem(t, mdbx.New(dbcfg.ChainDB, log.New()), dirs.Chaindata).MustOpen()
	t.Cleanup(db.Close)

	require.NoError(db.Update(t.Context(), func(tx kv.RwTx) error {
		require.NoError(TxNums.Append(tx, 0, 3))
		require.NoError(TxNums.Append(tx, 1, 99))
		return nil
	}))

	require.NoError(db.View(t.Context(), func(tx kv.Tx) error {
		maxTxNum, ok, err := DefaultTxBlockIndexInstance.MaxTxNum(t.Context(), tx, nil, 1)
		require.NoError(err)
		require.True(ok)
		require.Equal(99, int(maxTxNum))
		return nil
	}))
}

func TestExactTxNumBounds(t *testing.T) {
	db, spans := newTxNumsDB(t, 3)
	ctx := t.Context()
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		return tx.Delete(kv.MaxTxNum, hexutil.EncodeTs(1))
	}))
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		cursor, cursorErr := tx.Cursor(kv.MaxTxNum)
		require.NoError(t, cursorErr)
		defer cursor.Close()
		for _, c := range []kv.Cursor{nil, cursor} {
			for _, tc := range []struct {
				block, min, max uint64
				hasMin, hasMax  bool
			}{
				{0, 0, spans[0].max, true, true},
				{1, spans[1].min, 0, true, false},
				{2, 0, spans[2].max, false, true},
				{3, spans[2].max + 1, 0, true, false},
			} {
				minTxNum, ok, err := TxNums.MinExact(ctx, tx, c, tc.block)
				require.NoError(t, err)
				require.Equal(t, tc.hasMin, ok)
				require.Equal(t, tc.min, minTxNum)
				maxTxNum, ok, err := TxNums.MaxExact(ctx, tx, c, tc.block)
				require.NoError(t, err)
				require.Equal(t, tc.hasMax, ok)
				require.Equal(t, tc.max, maxTxNum)
			}
		}
		minTxNum, err := TxNums.MinWithCursor(ctx, tx, cursor, 2)
		require.NoError(t, err)
		require.Equal(t, spans[2].max+1, minTxNum, "Min retains its latest-state fallback")
		maxTxNum, err := TxNums.MaxWithCursor(ctx, tx, cursor, 1)
		require.NoError(t, err)
		require.Equal(t, spans[2].max, maxTxNum, "Max retains its latest-state fallback")
		return nil
	}))
}

type blockSpan struct{ min, max uint64 }

// newTxNumsDB writes blocks of 2..7 txNums (system txs included) and returns each block's txNum range.
func newTxNumsDB(t *testing.T, blocks int) (kv.RwDB, []blockSpan) {
	t.Helper()
	db := mdbxtest.InMem(t, mdbx.New(dbcfg.ChainDB, log.New()), datadir.New(t.TempDir()).Chaindata).MustOpen()
	t.Cleanup(db.Close)
	rnd := rand.New(rand.NewSource(1))
	spans := make([]blockSpan, blocks)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		var next uint64
		for b := range spans {
			spans[b] = blockSpan{next, next + 1 + uint64(rnd.Intn(6))}
			next = spans[b].max + 1
			if err := TxNums.Append(tx, uint64(b), spans[b].max); err != nil {
				return err
			}
		}
		return nil
	}))
	return db, spans
}

func TestTxNums2BlockNums(t *testing.T) {
	db, spans := newTxNumsDB(t, 3000)
	rnd := rand.New(rand.NewSource(2))
	var all, sparse, far []uint64
	for b, s := range spans {
		for n := s.min; n <= s.max; n++ {
			all = append(all, n)
			if rnd.Intn(40) == 0 {
				sparse = append(sparse, n)
			}
		}
		if b%997 == 0 {
			far = append(far, s.max)
		}
	}
	for name, txNums := range map[string][]uint64{"all": all, "sparse": sparse, "far": far} {
		for _, asc := range []order.By{order.Asc, order.Desc} {
			t.Run(name+"/"+map[order.By]string{order.Asc: "asc", order.Desc: "desc"}[asc], func(t *testing.T) {
				in := slices.Clone(txNums)
				if !asc {
					slices.Reverse(in)
				}
				tx, err := db.BeginRo(t.Context())
				require.NoError(t, err)
				defer tx.Rollback()
				it := TxNums2BlockNums(t.Context(), tx, TxNums, stream.Array(in), asc)
				defer it.Close()
				prevBlock := -1
				for _, want := range in {
					require.True(t, it.HasNext())
					txNum, blockNum, txIndex, isFinal, isSystem, changed, err := it.Next()
					require.NoError(t, err)
					b, _ := slices.BinarySearchFunc(spans, want, func(s blockSpan, n uint64) int { return int(s.max) - int(n) })
					require.Equal(t, want, txNum)
					require.Equal(t, uint64(b), blockNum, "txNum %d", want)
					require.Equal(t, int(want)-int(spans[b].min)-1, txIndex)
					require.Equal(t, bool(asc) && want == spans[b].max || !bool(asc) && want == spans[b].min, isFinal)
					require.Equal(t, want == spans[b].min || want == spans[b].max, isSystem)
					require.Equal(t, b != prevBlock, changed)
					prevBlock = b
				}
				require.False(t, it.HasNext())
			})
		}
	}
}

type countingTxBlockIndex struct {
	TxBlockIndex
	blockNumberCalls, maxTxNumCalls int
}

func (c *countingTxBlockIndex) MaxTxNum(ctx context.Context, tx kv.Tx, cursor kv.Cursor, blockNum uint64) (uint64, bool, error) {
	c.maxTxNumCalls++
	return c.TxBlockIndex.MaxTxNum(ctx, tx, cursor, blockNum)
}

func (c *countingTxBlockIndex) BlockNumber(ctx context.Context, tx kv.Tx, txNum uint64) (uint64, bool, error) {
	c.blockNumberCalls++
	return c.TxBlockIndex.BlockNumber(ctx, tx, txNum)
}

// TestTxNums2BlockNumsSearchesOnce pins that sorted txNums are mapped by moving from the current
// block, not with a search over the whole chain on every block change.
func TestTxNums2BlockNumsSearchesOnce(t *testing.T) {
	db, spans := newTxNumsDB(t, 1000)
	index := &countingTxBlockIndex{TxBlockIndex: DefaultTxBlockIndexInstance}
	reader := TxNums.WithCustomReadTxNumFunc(index)
	txNums := make([]uint64, 0, len(spans))
	for _, s := range spans {
		txNums = append(txNums, s.min+1)
	}
	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	for _, asc := range []order.By{order.Asc, order.Desc} {
		in := slices.Clone(txNums)
		if !asc {
			slices.Reverse(in)
		}
		index.blockNumberCalls = 0
		it := TxNums2BlockNums(t.Context(), tx, reader, stream.Array(in), asc)
		for it.HasNext() {
			_, _, _, _, _, _, err := it.Next()
			require.NoError(t, err)
		}
		it.Close()
		require.Equal(t, 1, index.blockNumberCalls)
	}
}

// TestTxNums2BlockNumsFarJump pins that a far jump costs a few probes plus one FindBlockNum, not
// O(log distance) probes: for blocks in files every probe can read a body from bodies.seg.
func TestTxNums2BlockNumsFarJump(t *testing.T) {
	db, spans := newTxNumsDB(t, 2000)
	index := &countingTxBlockIndex{TxBlockIndex: DefaultTxBlockIndexInstance}
	reader := TxNums.WithCustomReadTxNumFunc(index)
	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	for _, asc := range []order.By{order.Asc, order.Desc} {
		in := []uint64{spans[10].min, spans[1010].min}
		if !asc {
			slices.Reverse(in)
		}
		it := TxNums2BlockNums(t.Context(), tx, reader, stream.Array(in), asc)
		_, _, _, _, _, _, err := it.Next()
		require.NoError(t, err)
		index.blockNumberCalls, index.maxTxNumCalls = 0, 0
		_, blockNum, _, _, _, _, err := it.Next()
		require.NoError(t, err)
		it.Close()
		require.Equal(t, map[order.By]uint64{order.Asc: 1010, order.Desc: 10}[asc], blockNum)
		require.Equal(t, 1, index.blockNumberCalls)
		require.LessOrEqual(t, index.maxTxNumCalls, 5)
	}
}

func TestTxNums2BlockNumsPastLastBlock(t *testing.T) {
	db, spans := newTxNumsDB(t, 100)
	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	it := TxNums2BlockNums(t.Context(), tx, TxNums, stream.Array([]uint64{spans[10].min, spans[99].max + 5}), order.Asc)
	defer it.Close()
	_, blockNum, _, _, _, _, err := it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(10), blockNum)
	_, _, _, _, _, _, err = it.Next()
	require.ErrorContains(t, err, "can't find blockNumber")
}
