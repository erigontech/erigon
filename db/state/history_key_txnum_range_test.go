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
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
)

// expectedKeyTxNums builds expected (key, txNum) pairs for the filledHistory data pattern.
// keys are 1..31, each key K changes at every txNum that is a multiple of K.
// fromTxNum/toTxNum use the same semantics as HistoryKeyTxNumRange: -1 means unbounded.
func expectedKeyTxNums(fromTxNum, toTxNum int, txs uint64) []string {
	var expected []string
	for keyNum := uint64(1); keyNum <= 31; keyNum++ {
		var k [8]byte
		binary.BigEndian.PutUint64(k[:], keyNum)
		k[0] = 1
		start := uint64(1)
		if fromTxNum > 0 {
			start = uint64(fromTxNum)
		}
		for txNum := start; txNum <= txs; txNum++ {
			if toTxNum >= 0 && txNum >= uint64(toTxNum) {
				break
			}
			if txNum%keyNum == 0 {
				expected = append(expected, fmt.Sprintf("%x:%d", k[:], txNum))
			}
		}
	}
	return expected
}

func collectKeyTxNumRange(t *testing.T, it stream.KU64) []string {
	t.Helper()
	var results []string
	for it.HasNext() {
		k, txNum, err := it.Next()
		require.NoError(t, err)
		results = append(results, fmt.Sprintf("%x:%d", k, txNum))
	}
	it.Close()
	return results
}

func TestHistoryKeyTxNumRange(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)

		collateAndMergeHistory(t, db, h, txs, true)

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()
		ic := h.BeginFilesRo()
		defer ic.Close()

		// Small range [2, 20)
		it, err := ic.HistoryKeyTxNumRange(2, 20, order.Asc, -1, tx)
		require.NoError(err)
		require.Equal(expectedKeyTxNums(2, 20, txs), collectKeyTxNumRange(t, it))

		// Limit
		it, err = ic.HistoryKeyTxNumRange(2, 20, order.Asc, 5, tx)
		require.NoError(err)
		require.Len(collectKeyTxNumRange(t, it), 5)

		// Larger range within files [100, 200)
		it, err = ic.HistoryKeyTxNumRange(100, 200, order.Asc, -1, tx)
		require.NoError(err)
		require.Equal(expectedKeyTxNums(100, 200, txs), collectKeyTxNumRange(t, it))

		// Range [980, 1001) spanning frozen files + DB
		it, err = ic.HistoryKeyTxNumRange(980, 1001, order.Asc, -1, tx)
		require.NoError(err)
		require.Equal(expectedKeyTxNums(980, 1001, txs), collectKeyTxNumRange(t, it))
	}
	t.Run("large_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, false, logger)
		test(t, h, db, txs)
	})
}

func TestHistoryKeyTxNumRange_EdgeCases(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)

		collateAndMergeHistory(t, db, h, txs, true)

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()
		ic := h.BeginFilesRo()
		defer ic.Close()

		t.Run("empty_range", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(5, 5, order.Asc, -1, tx)
			require.NoError(err)
			require.Empty(collectKeyTxNumRange(t, it))
		})
		t.Run("inverted_range", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(20, 5, order.Asc, -1, tx)
			require.NoError(err)
			require.Empty(collectKeyTxNumRange(t, it))
		})
		t.Run("range_beyond_data", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(2000, 3000, order.Asc, -1, tx)
			require.NoError(err)
			require.Empty(collectKeyTxNumRange(t, it))
		})
		t.Run("limit_zero", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(2, 20, order.Asc, 0, tx)
			require.NoError(err)
			require.Empty(collectKeyTxNumRange(t, it))
		})
		t.Run("from_zero", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(0, 5, order.Asc, -1, tx)
			require.NoError(err)
			require.Equal(expectedKeyTxNums(0, 5, txs), collectKeyTxNumRange(t, it))
		})
		t.Run("unbounded_end", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(995, -1, order.Asc, -1, tx)
			require.NoError(err)
			require.Equal(expectedKeyTxNums(995, -1, txs), collectKeyTxNumRange(t, it))
		})
		t.Run("unbounded_start", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(-1, 5, order.Asc, -1, tx)
			require.NoError(err)
			require.Equal(expectedKeyTxNums(-1, 5, txs), collectKeyTxNumRange(t, it))
		})
		t.Run("fully_unbounded", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(-1, -1, order.Asc, -1, tx)
			require.NoError(err)
			require.Equal(expectedKeyTxNums(-1, -1, txs), collectKeyTxNumRange(t, it))
		})
		t.Run("limit_one", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(1, 100, order.Asc, 1, tx)
			require.NoError(err)
			require.Len(collectKeyTxNumRange(t, it), 1)
		})
		t.Run("single_txnum_range", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(6, 7, order.Asc, -1, tx)
			require.NoError(err)
			require.Equal(expectedKeyTxNums(6, 7, txs), collectKeyTxNumRange(t, it))
		})
	}
	t.Run("large_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, false, logger)
		test(t, h, db, txs)
	})
}

func TestHistoryKeyTxNumRange_DBOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)

		// No collation/merge — all data stays in the DB
		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()
		ic := h.BeginFilesRo()
		defer ic.Close()
		require.Empty(ic.iit.files)

		it, err := ic.HistoryKeyTxNumRange(-1, -1, order.Asc, -1, tx)
		require.NoError(err)
		require.Equal(expectedKeyTxNums(-1, -1, txs), collectKeyTxNumRange(t, it))

		it, err = ic.HistoryKeyTxNumRange(10, 30, order.Asc, -1, tx)
		require.NoError(err)
		require.Equal(expectedKeyTxNums(10, 30, txs), collectKeyTxNumRange(t, it))

		it, err = ic.HistoryKeyTxNumRange(1, 100, order.Asc, 3, tx)
		require.NoError(err)
		require.Len(collectKeyTxNumRange(t, it), 3)
	}
	t.Run("large_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, false, logger)
		test(t, h, db, txs)
	})
}

func TestHistoryKeyTxNumRange_RandomRanges(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)

		collateAndMergeHistory(t, db, h, txs, true)

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()
		ic := h.BeginFilesRo()
		defer ic.Close()

		rng := rand.New(rand.NewSource(0))
		for i := 0; i < 50; i++ {
			from := rng.Intn(int(txs) + 100)
			to := rng.Intn(int(txs) + 100)
			if from > to {
				from, to = to, from
			}
			limit := -1
			if rng.Intn(3) == 0 {
				limit = rng.Intn(20)
			}
			t.Logf("iter %d: from=%d to=%d limit=%d", i, from, to, limit)

			it, err := ic.HistoryKeyTxNumRange(from, to, order.Asc, limit, tx)
			require.NoError(err)
			got := collectKeyTxNumRange(t, it)

			expected := expectedKeyTxNums(from, to, txs)
			if limit >= 0 && len(expected) > limit {
				expected = expected[:limit]
			}
			if len(expected) == 0 {
				expected = nil
			}
			require.Equal(expected, got, "iter %d: from=%d to=%d limit=%d", i, from, to, limit)
		}
	}
	t.Run("large_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, false, logger)
		test(t, h, db, txs)
	})
}
