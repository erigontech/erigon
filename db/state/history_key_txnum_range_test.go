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
	"strings"
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
		t.Skip()
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

		// Case 1: small range [2, 20) — verify all (key, txNum) pairs
		it, err := ic.HistoryKeyTxNumRange(2, 20, order.Asc, -1, tx)
		require.NoError(err)
		var results []string
		for it.HasNext() {
			k, txNum, err := it.Next()
			require.NoError(err)
			results = append(results, fmt.Sprintf("%x:%d", k, txNum))
		}
		it.Close()

		// Build expected: for each key N (1..31), txNums in [2,20) that are multiples of N
		var expected []string
		for keyNum := uint64(1); keyNum <= 31; keyNum++ {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], keyNum)
			k[0] = 1
			for txNum := uint64(2); txNum < 20; txNum++ {
				if txNum%keyNum == 0 {
					expected = append(expected, fmt.Sprintf("%x:%d", k[:], txNum))
				}
			}
		}
		require.Equal(expected, results)

		// Case 2: key 1 should appear 18 times (txNums 2..19)
		count := 0
		for _, s := range results {
			if strings.HasPrefix(s, "0100000000000001:") {
				count++
			}
		}
		require.Equal(18, count, "key 1 should have 18 entries in [2,20)")

		// Case 3: limit
		it, err = ic.HistoryKeyTxNumRange(2, 20, order.Asc, 5, tx)
		require.NoError(err)
		results = results[:0]
		for it.HasNext() {
			k, txNum, err := it.Next()
			require.NoError(err)
			results = append(results, fmt.Sprintf("%x:%d", k, txNum))
		}
		it.Close()
		require.Len(results, 5, "limit should be respected")

		// Case 4: larger range entirely within files [100, 200)
		it, err = ic.HistoryKeyTxNumRange(100, 200, order.Asc, -1, tx)
		require.NoError(err)
		results = results[:0]
		for it.HasNext() {
			k, txNum, err := it.Next()
			require.NoError(err)
			results = append(results, fmt.Sprintf("%x:%d", k, txNum))
		}
		it.Close()
		// Build expected for [100, 200)
		expected = expected[:0]
		for keyNum := uint64(1); keyNum <= 31; keyNum++ {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], keyNum)
			k[0] = 1
			for txNum := uint64(100); txNum < 200; txNum++ {
				if txNum%keyNum == 0 {
					expected = append(expected, fmt.Sprintf("%x:%d", k[:], txNum))
				}
			}
		}
		require.Equal(expected, results)

		// Case 5: range [980, 1001) spanning frozen files + DB
		// filledHistory writes 1000 txs with stepSize=16.
		// collateAndMergeHistory freezes steps 0..61 (txNums 0..991), leaves step 62 (992..1000) in DB.
		// So [980, 1001) covers both file data (980..991) and DB data (992..1000).
		it, err = ic.HistoryKeyTxNumRange(980, 1001, order.Asc, -1, tx)
		require.NoError(err)
		results = results[:0]
		for it.HasNext() {
			k, txNum, err := it.Next()
			require.NoError(err)
			results = append(results, fmt.Sprintf("%x:%d", k, txNum))
		}
		it.Close()
		// Build expected for [980, 1001)
		expected = expected[:0]
		for keyNum := uint64(1); keyNum <= 31; keyNum++ {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], keyNum)
			k[0] = 1
			for txNum := uint64(980); txNum < 1001; txNum++ {
				if txNum <= txs && txNum%keyNum == 0 {
					expected = append(expected, fmt.Sprintf("%x:%d", k[:], txNum))
				}
			}
		}
		require.Equal(expected, results)

		// Verify DB-side has multiple entries per key (key 1 should have entries 992..1000)
		dbEntries := 0
		for _, s := range results {
			if strings.HasPrefix(s, "0100000000000001:") {
				parts := strings.Split(s, ":")
				var txNum uint64
				fmt.Sscanf(parts[1], "%d", &txNum)
				if txNum >= 992 {
					dbEntries++
				}
			}
		}
		require.Equal(9, dbEntries, "key 1 should have 9 DB-side entries (992..1000)")
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
		t.Skip()
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

		// Empty range: fromTxNum == toTxNum
		t.Run("empty_range", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(5, 5, order.Asc, -1, tx)
			require.NoError(err)
			results := collectKeyTxNumRange(t, it)
			require.Empty(results)
		})

		// Empty range: fromTxNum > toTxNum (edge case)
		t.Run("inverted_range", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(20, 5, order.Asc, -1, tx)
			require.NoError(err)
			results := collectKeyTxNumRange(t, it)
			require.Empty(results)
		})

		// Range beyond all data
		t.Run("range_beyond_data", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(2000, 3000, order.Asc, -1, tx)
			require.NoError(err)
			results := collectKeyTxNumRange(t, it)
			require.Empty(results)
		})

		// Limit = 0
		t.Run("limit_zero", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(2, 20, order.Asc, 0, tx)
			require.NoError(err)
			results := collectKeyTxNumRange(t, it)
			require.Empty(results)
		})

		// Range from 0 (includes first changes)
		t.Run("from_zero", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(0, 5, order.Asc, -1, tx)
			require.NoError(err)
			results := collectKeyTxNumRange(t, it)
			expected := expectedKeyTxNums(0, 5, txs)
			require.Equal(expected, results)
		})

		// Unbounded end (toTxNum = -1)
		t.Run("unbounded_end", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(995, -1, order.Asc, -1, tx)
			require.NoError(err)
			results := collectKeyTxNumRange(t, it)
			expected := expectedKeyTxNums(995, -1, txs)
			require.Equal(expected, results)
		})

		// Unbounded start (fromTxNum = -1)
		t.Run("unbounded_start", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(-1, 5, order.Asc, -1, tx)
			require.NoError(err)
			results := collectKeyTxNumRange(t, it)
			expected := expectedKeyTxNums(-1, 5, txs)
			require.Equal(expected, results)
		})

		// Fully unbounded (fromTxNum = -1, toTxNum = -1)
		t.Run("fully_unbounded", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(-1, -1, order.Asc, -1, tx)
			require.NoError(err)
			results := collectKeyTxNumRange(t, it)
			expected := expectedKeyTxNums(-1, -1, txs)
			require.Equal(expected, results)
		})

		// Limit = 1
		t.Run("limit_one", func(t *testing.T) {
			it, err := ic.HistoryKeyTxNumRange(1, 100, order.Asc, 1, tx)
			require.NoError(err)
			results := collectKeyTxNumRange(t, it)
			require.Len(results, 1)
		})

		// Single-step range (from, from+1)
		t.Run("single_txnum_range", func(t *testing.T) {
			// txNum=6 is a multiple of 1,2,3,6 → 4 keys change at txNum=6
			it, err := ic.HistoryKeyTxNumRange(6, 7, order.Asc, -1, tx)
			require.NoError(err)
			results := collectKeyTxNumRange(t, it)
			expected := expectedKeyTxNums(6, 7, txs)
			require.Equal(expected, results)
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
		t.Skip()
	}

	t.Parallel()

	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)

		// NO collation/merge — all data stays in the DB
		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()
		ic := h.BeginFilesRo()
		defer ic.Close()

		// Verify no frozen files
		require.Empty(ic.iit.files, "expected no frozen files")

		// Full range
		it, err := ic.HistoryKeyTxNumRange(-1, -1, order.Asc, -1, tx)
		require.NoError(err)
		results := collectKeyTxNumRange(t, it)
		expected := expectedKeyTxNums(-1, -1, txs)
		require.Equal(expected, results)

		// Bounded sub-range
		it, err = ic.HistoryKeyTxNumRange(10, 30, order.Asc, -1, tx)
		require.NoError(err)
		results = collectKeyTxNumRange(t, it)
		expected = expectedKeyTxNums(10, 30, txs)
		require.Equal(expected, results)

		// With limit
		it, err = ic.HistoryKeyTxNumRange(1, 100, order.Asc, 3, tx)
		require.NoError(err)
		results = collectKeyTxNumRange(t, it)
		require.Len(results, 3)
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
