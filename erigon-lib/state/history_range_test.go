// Copyright 2022 The Erigon Authors
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
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/log/v3"
)

/*
HistoryRange Method Documentation and Test Suite

## What HistoryRange Does

HistoryRange returns a "state patch" - a sorted list of keys that were updated
between fromTxNum and toTxNum (inclusive start, exclusive end: [fromTxNum, toTxNum))
along with their PREVIOUS values before the change occurred.

## Method Signature
```go
func (ht *HistoryRoTx) HistoryRange(fromTxNum, toTxNum int, asc order.By, limit int, roTx kv.Tx) (stream.KV, error)
```

## Parameters
- fromTxNum: Starting transaction number (inclusive). Use -1 for no lower bound.
- toTxNum: Ending transaction number (exclusive). Use -1 for no upper bound.
- asc: Sort order. Currently only order.Asc is supported (order.Desc panics).
- limit: Maximum number of results to return. Use -1 for unlimited.
- roTx: Read-only database transaction for accessing recent data.

## Return Value
Returns a stream.KV iterator that yields key-value pairs where:
- Key: The key that was changed
- Value: The PREVIOUS value of the key before the change (empty if it was a new key)

## Key Invariants

1. **Ascending Order**: Keys are returned in lexicographically ascending order
2. **No Duplicates**: Each key appears at most once in the result set
3. **Previous Values**: Values represent the state BEFORE the change, not after
4. **Range Semantics**: Includes changes at fromTxNum, excludes changes at toTxNum
5. **Stream Invariants**:
   - HasNext() is idempotent
   - Key/Value pairs remain valid for at least 2 Next() calls
   - Automatically handles context cancellation
6. **Limit Respect**: Never returns more than the specified limit
7. **Data Source Merging**: Seamlessly combines data from files (frozen) and database (recent)

## How It Works Internally

1. Creates two iterators:
   - iterateChangedFrozen: Reads from historical files
   - iterateChangedRecent: Reads from database
2. Uses stream.IntersectKV to merge both sources, prioritizing database values
3. Returns changes in transaction number order with deduplication

## Use Cases

- Generate state patches for synchronization
- Audit trail of what changed in a transaction range
- Incremental state updates
- Historical analysis of state changes

## Performance Characteristics

- Efficient for ranges spanning both files and database
- Scales well with large datasets due to streaming interface
- Memory usage is bounded by the limit parameter
- Leverages file-based compression for historical data

## Limitations

- Descending order is not currently supported
- Cannot seek to arbitrary positions within the stream
- Context cancellation behavior may vary by implementation layer
*/

func TestHistoryRange_Basic(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		defer db.Close()

		// Collate and merge to have data in both files and DB
		collateAndMergeHistory(t, db, h, txs, true)

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()

		// Test basic range functionality
		it, err := hc.HistoryRange(2, 20, order.Asc, -1, tx)
		require.NoError(err)
		defer it.Close()

		var keys, vals []string
		for it.HasNext() {
			k, v, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			vals = append(vals, fmt.Sprintf("%x", v))
		}

		// Verify keys are in ascending order
		require.True(sort.StringsAreSorted(keys), "Keys should be in ascending order")

		// Verify no duplicate keys
		uniqueKeys := make(map[string]bool)
		for _, key := range keys {
			require.False(uniqueKeys[key], "Found duplicate key: %s", key)
			uniqueKeys[key] = true
		}

		// Expected keys based on filledHistory logic:
		// Keys 1-31 (encoded with first byte = 1), changes at txNum % keyNum == 0
		// So in range [2, 20), we expect keys that changed in transactions 2-19
		expectedKeys := []string{
			"0100000000000001", // key 1 changes every tx
			"0100000000000002", // key 2 changes at tx 2,4,6,8,10,12,14,16,18
			"0100000000000003", // key 3 changes at tx 3,6,9,12,15,18
			"0100000000000004", // key 4 changes at tx 4,8,12,16
			"0100000000000005", // key 5 changes at tx 5,10,15
			"0100000000000006", // key 6 changes at tx 6,12,18
			"0100000000000007", // key 7 changes at tx 7,14
			"0100000000000008", // key 8 changes at tx 8,16
			"0100000000000009", // key 9 changes at tx 9,18
			"010000000000000a", // key 10 changes at tx 10
			"010000000000000b", // key 11 changes at tx 11
			"010000000000000c", // key 12 changes at tx 12
			"010000000000000d", // key 13 changes at tx 13
			"010000000000000e", // key 14 changes at tx 14
			"010000000000000f", // key 15 changes at tx 15
			"0100000000000010", // key 16 changes at tx 16
			"0100000000000011", // key 17 changes at tx 17
			"0100000000000012", // key 18 changes at tx 18
			"0100000000000013", // key 19 changes at tx 19
		}
		require.Equal(expectedKeys, keys)
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}

func TestHistoryRange_EdgeCases(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		defer db.Close()

		collateAndMergeHistory(t, db, h, txs, true)

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()

		// Test empty range (fromTxNum >= toTxNum)
		it, err := hc.HistoryRange(10, 10, order.Asc, -1, tx)
		require.NoError(err)
		require.False(it.HasNext(), "Empty range should have no results")
		it.Close()

		// Test range beyond available data
		it, err = hc.HistoryRange(2000, 3000, order.Asc, -1, tx)
		require.NoError(err)
		require.False(it.HasNext(), "Range beyond data should have no results")
		it.Close()

		// Test negative fromTxNum (should start from beginning)
		it, err = hc.HistoryRange(-1, 20, order.Asc, 3, tx)
		require.NoError(err)

		var keys []string
		count := 0
		for it.HasNext() {
			k, _, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			count++
		}
		require.LessOrEqual(count, 3, "Should respect limit")
		require.True(sort.StringsAreSorted(keys), "Keys should be in ascending order")
		it.Close()

		// Test negative toTxNum (no upper bound)
		it, err = hc.HistoryRange(995, -1, order.Asc, 5, tx)
		require.NoError(err)

		keys = keys[:0]
		count = 0
		for it.HasNext() {
			k, _, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			count++
		}
		require.Equal(5, count, "Should respect limit even with no upper bound")
		require.True(sort.StringsAreSorted(keys), "Keys should be in ascending order")
		it.Close()
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}

func TestHistoryRange_Limits(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		defer db.Close()

		collateAndMergeHistory(t, db, h, txs, true)

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()

		// Test with limit = 0 (should return nothing)
		it, err := hc.HistoryRange(1, 100, order.Asc, 0, tx)
		require.NoError(err)
		require.False(it.HasNext(), "Limit 0 should return no results")
		it.Close()

		// Test with limit = 1
		it, err = hc.HistoryRange(1, 100, order.Asc, 1, tx)
		require.NoError(err)
		count := 0
		for it.HasNext() {
			_, _, err := it.Next()
			require.NoError(err)
			count++
		}
		require.Equal(1, count, "Should respect limit = 1")
		it.Close()

		// Test with various limits
		limits := []int{1, 5, 10, 50}
		for _, limit := range limits {
			it, err := hc.HistoryRange(1, 100, order.Asc, limit, tx)
			require.NoError(err)

			count := 0
			for it.HasNext() {
				_, _, err := it.Next()
				require.NoError(err)
				count++
			}
			require.LessOrEqual(count, limit, "Should not exceed limit %d", limit)
			it.Close()
		}
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}

func TestHistoryRange_Ordering(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		defer db.Close()

		collateAndMergeHistory(t, db, h, txs, true)

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()

		// Test descending order should panic (not supported yet)
		require.Panics(func() {
			hc.HistoryRange(1, 100, order.Desc, -1, tx)
		}, "Descending order should panic")

		// Test ascending order
		it, err := hc.HistoryRange(10, 50, order.Asc, -1, tx)
		require.NoError(err)
		defer it.Close()

		var prevKey []byte
		for it.HasNext() {
			k, _, err := it.Next()
			require.NoError(err)

			if prevKey != nil {
				require.True(bytes.Compare(prevKey, k) < 0,
					"Keys should be in ascending order: %x should be < %x", prevKey, k)
			}
			// Make a copy since the stream guarantees keys are valid for 2 Next() calls
			prevKey = make([]byte, len(k))
			copy(prevKey, k)
		}
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}

func TestHistoryRange_Values(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		defer db.Close()

		collateAndMergeHistory(t, db, h, txs, true)

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()

		// Test specific ranges and verify values
		it, err := hc.HistoryRange(995, 1000, order.Asc, -1, tx)
		require.NoError(err)
		defer it.Close()

		var results []struct {
			key string
			val string
		}

		for it.HasNext() {
			k, v, err := it.Next()
			require.NoError(err)
			results = append(results, struct {
				key string
				val string
			}{
				key: fmt.Sprintf("%x", k),
				val: fmt.Sprintf("%x", v),
			})
		}

		// Verify we got some results
		require.Greater(len(results), 0, "Should have some results in range [995, 1000)")

		// Verify all keys are valid (should be 8-byte big-endian encoded numbers with first byte = 1)
		for _, result := range results {
			require.Equal(16, len(result.key), "Key should be 16 hex chars (8 bytes)")
			require.Equal("01", result.key[:2], "Key should start with 01")
		}

		// Verify values are valid (should be 8-byte big-endian encoded numbers with first byte = 255)
		for _, result := range results {
			if result.val != "" { // Skip empty values (deletions)
				require.Equal(16, len(result.val), "Value should be 16 hex chars (8 bytes)")
				require.Equal("ff", result.val[:2], "Value should start with ff")
			}
		}
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}

func TestHistoryRange_Consistency(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		defer db.Close()

		// Test before merge (data in DB only)
		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()

		// Get results before merge
		it1, err := hc.HistoryRange(10, 30, order.Asc, -1, tx)
		require.NoError(err)

		var beforeMerge []struct {
			key string
			val string
		}
		for it1.HasNext() {
			k, v, err := it1.Next()
			require.NoError(err)
			beforeMerge = append(beforeMerge, struct {
				key string
				val string
			}{
				key: fmt.Sprintf("%x", k),
				val: fmt.Sprintf("%x", v),
			})
		}
		it1.Close()

		// Merge data to files
		collateAndMergeHistory(t, db, h, txs, true)

		// Get results after merge
		it2, err := hc.HistoryRange(10, 30, order.Asc, -1, tx)
		require.NoError(err)

		var afterMerge []struct {
			key string
			val string
		}
		for it2.HasNext() {
			k, v, err := it2.Next()
			require.NoError(err)
			afterMerge = append(afterMerge, struct {
				key string
				val string
			}{
				key: fmt.Sprintf("%x", k),
				val: fmt.Sprintf("%x", v),
			})
		}
		it2.Close()

		// Results should be identical before and after merge
		require.Equal(len(beforeMerge), len(afterMerge), "Should have same number of results")
		for i := range beforeMerge {
			require.Equal(beforeMerge[i].key, afterMerge[i].key, "Keys should match at index %d", i)
			require.Equal(beforeMerge[i].val, afterMerge[i].val, "Values should match at index %d", i)
		}
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}

func TestHistoryRange_StatePatch(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()

	// Create a custom test with known data pattern
	db, h := testDbAndHistory(t, false, logger)
	defer db.Close()

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	hc := h.BeginFilesRo()
	defer hc.Close()
	writer := hc.NewWriter()
	defer writer.close()

	// Create a specific pattern:
	// key1: changes at tx 1, 5, 10
	// key2: changes at tx 3, 7
	// key3: changes at tx 2, 8, 12
	var key1 = []byte{1, 0, 0, 0, 0, 0, 0, 1}
	var key2 = []byte{1, 0, 0, 0, 0, 0, 0, 2}
	var key3 = []byte{1, 0, 0, 0, 0, 0, 0, 3}

	// AddPrevValue takes the PREVIOUS value, so we need to track state
	var prevVal1, prevVal2, prevVal3 []byte

	// Change at tx 1: key1 gets value {1, 1}
	err = writer.AddPrevValue(key1, 1, prevVal1)
	require.NoError(t, err)
	prevVal1 = []byte{1, 1}

	// Change at tx 2: key3 gets value {3, 2}
	err = writer.AddPrevValue(key3, 2, prevVal3)
	require.NoError(t, err)
	prevVal3 = []byte{3, 2}

	// Change at tx 3: key2 gets value {2, 3}
	err = writer.AddPrevValue(key2, 3, prevVal2)
	require.NoError(t, err)
	prevVal2 = []byte{2, 3}

	// Change at tx 5: key1 gets value {1, 5}
	err = writer.AddPrevValue(key1, 5, prevVal1)
	require.NoError(t, err)
	prevVal1 = []byte{1, 5}

	// Change at tx 7: key2 gets value {2, 7}
	err = writer.AddPrevValue(key2, 7, prevVal2)
	require.NoError(t, err)
	prevVal2 = []byte{2, 7}

	// Change at tx 8: key3 gets value {3, 8}
	err = writer.AddPrevValue(key3, 8, prevVal3)
	require.NoError(t, err)
	prevVal3 = []byte{3, 8}

	err = writer.Flush(ctx, tx)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	// Now test HistoryRange
	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	// Test range [3, 9) - should include changes at tx 3, 5, 7, 8
	it, err := hc.HistoryRange(3, 9, order.Asc, -1, roTx)
	require.NoError(t, err)
	defer it.Close()

	var results []struct {
		key []byte
		val []byte
	}

	for it.HasNext() {
		k, v, err := it.Next()
		require.NoError(t, err)
		// Copy the slices since they're only valid for 2 Next() calls
		keyCopy := make([]byte, len(k))
		valCopy := make([]byte, len(v))
		copy(keyCopy, k)
		copy(valCopy, v)
		results = append(results, struct {
			key []byte
			val []byte
		}{keyCopy, valCopy})
	}

	// Should have 3 unique keys that changed in range [3, 9)
	require.Len(t, results, 3, "Should have 3 keys that changed in range [3, 9)")

	// Verify specific results - HistoryRange returns the PREVIOUS values that were stored
	expectedResults := []struct {
		key []byte
		val []byte
	}{
		{key1, []byte{1, 1}}, // key1 changed at tx 5, previous value was {1, 1}
		{key2, []byte{}},     // key2 changed at tx 3, previous value was empty
		{key3, []byte{3, 2}}, // key3 changed at tx 8, previous value was {3, 2}
	}

	for i, expected := range expectedResults {
		require.Equal(t, expected.key, results[i].key, "Key mismatch at index %d", i)
		require.Equal(t, expected.val, results[i].val, "Value mismatch at index %d", i)
	}
}

func TestHistoryRange_EmptyDatabase(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()

	// Test with empty database
	db, h := testDbAndHistory(t, false, logger)
	defer db.Close()

	tx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	hc := h.BeginFilesRo()
	defer hc.Close()

	// Test empty database
	it, err := hc.HistoryRange(1, 100, order.Asc, -1, tx)
	require.NoError(t, err)
	defer it.Close()

	require.False(t, it.HasNext(), "Empty database should have no results")
}

func TestHistoryRange_StreamInvariants(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()

	db, h, txs := filledHistory(t, false, logger)
	defer db.Close()

	collateAndMergeHistory(t, db, h, txs, true)

	tx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	hc := h.BeginFilesRo()
	defer hc.Close()

	it, err := hc.HistoryRange(10, 50, order.Asc, -1, tx)
	require.NoError(t, err)
	defer it.Close()

	// Test Stream Invariant 2: K, V are valid for at least 2 Next() calls
	var firstKey, firstVal, secondKey []byte

	if it.HasNext() {
		firstKey, firstVal, err = it.Next()
		require.NoError(t, err)

		if it.HasNext() {
			secondKey, _, err = it.Next()
			require.NoError(t, err)

			// First key/value should still be valid after second Next() call
			require.NotNil(t, firstKey, "First key should still be valid")
			require.NotNil(t, firstVal, "First value should still be valid")

			// Keys should be different (we don't test ordering here since it's tested elsewhere)
			require.NotEqual(t, firstKey, secondKey, "Keys should be different")
		}
	}

	// Test HasNext() idempotency (Invariant 1)
	for i := 0; i < 5; i++ {
		hasNext1 := it.HasNext()
		hasNext2 := it.HasNext()
		require.Equal(t, hasNext1, hasNext2, "HasNext() should be idempotent")

		if hasNext1 {
			it.Next()
			break // Only test one iteration to avoid consuming all data
		}
	}
}

func TestHistoryRange_ComprehensiveIntegration(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		defer db.Close()

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()

		// Test 1: Comprehensive range with various scenarios
		testCases := []struct {
			name        string
			fromTxNum   int
			toTxNum     int
			limit       int
			expectEmpty bool
		}{
			{"early_range", 1, 10, -1, false},
			{"mid_range", 100, 200, -1, false},
			{"late_range", 900, 1000, -1, false},
			{"empty_range_same", 10, 10, -1, true},
			{"empty_range_reverse", 20, 10, -1, true},
			{"future_range", 2000, 3000, -1, true},
			{"no_lower_bound", -1, 50, 10, false},
			{"no_upper_bound", 950, -1, 10, false},
			{"no_bounds", -1, -1, 5, false},
			{"zero_limit", 1, 100, 0, true},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				it, err := hc.HistoryRange(tc.fromTxNum, tc.toTxNum, order.Asc, tc.limit, tx)
				require.NoError(err, "HistoryRange should not error for case: %s", tc.name)
				defer it.Close()

				var keys []string
				count := 0
				var prevKey []byte

				for it.HasNext() {
					k, v, err := it.Next()
					require.NoError(err, "Next() should not error for case: %s", tc.name)

					// Verify ordering invariant
					if prevKey != nil {
						require.True(bytes.Compare(prevKey, k) < 0,
							"Keys should be in ascending order in case: %s", tc.name)
					}

					// Verify key format (should be 8-byte big-endian with first byte = 1)
					require.Len(k, 8, "Key should be 8 bytes in case: %s", tc.name)
					require.Equal(byte(1), k[0], "Key should start with 1 in case: %s", tc.name)

					// Value can be empty or should be 8 bytes with first byte = 255
					if len(v) > 0 {
						require.Len(v, 8, "Non-empty value should be 8 bytes in case: %s", tc.name)
						require.Equal(byte(255), v[0], "Value should start with 255 in case: %s", tc.name)
					}

					keys = append(keys, fmt.Sprintf("%x", k))
					prevKey = make([]byte, len(k))
					copy(prevKey, k)
					count++

					// Safety check to prevent infinite loops in tests
					if count > 1000 {
						t.Fatalf("Too many results (>1000) for case: %s", tc.name)
					}
				}

				if tc.expectEmpty {
					require.Equal(0, count, "Expected empty result for case: %s", tc.name)
				} else {
					require.Greater(count, 0, "Expected non-empty result for case: %s", tc.name)
				}

				if tc.limit > 0 {
					require.LessOrEqual(count, tc.limit, "Should respect limit for case: %s", tc.name)
				}

				// Verify no duplicates
				uniqueKeys := make(map[string]bool)
				for _, key := range keys {
					require.False(uniqueKeys[key], "Found duplicate key %s in case: %s", key, tc.name)
					uniqueKeys[key] = true
				}
			})
		}

		// Test 2: Before and after merge consistency
		// Store results before merge
		beforeMergeResults := make(map[string][]string)
		testRanges := []struct {
			from, to int
		}{
			{1, 50},
			{100, 200},
			{500, 600},
		}

		for _, tr := range testRanges {
			it, err := hc.HistoryRange(tr.from, tr.to, order.Asc, -1, tx)
			require.NoError(err)

			var keys []string
			for it.HasNext() {
				k, _, err := it.Next()
				require.NoError(err)
				keys = append(keys, fmt.Sprintf("%x", k))
			}
			it.Close()
			beforeMergeResults[fmt.Sprintf("%d-%d", tr.from, tr.to)] = keys
		}

		// Merge data
		collateAndMergeHistory(t, db, h, txs, true)

		// Verify results after merge are identical
		for _, tr := range testRanges {
			it, err := hc.HistoryRange(tr.from, tr.to, order.Asc, -1, tx)
			require.NoError(err)

			var keysAfterMerge []string
			for it.HasNext() {
				k, _, err := it.Next()
				require.NoError(err)
				keysAfterMerge = append(keysAfterMerge, fmt.Sprintf("%x", k))
			}
			it.Close()

			rangeKey := fmt.Sprintf("%d-%d", tr.from, tr.to)
			require.Equal(beforeMergeResults[rangeKey], keysAfterMerge,
				"Results should be identical before and after merge for range %s", rangeKey)
		}

		// Test 3: Descending order should panic
		require.Panics(func() {
			hc.HistoryRange(1, 100, order.Desc, -1, tx)
		}, "Descending order should panic")

		// Test 4: Verify mathematical relationship between ranges
		// If we have ranges [a,b) and [b,c), the union should equal [a,c)
		it1, err := hc.HistoryRange(10, 20, order.Asc, -1, tx)
		require.NoError(err)
		keys1 := make(map[string]bool)
		for it1.HasNext() {
			k, _, err := it1.Next()
			require.NoError(err)
			keys1[fmt.Sprintf("%x", k)] = true
		}
		it1.Close()

		it2, err := hc.HistoryRange(20, 30, order.Asc, -1, tx)
		require.NoError(err)
		keys2 := make(map[string]bool)
		for it2.HasNext() {
			k, _, err := it2.Next()
			require.NoError(err)
			keys2[fmt.Sprintf("%x", k)] = true
		}
		it2.Close()

		it3, err := hc.HistoryRange(10, 30, order.Asc, -1, tx)
		require.NoError(err)
		keys3 := make(map[string]bool)
		for it3.HasNext() {
			k, _, err := it3.Next()
			require.NoError(err)
			keys3[fmt.Sprintf("%x", k)] = true
		}
		it3.Close()

		// Verify union property: keys3 should contain all keys from keys1 and keys2
		for key := range keys1 {
			require.True(keys3[key], "Union range should contain key %s from first range", key)
		}
		for key := range keys2 {
			require.True(keys3[key], "Union range should contain key %s from second range", key)
		}
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}
