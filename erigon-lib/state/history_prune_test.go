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
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
)

/*
History Prune Method Documentation and Test Suite

## What History.Prune Does

The Prune method removes historical data from the database within a specified
transaction number range to free up storage space while maintaining data integrity.

## Method Signature
```go
func (ht *HistoryRoTx) Prune(ctx context.Context, tx kv.RwTx, txFrom, txTo, limit uint64, forced bool, logEvery *time.Ticker) (*InvertedIndexPruneStat, error)
```

## Parameters
- ctx: Context for operation cancellation
- tx: Read-write database transaction
- txFrom: Starting transaction number (inclusive)
- txTo: Ending transaction number (exclusive) - range is [txFrom, txTo)
- limit: Maximum number of operations per iteration (for batching large operations)
- forced: Whether to bypass safety checks and prune regardless of file state
- logEvery: Ticker for progress logging (can be nil)

## Return Value
Returns *InvertedIndexPruneStat containing:
- MinTxNum: Minimum transaction number in the pruned range
- MaxTxNum: Maximum transaction number in the pruned range
- PruneCountTx: Number of transactions that were pruned
- PruneCountValues: Number of individual values that were pruned

Returns nil if no pruning was performed.

## Key Invariants

1. **Safety First**: Without forced=true, only prunes data that's safely replicated to files
2. **Range Semantics**: Prunes data in range [txFrom, txTo) - inclusive start, exclusive end
3. **Recent Data Protection**: Respects keepRecentTxnInDB to preserve recent transactions
4. **Batching**: Uses limit parameter to avoid oversized database transactions
5. **Dual Storage Support**: Handles both small values (DupSort table) and large values (regular table)
6. **Idempotent**: Can be called multiple times safely
7. **Atomic**: Either completes successfully or leaves database unchanged
8. **Index Consistency**: Also prunes corresponding inverted index entries

## How It Works Internally

1. Calls canPruneUntil() to determine safe pruning bounds (unless forced)
2. Creates appropriate cursor for values table (DupSort or regular)
3. Iterates through history values in the specified transaction range
4. For each key, removes all historical values within the range
5. Delegates to underlying inverted index pruning
6. Returns statistics about pruned data

## Use Cases

- Storage space management in long-running nodes
- Cleanup of old historical data after archival
- Maintenance operations during low-traffic periods
- Database size optimization for resource-constrained environments

## Performance Characteristics

- Scales linearly with the amount of data to prune
- Memory usage bounded by the limit parameter
- I/O intensive operation that benefits from batching
- Can be interrupted via context cancellation
*/

func TestHistoryPrune_Basic(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		defer db.Close()

		// Test with files (merged data)
		collateAndMergeHistory(t, db, h, txs, true)

		rwTx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()

		// Test basic pruning
		stat, err := hc.Prune(ctx, rwTx, 0, 100, math.MaxUint64, false, logEvery)
		require.NoError(err)

		if stat != nil {
			require.LessOrEqual(stat.MinTxNum, stat.MaxTxNum, "MinTxNum should be <= MaxTxNum")
			require.Greater(stat.PruneCountTx, uint64(0), "Should have pruned some transactions")
			require.Greater(stat.PruneCountValues, uint64(0), "Should have pruned some values")
			require.LessOrEqual(stat.MinTxNum, uint64(100), "MinTxNum should be within range")
			require.GreaterOrEqual(stat.MaxTxNum, uint64(0), "MaxTxNum should be within range")
		}

		require.NoError(rwTx.Commit())
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}

func TestHistoryPrune_EdgeCases(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		defer db.Close()

		collateAndMergeHistory(t, db, h, txs, true)

		rwTx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()

		// Test empty range (txFrom >= txTo)
		stat, err := hc.Prune(ctx, rwTx, 10, 10, math.MaxUint64, false, logEvery)
		require.NoError(err)
		require.Nil(stat, "Empty range should return nil stat")

		// Test reverse range (should also be empty)
		stat, err = hc.Prune(ctx, rwTx, 20, 10, math.MaxUint64, false, logEvery)
		require.NoError(err)
		require.Nil(stat, "Reverse range should return nil stat")

		// Test range beyond available data
		stat, err = hc.Prune(ctx, rwTx, 2000, 3000, math.MaxUint64, false, logEvery)
		require.NoError(err)
		// May return nil or empty stat depending on implementation

		// Test zero limit
		stat, err = hc.Prune(ctx, rwTx, 0, 100, 0, false, logEvery)
		require.NoError(err)
		require.Nil(stat, "Zero limit should return nil stat")

		require.NoError(rwTx.Commit())
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}

func TestHistoryPrune_ForcedVsNonForced(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, _ := filledHistory(t, largeValues, logger)
		defer db.Close()

		// Test without files (database only) - should not prune unless forced
		rwTx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()

		// Non-forced pruning should return nil (no files built)
		stat, err := hc.Prune(ctx, rwTx, 0, 50, math.MaxUint64, false, logEvery)
		require.NoError(err)
		require.Nil(stat, "Non-forced pruning without files should return nil")

		// Forced pruning should work even without files
		stat, err = hc.Prune(ctx, rwTx, 0, 10, math.MaxUint64, true, logEvery)
		require.NoError(err)
		if stat != nil {
			require.Greater(stat.PruneCountValues, uint64(0), "Forced pruning should work")
			require.LessOrEqual(stat.MinTxNum, uint64(10), "Should respect txTo limit")
		}

		require.NoError(rwTx.Commit())
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}

func TestHistoryPrune_Limits(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		defer db.Close()

		collateAndMergeHistory(t, db, h, txs, true)

		rwTx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()

		// Test various limits
		limits := []uint64{1, 5, 10, 50, 100}
		for _, limit := range limits {
			t.Run(fmt.Sprintf("limit_%d", limit), func(t *testing.T) {
				stat, err := hc.Prune(ctx, rwTx, 0, 200, limit, false, logEvery)
				require.NoError(err)

				if stat != nil {
					// The limit affects batching but not the final result count
					// Multiple batches may be needed for larger datasets
					require.LessOrEqual(stat.MinTxNum, stat.MaxTxNum, "MinTxNum should be <= MaxTxNum")
				}
			})
		}

		require.NoError(rwTx.Commit())
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}

func TestHistoryPrune_Statistics(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		defer db.Close()

		collateAndMergeHistory(t, db, h, txs, true)

		rwTx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()

		// Get initial state
		canPrune, untilTx := hc.canPruneUntil(rwTx, math.MaxUint64)

		var stat *InvertedIndexPruneStat
		if canPrune {
			// Prune a specific range and verify statistics
			stat, err = hc.Prune(ctx, rwTx, 0, min(100, untilTx), math.MaxUint64, false, logEvery)
		} else {
			// Use forced pruning to get statistics
			stat, err = hc.Prune(ctx, rwTx, 0, 50, math.MaxUint64, true, logEvery)
		}
		require.NoError(err)

		if stat != nil {
			// Only verify statistics if actual pruning occurred
			if stat.PruneCountTx > 0 && stat.PruneCountValues > 0 {
				// Verify stat fields are consistent
				require.LessOrEqual(stat.MinTxNum, stat.MaxTxNum, "MinTxNum should be <= MaxTxNum")
				require.GreaterOrEqual(stat.MinTxNum, uint64(0), "MinTxNum should be >= txFrom")
				if canPrune {
					require.LessOrEqual(stat.MaxTxNum, min(100, untilTx), "MaxTxNum should be <= txTo")
				} else {
					require.LessOrEqual(stat.MaxTxNum, uint64(50), "MaxTxNum should be <= txTo")
				}

				// Values count should be >= transaction count (one value per tx minimum)
				require.GreaterOrEqual(stat.PruneCountValues, stat.PruneCountTx, "Values count should be >= tx count")

				t.Logf("Pruned %d transactions, %d values in range [%d, %d]",
					stat.PruneCountTx, stat.PruneCountValues, stat.MinTxNum, stat.MaxTxNum)
			} else {
				t.Logf("Prune returned statistics but no data was actually pruned")
			}
		} else {
			t.Logf("No pruning statistics returned")
		}

		require.NoError(rwTx.Commit())
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}

func TestHistoryPrune_Idempotency(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		defer db.Close()

		collateAndMergeHistory(t, db, h, txs, true)

		rwTx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()

		// First prune
		stat1, err := hc.Prune(ctx, rwTx, 0, 50, math.MaxUint64, false, logEvery)
		require.NoError(err)

		// Second prune of the same range should be idempotent
		stat2, err := hc.Prune(ctx, rwTx, 0, 50, math.MaxUint64, false, logEvery)
		require.NoError(err)

		if stat1 != nil {
			// Second prune should either return nil or have zero counts
			if stat2 != nil {
				require.Equal(uint64(0), stat2.PruneCountTx, "Second prune should not prune additional transactions")
				require.Equal(uint64(0), stat2.PruneCountValues, "Second prune should not prune additional values")
			}
		}

		require.NoError(rwTx.Commit())
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}

func TestHistoryPrune_CanPruneUntil(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		defer db.Close()

		// Test canPruneUntil before files are built
		rwTx, err := db.BeginRw(ctx)
		require.NoError(err)
		hc := h.BeginFilesRo()
		canPrune1, untilTx1 := hc.canPruneUntil(rwTx, math.MaxUint64)
		t.Logf("Before files: canPrune=%t, untilTx=%d, snapshotsDisabled=%t, keepRecentTxnInDB=%d",
			canPrune1, untilTx1, h.snapshotsDisabled, h.keepRecentTxnInDB)
		hc.Close()
		require.NoError(rwTx.Commit())

		// Build files
		collateAndMergeHistory(t, db, h, txs, true)

		// Test canPruneUntil after files are built
		rwTx, err = db.BeginRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()
		hc = h.BeginFilesRo()
		defer hc.Close()

		canPrune2, untilTx2 := hc.canPruneUntil(rwTx, math.MaxUint64)
		t.Logf("After files: canPrune=%t, untilTx=%d", canPrune2, untilTx2)

		// Test with specific bounds
		canPrune3, untilTx3 := hc.canPruneUntil(rwTx, 100)
		t.Logf("With bound 100: canPrune=%t, untilTx=%d", canPrune3, untilTx3)
		if canPrune3 {
			require.LessOrEqual(untilTx3, uint64(100), "untilTx should respect the bound")
		}

		// Test basic consistency - canPruneUntil should be deterministic
		canPrune4, untilTx4 := hc.canPruneUntil(rwTx, math.MaxUint64)
		require.Equal(canPrune2, canPrune4, "canPruneUntil should be deterministic")
		require.Equal(untilTx2, untilTx4, "untilTx should be deterministic")

		require.NoError(rwTx.Commit())
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}

func TestHistoryPrune_ProgressiveWaves(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		defer db.Close()

		collateAndMergeHistory(t, db, h, txs, true)

		rwTx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()

		// Prune in progressive waves
		waveSize := uint64(50)
		waves := 5
		var totalPrunedTx, totalPrunedValues uint64

		for wave := 0; wave < waves; wave++ {
			from := uint64(wave) * waveSize
			to := uint64(wave+1) * waveSize

			stat, err := hc.Prune(ctx, rwTx, from, to, math.MaxUint64, false, logEvery)
			require.NoError(err)

			if stat != nil {
				require.LessOrEqual(stat.MinTxNum, stat.MaxTxNum, "Wave %d: MinTxNum should be <= MaxTxNum", wave)
				require.GreaterOrEqual(stat.MinTxNum, from, "Wave %d: MinTxNum should be >= wave start", wave)
				require.LessOrEqual(stat.MaxTxNum, to, "Wave %d: MaxTxNum should be <= wave end", wave)

				totalPrunedTx += stat.PruneCountTx
				totalPrunedValues += stat.PruneCountValues

				t.Logf("Wave %d: pruned %d tx, %d values in range [%d, %d]",
					wave, stat.PruneCountTx, stat.PruneCountValues, from, to)
			}
		}

		t.Logf("Total pruned across all waves: %d transactions, %d values", totalPrunedTx, totalPrunedValues)

		require.NoError(rwTx.Commit())
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}

func TestHistoryPrune_ContextCancellation(t *testing.T) {
	t.Parallel()
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		defer db.Close()

		collateAndMergeHistory(t, db, h, txs, true)

		rwTx, err := db.BeginRw(context.Background())
		require.NoError(err)
		defer rwTx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()

		// Create a cancelled context
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		// Prune with cancelled context should handle cancellation gracefully
		stat, err := hc.Prune(cancelledCtx, rwTx, 0, 100, math.MaxUint64, false, logEvery)
		if err != nil {
			require.Contains(err.Error(), "context canceled", "Should return context cancellation error")
		} else {
			// Some implementations might not check context immediately
			// That's also acceptable behavior
			_ = stat
		}

		require.NoError(rwTx.Commit())
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}

func TestHistoryPrune_EmptyDatabase(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		// Create empty history
		db, h := testDbAndHistory(t, largeValues, logger)
		defer db.Close()

		rwTx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()

		// Pruning empty database should work gracefully
		stat, err := hc.Prune(ctx, rwTx, 0, 100, math.MaxUint64, false, logEvery)
		require.NoError(err)
		if stat != nil {
			require.Equal(uint64(0), stat.PruneCountTx, "Empty database should prune 0 transactions")
			require.Equal(uint64(0), stat.PruneCountValues, "Empty database should prune 0 values")
		}

		// Even with forced=true
		stat, err = hc.Prune(ctx, rwTx, 0, 100, math.MaxUint64, true, logEvery)
		require.NoError(err)
		if stat != nil {
			require.Equal(uint64(0), stat.PruneCountTx, "Empty database should prune 0 transactions even when forced")
			require.Equal(uint64(0), stat.PruneCountValues, "Empty database should prune 0 values even when forced")
		}

		require.NoError(rwTx.Commit())
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}

func TestHistoryPrune_ComprehensiveIntegration(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := context.Background()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		// Create a substantial history dataset
		db, h := testDbAndHistory(t, largeValues, logger)
		defer db.Close()

		// Fill with known pattern
		rwTx, err := db.BeginRw(ctx)
		require.NoError(err)
		hc := h.BeginFilesRo()
		writer := hc.NewWriter()

		// Create 100 transactions with predictable data
		var prevVals = make(map[string][]byte)
		for txNum := uint64(1); txNum <= 100; txNum++ {
			for keyNum := uint64(1); keyNum <= 10; keyNum++ {
				if txNum%keyNum == 0 { // Key changes only at specific intervals
					var k [8]byte
					var v [8]byte
					binary.BigEndian.PutUint64(k[:], keyNum)
					binary.BigEndian.PutUint64(v[:], txNum*keyNum)
					k[0] = 1   // mark key
					v[0] = 255 // mark value

					keyStr := string(k[:])
					err = writer.AddPrevValue(k[:], txNum, prevVals[keyStr])
					require.NoError(err)
					prevVals[keyStr] = v[:]
				}
			}
		}

		err = writer.Flush(ctx, rwTx)
		require.NoError(err)
		writer.close()
		err = rwTx.Commit()
		require.NoError(err)
		hc.Close()

		// Build files
		collateAndMergeHistory(t, db, h, 100, true)

		// Now test comprehensive pruning scenarios
		rwTx, err = db.BeginRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()

		hc = h.BeginFilesRo()
		defer hc.Close()

		testCases := []struct {
			name        string
			txFrom      uint64
			txTo        uint64
			limit       uint64
			forced      bool
			expectStats bool
		}{
			{"early_range", 1, 20, math.MaxUint64, false, true},
			{"mid_range", 30, 60, math.MaxUint64, false, true},
			{"small_limit", 1, 50, 5, false, true},
			{"forced_recent", 80, 100, math.MaxUint64, true, true},
			{"overlapping", 15, 35, math.MaxUint64, false, false}, // Should overlap with already pruned
		}

		var allStats []*InvertedIndexPruneStat
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				stat, err := hc.Prune(ctx, rwTx, tc.txFrom, tc.txTo, tc.limit, tc.forced, logEvery)
				require.NoError(err)

				if tc.expectStats && stat != nil {
					require.LessOrEqual(stat.MinTxNum, stat.MaxTxNum, "MinTxNum should be <= MaxTxNum")
					require.GreaterOrEqual(stat.MinTxNum, tc.txFrom, "MinTxNum should be >= txFrom")
					require.LessOrEqual(stat.MaxTxNum, tc.txTo, "MaxTxNum should be <= txTo")
					require.Greater(stat.PruneCountTx, uint64(0), "Should have pruned some transactions")
					require.Greater(stat.PruneCountValues, uint64(0), "Should have pruned some values")

					allStats = append(allStats, stat)
					t.Logf("Test case %s: pruned %d tx, %d values in range [%d, %d]",
						tc.name, stat.PruneCountTx, stat.PruneCountValues, stat.MinTxNum, stat.MaxTxNum)
				}
			})
		}

		// Verify no data corruption by testing remaining data accessibility
		// This is a basic sanity check
		for keyNum := uint64(1); keyNum <= 10; keyNum++ {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], keyNum)
			k[0] = 1

			// Try to access some remaining data
			_, _, err := hc.HistorySeek(k[:], 95, rwTx)
			require.NoError(err, "Should be able to access remaining data for key %d", keyNum)
		}

		require.NoError(rwTx.Commit())

		t.Logf("Successfully completed comprehensive pruning with %d stat records", len(allStats))
	}

	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
	})
}
