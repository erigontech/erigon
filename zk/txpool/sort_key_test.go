package txpool

import (
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
)

// TestSortKeyEquivalence verifies that betterUsingSortKey produces the same results as better()
func TestSortKeyEquivalence(t *testing.T) {
	pendingBaseFee := uint64(1000000000) // 1 gwei

	// Create test transactions with different properties to test all comparison paths
	testCases := []struct {
		name string
		tx1  *metaTx
		tx2  *metaTx
	}{
		{
			name: "different_subpool_priority",
			tx1: &metaTx{
				Tx:                        &types.TxSlot{SenderID: 1, Nonce: 1, FeeCap: *uint256.NewInt(2000000000), Tip: *uint256.NewInt(1000000000)},
				minFeeCap:                 *uint256.NewInt(2000000000),
				minTip:                    1000000000,
				nonceDistance:             0,
				cumulativeBalanceDistance: 0,
				timestamp:                 uint64(time.Now().Unix()),
				subPool:                   EnoughFeeCapProtocol | NoNonceGaps | EnoughBalance | NotTooMuchGas,
				currentSubPool:            PendingSubPool,
			},
			tx2: &metaTx{
				Tx:                        &types.TxSlot{SenderID: 2, Nonce: 1, FeeCap: *uint256.NewInt(1500000000), Tip: *uint256.NewInt(500000000)},
				minFeeCap:                 *uint256.NewInt(1500000000),
				minTip:                    500000000,
				nonceDistance:             1,
				cumulativeBalanceDistance: 1000,
				timestamp:                 uint64(time.Now().Unix()),
				subPool:                   EnoughFeeCapProtocol | EnoughBalance | NotTooMuchGas, // Missing NoNonceGaps
				currentSubPool:            PendingSubPool,
			},
		},
		{
			name: "pending_pool_different_effective_tip",
			tx1: &metaTx{
				Tx:                        &types.TxSlot{SenderID: 1, Nonce: 1, FeeCap: *uint256.NewInt(2000000000), Tip: *uint256.NewInt(1500000000)},
				minFeeCap:                 *uint256.NewInt(2000000000),
				minTip:                    1500000000,
				nonceDistance:             0,
				cumulativeBalanceDistance: 0,
				timestamp:                 uint64(time.Now().Unix()),
				subPool:                   EnoughFeeCapProtocol | NoNonceGaps | EnoughBalance | NotTooMuchGas,
				currentSubPool:            PendingSubPool,
			},
			tx2: &metaTx{
				Tx:                        &types.TxSlot{SenderID: 2, Nonce: 1, FeeCap: *uint256.NewInt(2000000000), Tip: *uint256.NewInt(1000000000)},
				minFeeCap:                 *uint256.NewInt(2000000000),
				minTip:                    1000000000,
				nonceDistance:             0,
				cumulativeBalanceDistance: 0,
				timestamp:                 uint64(time.Now().Unix()),
				subPool:                   EnoughFeeCapProtocol | NoNonceGaps | EnoughBalance | NotTooMuchGas,
				currentSubPool:            PendingSubPool,
			},
		},
		{
			name: "pending_pool_different_nonce_distance",
			tx1: &metaTx{
				Tx:                        &types.TxSlot{SenderID: 1, Nonce: 1, FeeCap: *uint256.NewInt(2000000000), Tip: *uint256.NewInt(1000000000)},
				minFeeCap:                 *uint256.NewInt(2000000000),
				minTip:                    1000000000,
				nonceDistance:             0, // Better (lower nonce distance)
				cumulativeBalanceDistance: 0,
				timestamp:                 uint64(time.Now().Unix()),
				subPool:                   EnoughFeeCapProtocol | NoNonceGaps | EnoughBalance | NotTooMuchGas,
				currentSubPool:            PendingSubPool,
			},
			tx2: &metaTx{
				Tx:                        &types.TxSlot{SenderID: 2, Nonce: 1, FeeCap: *uint256.NewInt(2000000000), Tip: *uint256.NewInt(1000000000)},
				minFeeCap:                 *uint256.NewInt(2000000000),
				minTip:                    1000000000,
				nonceDistance:             2, // Worse (higher nonce distance)
				cumulativeBalanceDistance: 0,
				timestamp:                 uint64(time.Now().Unix()),
				subPool:                   EnoughFeeCapProtocol | NoNonceGaps | EnoughBalance | NotTooMuchGas,
				currentSubPool:            PendingSubPool,
			},
		},
		{
			name: "basefee_pool_different_feecap",
			tx1: &metaTx{
				Tx:                        &types.TxSlot{SenderID: 1, Nonce: 1, FeeCap: *uint256.NewInt(2000000000)},
				minFeeCap:                 *uint256.NewInt(2000000000), // Higher fee cap is better
				nonceDistance:             0,
				cumulativeBalanceDistance: 0,
				timestamp:                 uint64(time.Now().Unix()),
				subPool:                   BaseFeePoolBits,
				currentSubPool:            BaseFeeSubPool,
			},
			tx2: &metaTx{
				Tx:                        &types.TxSlot{SenderID: 2, Nonce: 1, FeeCap: *uint256.NewInt(1500000000)},
				minFeeCap:                 *uint256.NewInt(1500000000), // Lower fee cap is worse
				nonceDistance:             0,
				cumulativeBalanceDistance: 0,
				timestamp:                 uint64(time.Now().Unix()),
				subPool:                   BaseFeePoolBits,
				currentSubPool:            BaseFeeSubPool,
			},
		},
		{
			name: "queued_pool_different_balance_distance",
			tx1: &metaTx{
				Tx:                        &types.TxSlot{SenderID: 1, Nonce: 1, FeeCap: *uint256.NewInt(1500000000)},
				minFeeCap:                 *uint256.NewInt(1500000000),
				nonceDistance:             1,
				cumulativeBalanceDistance: 100, // Lower balance distance is better
				timestamp:                 uint64(time.Now().Unix()),
				subPool:                   0, // Queued pool has minimal bits
				currentSubPool:            QueuedSubPool,
			},
			tx2: &metaTx{
				Tx:                        &types.TxSlot{SenderID: 2, Nonce: 1, FeeCap: *uint256.NewInt(1500000000)},
				minFeeCap:                 *uint256.NewInt(1500000000),
				nonceDistance:             1,
				cumulativeBalanceDistance: 1000, // Higher balance distance is worse
				timestamp:                 uint64(time.Now().Unix()),
				subPool:                   0,
				currentSubPool:            QueuedSubPool,
			},
		},
		{
			name: "timestamp_tiebreaker",
			tx1: &metaTx{
				Tx:                        &types.TxSlot{SenderID: 1, Nonce: 1, FeeCap: *uint256.NewInt(2000000000), Tip: *uint256.NewInt(1000000000)},
				minFeeCap:                 *uint256.NewInt(2000000000),
				minTip:                    1000000000,
				nonceDistance:             0,
				cumulativeBalanceDistance: 0,
				timestamp:                 1000, // Earlier timestamp is better
				subPool:                   EnoughFeeCapProtocol | NoNonceGaps | EnoughBalance | NotTooMuchGas,
				currentSubPool:            PendingSubPool,
			},
			tx2: &metaTx{
				Tx:                        &types.TxSlot{SenderID: 2, Nonce: 1, FeeCap: *uint256.NewInt(2000000000), Tip: *uint256.NewInt(1000000000)},
				minFeeCap:                 *uint256.NewInt(2000000000),
				minTip:                    1000000000,
				nonceDistance:             0,
				cumulativeBalanceDistance: 0,
				timestamp:                 2000, // Later timestamp is worse
				subPool:                   EnoughFeeCapProtocol | NoNonceGaps | EnoughBalance | NotTooMuchGas,
				currentSubPool:            PendingSubPool,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test both directions to ensure consistency
			originalResult1 := tc.tx1.better(tc.tx2, pendingBaseFee)
			sortKeyResult1 := tc.tx1.betterUsingSortKey(tc.tx2, pendingBaseFee)

			originalResult2 := tc.tx2.better(tc.tx1, pendingBaseFee)
			sortKeyResult2 := tc.tx2.betterUsingSortKey(tc.tx1, pendingBaseFee)

			assert.Equal(t, originalResult1, sortKeyResult1, "tx1.better(tx2) != tx1.betterUsingSortKey(tx2)")
			assert.Equal(t, originalResult2, sortKeyResult2, "tx2.better(tx1) != tx2.betterUsingSortKey(tx1)")

			// Ensure they are consistent (one should be better than the other, not both)
			if originalResult1 && originalResult2 {
				t.Error("Both transactions cannot be better than each other")
			}
		})
	}
}

// BenchmarkSortKeyVsBetter compares the performance of the sort key approach vs the original better() method
func BenchmarkSortKeyVsBetter(b *testing.B) {
	pendingBaseFee := uint64(1000000000)

	// Create two test transactions with realistic values
	tx1 := &metaTx{
		Tx:                        &types.TxSlot{SenderID: 1, Nonce: 1, FeeCap: *uint256.NewInt(2000000000), Tip: *uint256.NewInt(1500000000)},
		minFeeCap:                 *uint256.NewInt(2000000000),
		minTip:                    1500000000,
		nonceDistance:             0,
		cumulativeBalanceDistance: 0,
		timestamp:                 uint64(time.Now().Unix()),
		subPool:                   EnoughFeeCapProtocol | NoNonceGaps | EnoughBalance | NotTooMuchGas,
		currentSubPool:            PendingSubPool,
	}

	tx2 := &metaTx{
		Tx:                        &types.TxSlot{SenderID: 2, Nonce: 1, FeeCap: *uint256.NewInt(2000000000), Tip: *uint256.NewInt(1000000000)},
		minFeeCap:                 *uint256.NewInt(2000000000),
		minTip:                    1000000000,
		nonceDistance:             1,
		cumulativeBalanceDistance: 100,
		timestamp:                 uint64(time.Now().Unix()) + 1,
		subPool:                   EnoughFeeCapProtocol | NoNonceGaps | EnoughBalance | NotTooMuchGas,
		currentSubPool:            PendingSubPool,
	}

	b.Run("original_better", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = tx1.better(tx2, pendingBaseFee)
		}
	})

	b.Run("sort_key_optimized", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = tx1.betterUsingSortKey(tx2, pendingBaseFee)
		}
	})
}

// BenchmarkSortKeyGeneration measures the cost of generating sort keys
func BenchmarkSortKeyGeneration(b *testing.B) {
	pendingBaseFee := uint64(1000000000)

	tx := &metaTx{
		Tx:                        &types.TxSlot{SenderID: 1, Nonce: 1, FeeCap: *uint256.NewInt(2000000000), Tip: *uint256.NewInt(1500000000)},
		minFeeCap:                 *uint256.NewInt(2000000000),
		minTip:                    1500000000,
		nonceDistance:             0,
		cumulativeBalanceDistance: 0,
		timestamp:                 uint64(time.Now().Unix()),
		subPool:                   EnoughFeeCapProtocol | NoNonceGaps | EnoughBalance | NotTooMuchGas,
		currentSubPool:            PendingSubPool,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Invalidate the sort key to force regeneration
		tx.invalidateSortKey()
		tx.generateSortKey(pendingBaseFee)
	}
}

// TestSortKeyInvalidation verifies that sort keys are properly invalidated when transaction properties change
func TestSortKeyInvalidation(t *testing.T) {
	pendingBaseFee := uint64(1000000000)

	tx := &metaTx{
		Tx:                        &types.TxSlot{SenderID: 1, Nonce: 1, FeeCap: *uint256.NewInt(2000000000), Tip: *uint256.NewInt(1500000000)},
		minFeeCap:                 *uint256.NewInt(2000000000),
		minTip:                    1500000000,
		nonceDistance:             0,
		cumulativeBalanceDistance: 0,
		timestamp:                 uint64(time.Now().Unix()),
		subPool:                   EnoughFeeCapProtocol | NoNonceGaps | EnoughBalance | NotTooMuchGas,
		currentSubPool:            PendingSubPool,
	}

	// Generate initial sort key
	tx.generateSortKey(pendingBaseFee)
	assert.True(t, tx.sortKeyValid, "Sort key should be valid after generation")
	initialKey := make([]byte, len(tx.sortKey))
	copy(initialKey, tx.sortKey)

	// Invalidate the sort key
	tx.invalidateSortKey()
	assert.False(t, tx.sortKeyValid, "Sort key should be invalid after invalidation")

	// Modify transaction properties and regenerate
	tx.nonceDistance = 5
	tx.generateSortKey(pendingBaseFee)
	assert.True(t, tx.sortKeyValid, "Sort key should be valid after regeneration")

	// Verify the key changed due to the property modification
	assert.NotEqual(t, initialKey, tx.sortKey, "Sort key should change when transaction properties change")
}

// TestPendingBaseFeeChange verifies that sort keys are properly invalidated when pendingBaseFee changes
func TestPendingBaseFeeChange(t *testing.T) {
	initialBaseFee := uint64(1000000000) // 1 gwei
	newBaseFee := uint64(2000000000)     // 2 gwei

	// Create a transaction that will be affected by the base fee change
	tx := &metaTx{
		Tx:                        &types.TxSlot{SenderID: 1, Nonce: 1, FeeCap: *uint256.NewInt(1500000000), Tip: *uint256.NewInt(1000000000)},
		minFeeCap:                 *uint256.NewInt(1500000000), // Between the two base fees
		minTip:                    1000000000,
		nonceDistance:             0,
		cumulativeBalanceDistance: 0,
		timestamp:                 uint64(time.Now().Unix()),
		subPool:                   EnoughFeeCapProtocol | NoNonceGaps | EnoughBalance | NotTooMuchGas,
		currentSubPool:            PendingSubPool,
	}

	// Generate sort key with initial base fee
	tx.generateSortKey(initialBaseFee)
	assert.True(t, tx.sortKeyValid, "Sort key should be valid after generation")
	initialKey := make([]byte, len(tx.sortKey))
	copy(initialKey, tx.sortKey)

	// Generate sort key with new base fee (should produce different key)
	tx.invalidateSortKey()
	tx.generateSortKey(newBaseFee)
	newKey := make([]byte, len(tx.sortKey))
	copy(newKey, tx.sortKey)

	// The sort keys should be different because the EnoughFeeCapBlock bit changes
	// when pendingBaseFee changes from 1 gwei to 2 gwei for a 1.5 gwei feeCap transaction
	assert.NotEqual(t, initialKey, newKey, "Sort key should change when pendingBaseFee changes and affects EnoughFeeCapBlock bit")

	// Verify the specific bit that should change
	// With 1 gwei base fee: 1.5 gwei feeCap >= 1 gwei → EnoughFeeCapBlock = 1
	// With 2 gwei base fee: 1.5 gwei feeCap < 2 gwei → EnoughFeeCapBlock = 0
	// This should be reflected in byte 0 of the sort key (inverted subPool)
	assert.NotEqual(t, initialKey[0], newKey[0], "First byte of sort key should change when EnoughFeeCapBlock bit changes")
}

// TestSelectiveBaseFeeInvalidation verifies that selective invalidation only affects transactions
// whose EnoughFeeCapBlock bit would change
func TestSelectiveBaseFeeInvalidation(t *testing.T) {
	oldBaseFee := uint64(1000000000) // 1 gwei
	newBaseFee := uint64(2000000000) // 2 gwei

	// Create transactions with different fee caps
	txAffected := &metaTx{
		Tx:                        &types.TxSlot{SenderID: 1, Nonce: 1, FeeCap: *uint256.NewInt(1500000000)}, // 1.5 gwei - will be affected
		minFeeCap:                 *uint256.NewInt(1500000000),
		minTip:                    1000000000,
		nonceDistance:             0,
		cumulativeBalanceDistance: 0,
		timestamp:                 uint64(time.Now().Unix()),
		subPool:                   EnoughFeeCapProtocol | NoNonceGaps | EnoughBalance | NotTooMuchGas,
		currentSubPool:            PendingSubPool,
		sortKeyValid:              true, // Simulate already having a valid sort key
	}

	txUnaffected := &metaTx{
		Tx:                        &types.TxSlot{SenderID: 2, Nonce: 1, FeeCap: *uint256.NewInt(3000000000)}, // 3 gwei - won't be affected
		minFeeCap:                 *uint256.NewInt(3000000000),
		minTip:                    1000000000,
		nonceDistance:             0,
		cumulativeBalanceDistance: 0,
		timestamp:                 uint64(time.Now().Unix()),
		subPool:                   EnoughFeeCapProtocol | NoNonceGaps | EnoughBalance | NotTooMuchGas,
		currentSubPool:            PendingSubPool,
		sortKeyValid:              true, // Simulate already having a valid sort key
	}

	// Test the helper function directly
	assert.True(t, wouldEnoughFeeCapBlockChange(txAffected, oldBaseFee, newBaseFee),
		"Transaction with 1.5 gwei feeCap should be affected by base fee change from 1 to 2 gwei")

	assert.False(t, wouldEnoughFeeCapBlockChange(txUnaffected, oldBaseFee, newBaseFee),
		"Transaction with 3 gwei feeCap should not be affected by base fee change from 1 to 2 gwei")

	// Create a pending pool and add transactions
	pool := NewPendingSubPool(PendingSubPool, 1000, true)
	pool.Add(txAffected)
	pool.Add(txUnaffected)

	// Both should have valid sort keys initially
	assert.True(t, txAffected.sortKeyValid, "Affected transaction should have valid sort key initially")
	assert.True(t, txUnaffected.sortKeyValid, "Unaffected transaction should have valid sort key initially")

	// Apply selective invalidation
	pool.invalidateSortKeysForBaseFeeChange(oldBaseFee, newBaseFee)

	// Only the affected transaction should have its sort key invalidated
	assert.False(t, txAffected.sortKeyValid, "Affected transaction should have invalid sort key after base fee change")
	assert.True(t, txUnaffected.sortKeyValid, "Unaffected transaction should still have valid sort key after base fee change")
}
