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
