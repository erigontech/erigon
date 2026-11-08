package state

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func TestHistoryChainBatchOversizedWeightDoesNotBlock(t *testing.T) {
	budget := int64(commitmentV3BatchBytes * 2)
	batch := &historyChainBatch{buf: make([]byte, budget+1)}
	weight := historyBatchWeight(int64(len(batch.buf)), budget)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	sem := semaphore.NewWeighted(budget)
	require.NoError(t, sem.Acquire(ctx, weight))
	require.Equal(t, budget, weight)
	sem.Release(weight)
}

func TestHistoryChainBatchWeightStaysWithinBudget(t *testing.T) {
	budget := int64(commitmentV3BatchBytes * 2)
	batch := &historyChainBatch{buf: make([]byte, budget-1)}
	weight := historyBatchWeight(int64(len(batch.buf)), budget)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	sem := semaphore.NewWeighted(budget)
	require.NoError(t, sem.Acquire(ctx, weight))
	require.LessOrEqual(t, weight, budget)
	sem.Release(weight)
}
