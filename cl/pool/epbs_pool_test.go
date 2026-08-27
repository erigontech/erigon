package pool

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
)

func TestRemoveHighestBidOnlyRemovesMatchingBid(t *testing.T) {
	pool := NewEpbsPool()
	key := HighestBidKey{Slot: 1}
	rejected := &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{Value: 1}}
	replacement := &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{Value: 2}}

	pool.StoreHighestBid(key, rejected)
	require.False(t, pool.RemoveHighestBid(key, replacement))
	stored, found := pool.HighestBids.Get(key)
	require.True(t, found)
	require.Same(t, rejected, stored)

	pool.StoreHighestBid(key, replacement)
	require.False(t, pool.RemoveHighestBid(key, rejected))
	stored, found = pool.HighestBids.Get(key)
	require.True(t, found)
	require.Same(t, replacement, stored)

	require.True(t, pool.RemoveHighestBid(key, replacement))
	_, found = pool.HighestBids.Get(key)
	require.False(t, found)
}
