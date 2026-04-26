package receipts

import (
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// newTestGenerator builds a minimal Generator with only the two receipt caches
// populated. The other fields (blockReader, engine, …) are left nil because
// TryGetCachedReceipt never touches them.
func newTestGenerator(t *testing.T) *Generator {
	t.Helper()
	rc, err := lru.New[uint64, *types.Receipt](256)
	require.NoError(t, err)
	rsc, err := lru.New[common.Hash, types.Receipts](256)
	require.NoError(t, err)
	return &Generator{receiptCache: rc, receiptsCache: rsc}
}

func TestTryGetCachedReceipt(t *testing.T) {
	t.Parallel()

	blockHash := common.HexToHash("0xaaaa")
	otherHash := common.HexToHash("0xbbbb")
	const txNum = uint64(42)
	const txIndex = 1

	t.Run("hit receiptCache matching blockHash", func(t *testing.T) {
		t.Parallel()
		g := newTestGenerator(t)
		want := &types.Receipt{BlockHash: blockHash}
		g.receiptCache.Add(txNum, want)

		got, ok := g.TryGetCachedReceipt(blockHash, txNum, txIndex)
		require.True(t, ok)
		require.Same(t, want, got)
	})

	t.Run("stale blockHash in receiptCache not returned", func(t *testing.T) {
		t.Parallel()
		g := newTestGenerator(t)
		// txNum is cached but for a different block (e.g. after a reorg).
		g.receiptCache.Add(txNum, &types.Receipt{BlockHash: otherHash})

		got, ok := g.TryGetCachedReceipt(blockHash, txNum, txIndex)
		require.False(t, ok)
		require.Nil(t, got)
	})

	t.Run("postState mismatch in receiptCache not returned", func(t *testing.T) {
		t.Parallel()
		g := newTestGenerator(t)
		// receipt has postState set (pre-Byzantium); TryGetCachedReceipt must skip it
		// because callers pass calculatePostState=false and would get wrong data.
		g.receiptCache.Add(txNum, &types.Receipt{BlockHash: blockHash, PostState: []byte{0x01}})

		got, ok := g.TryGetCachedReceipt(blockHash, txNum, txIndex)
		require.False(t, ok)
		require.Nil(t, got)
	})

	t.Run("fallback to receiptsCache by txIndex", func(t *testing.T) {
		t.Parallel()
		g := newTestGenerator(t)
		// receiptCache empty; receiptsCache has the block receipts.
		want := &types.Receipt{BlockHash: blockHash}
		g.receiptsCache.Add(blockHash, types.Receipts{
			{BlockHash: blockHash}, // txIndex=0
			want,                   // txIndex=1
		})

		got, ok := g.TryGetCachedReceipt(blockHash, txNum, txIndex)
		require.True(t, ok)
		require.Same(t, want, got)
	})

	t.Run("txIndex -1 skips receiptsCache", func(t *testing.T) {
		t.Parallel()
		g := newTestGenerator(t)
		g.receiptsCache.Add(blockHash, types.Receipts{{BlockHash: blockHash}})

		got, ok := g.TryGetCachedReceipt(blockHash, txNum, -1)
		require.False(t, ok)
		require.Nil(t, got)
	})

	t.Run("txIndex out of bounds in receiptsCache", func(t *testing.T) {
		t.Parallel()
		g := newTestGenerator(t)
		g.receiptsCache.Add(blockHash, types.Receipts{{BlockHash: blockHash}}) // only 1 entry

		got, ok := g.TryGetCachedReceipt(blockHash, txNum, 5) // txIndex=5 > len
		require.False(t, ok)
		require.Nil(t, got)
	})
}
