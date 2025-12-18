package commitmentdb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

// mockPatriciaContext is a simple mock for testing warmup
type mockPatriciaContext struct {
	mu       sync.Mutex
	branches map[string][]byte
	calls    atomic.Int32
}

func newMockPatriciaContext() *mockPatriciaContext {
	return &mockPatriciaContext{
		branches: make(map[string][]byte),
	}
}

func (m *mockPatriciaContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	m.calls.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if data, ok := m.branches[string(prefix)]; ok {
		return data, 1, nil
	}
	return nil, 0, nil
}

func (m *mockPatriciaContext) PutBranch(prefix []byte, data []byte, prevData []byte, prevStep kv.Step) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.branches[string(prefix)] = data
	return nil
}

func (m *mockPatriciaContext) Account(plainKey []byte) (*commitment.Update, error) {
	return nil, nil
}

func (m *mockPatriciaContext) Storage(plainKey []byte) (*commitment.Update, error) {
	return nil, nil
}

func TestWarmupBranches(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Create prefixes to warm up
	prefixes := [][]byte{
		{0x00},
		{0x01},
		{0x00, 0x01},
		{0x00, 0x02},
	}

	// Track total calls across all workers
	var totalCalls atomic.Int32

	ctxFactory := func() (commitment.PatriciaContext, func()) {
		mockCtx := newMockPatriciaContext()
		// Each mock returns different data based on prefix
		mockCtx.branches[string([]byte{0x00})] = []byte{0xaa}
		mockCtx.branches[string([]byte{0x01})] = []byte{0xbb}
		mockCtx.branches[string([]byte{0x00, 0x01})] = []byte{0xcc}
		mockCtx.branches[string([]byte{0x00, 0x02})] = []byte{0xdd}

		cleanup := func() {
			totalCalls.Add(mockCtx.calls.Load())
		}
		return mockCtx, cleanup
	}

	err := WarmupBranches(ctx, prefixes, 2, ctxFactory)
	require.NoError(t, err)

	// Verify all prefixes were read (warming the cache)
	require.Equal(t, int32(4), totalCalls.Load())
}

func TestWarmupBranches_EmptyPrefixes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	var factoryCalled bool

	ctxFactory := func() (commitment.PatriciaContext, func()) {
		factoryCalled = true
		return newMockPatriciaContext(), nil
	}

	err := WarmupBranches(ctx, nil, 2, ctxFactory)
	require.NoError(t, err)
	require.False(t, factoryCalled, "factory should not be called for empty prefixes")

	err = WarmupBranches(ctx, [][]byte{}, 2, ctxFactory)
	require.NoError(t, err)
	require.False(t, factoryCalled, "factory should not be called for empty prefixes")
}

func TestCollectBranchPrefixesFromKeys(t *testing.T) {
	t.Parallel()

	hashedKeys := [][]byte{
		{0x0, 0x1, 0x2, 0x3},
		{0x0, 0x1, 0x4, 0x5},
		{0x0, 0x2, 0x0, 0x0},
	}

	prefixes := CollectBranchPrefixesFromKeys(hashedKeys, 2)
	require.NotEmpty(t, prefixes)

	// Should have:
	// - depth 0: empty prefix (1)
	// - depth 1: 0x0 (shared by all) (1)
	// - depth 2: 0x01, 0x02 (2)
	// Total unique: 4 compact prefixes
}
