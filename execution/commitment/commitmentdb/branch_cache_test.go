package commitmentdb

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

// mockPatriciaContext is a simple mock for testing the cache
type mockPatriciaContext struct {
	mu       sync.Mutex
	branches map[string][]byte
	calls    int
}

func newMockPatriciaContext() *mockPatriciaContext {
	return &mockPatriciaContext{
		branches: make(map[string][]byte),
	}
}

func (m *mockPatriciaContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
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

func (m *mockPatriciaContext) getCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func TestBranchCache_BasicOperations(t *testing.T) {
	t.Parallel()

	cache := NewBranchCache()

	// Test Put and Get
	prefix := []byte{0x01, 0x02}
	data := []byte{0xaa, 0xbb, 0xcc}
	cache.Put(prefix, data, 5, nil)

	gotData, gotStep, gotErr, found := cache.Get(prefix)
	require.True(t, found)
	require.Equal(t, data, gotData)
	require.Equal(t, kv.Step(5), gotStep)
	require.NoError(t, gotErr)

	// Test missing key
	_, _, _, found = cache.Get([]byte{0xff})
	require.False(t, found)

	// Test Size
	require.Equal(t, 1, cache.Size())

	// Test Clear
	cache.Clear()
	require.Equal(t, 0, cache.Size())
}

func TestCachedTrieContext(t *testing.T) {
	t.Parallel()

	mockCtx := newMockPatriciaContext()
	// Add some branch data to the mock
	mockCtx.branches[string([]byte{0x00})] = []byte{0x11, 0x22}
	mockCtx.branches[string([]byte{0x01})] = []byte{0x33, 0x44}

	cache := NewBranchCache()
	// Pre-warm only prefix 0x00
	cache.Put([]byte{0x00}, []byte{0x11, 0x22}, 1, nil)

	cachedCtx := NewCachedTrieContext(mockCtx, cache)

	// This should come from cache (no mock call)
	data, step, err := cachedCtx.Branch([]byte{0x00})
	require.NoError(t, err)
	require.Equal(t, []byte{0x11, 0x22}, data)
	require.Equal(t, kv.Step(1), step)
	require.Equal(t, 0, mockCtx.getCalls()) // No call to underlying context

	// This should hit the mock (not in cache)
	data, step, err = cachedCtx.Branch([]byte{0x01})
	require.NoError(t, err)
	require.Equal(t, []byte{0x33, 0x44}, data)
	require.Equal(t, kv.Step(1), step)
	require.Equal(t, 1, mockCtx.getCalls()) // One call to underlying context

	// Missing key should also hit mock
	data, step, err = cachedCtx.Branch([]byte{0x02})
	require.NoError(t, err)
	require.Nil(t, data)
	require.Equal(t, kv.Step(0), step)
	require.Equal(t, 2, mockCtx.getCalls())
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

	// Track calls per worker
	var callsMu sync.Mutex
	workerCalls := make(map[int]int)
	workerID := 0

	cache := NewBranchCache()

	ctxFactory := func() (commitment.PatriciaContext, func()) {
		callsMu.Lock()
		id := workerID
		workerID++
		callsMu.Unlock()

		mockCtx := newMockPatriciaContext()
		// Each mock returns different data based on prefix
		mockCtx.branches[string([]byte{0x00})] = []byte{0xaa}
		mockCtx.branches[string([]byte{0x01})] = []byte{0xbb}
		mockCtx.branches[string([]byte{0x00, 0x01})] = []byte{0xcc}
		mockCtx.branches[string([]byte{0x00, 0x02})] = []byte{0xdd}

		cleanup := func() {
			callsMu.Lock()
			workerCalls[id] = mockCtx.getCalls()
			callsMu.Unlock()
		}
		return mockCtx, cleanup
	}

	err := WarmupBranches(ctx, prefixes, cache, 2, ctxFactory)
	require.NoError(t, err)

	// Verify cache has all prefixes
	require.Equal(t, 4, cache.Size())

	// Verify data is correct
	data, _, _, found := cache.Get([]byte{0x00})
	require.True(t, found)
	require.Equal(t, []byte{0xaa}, data)

	data, _, _, found = cache.Get([]byte{0x01})
	require.True(t, found)
	require.Equal(t, []byte{0xbb}, data)

	data, _, _, found = cache.Get([]byte{0x00, 0x01})
	require.True(t, found)
	require.Equal(t, []byte{0xcc}, data)

	data, _, _, found = cache.Get([]byte{0x00, 0x02})
	require.True(t, found)
	require.Equal(t, []byte{0xdd}, data)
}

func TestCollectBranchPrefixes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Create updates in ModeUpdate (the only mode that supports ForEachHashedKey)
	updates := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), func(key []byte) []byte {
		// Simple identity hash for testing - returns fixed-length nibbles
		// For account keys (20 bytes), hash to 64 nibbles
		result := make([]byte, 64)
		for i := 0; i < len(result) && i < len(key)*2; i++ {
			if i%2 == 0 {
				result[i] = (key[i/2] >> 4) & 0x0f
			} else {
				result[i] = key[i/2] & 0x0f
			}
		}
		return result
	})
	defer updates.Close()

	// Add some storage keys (simpler than accounts - no deserialization needed)
	// Storage keys need to be address (20 bytes) + storage location (32 bytes) = 52 bytes
	key1 := make([]byte, 52)
	key1[0] = 0x01
	key2 := make([]byte, 52)
	key2[0] = 0x02
	key3 := make([]byte, 52)
	key3[0] = 0x03

	updates.TouchPlainKey(string(key1), []byte{0xaa, 0xbb}, updates.TouchStorage)
	updates.TouchPlainKey(string(key2), []byte{0xcc, 0xdd}, updates.TouchStorage)
	updates.TouchPlainKey(string(key3), []byte{0xee, 0xff}, updates.TouchStorage)

	// Collect prefixes with maxDepth=2
	prefixes, err := CollectBranchPrefixes(ctx, updates, 2)
	require.NoError(t, err)

	// Should have prefixes for each key at depths 0, 1, 2
	// Exact count depends on the hashed key values
	require.NotEmpty(t, prefixes)

	// Verify the empty prefix (depth 0) is included
	hasEmptyPrefix := false
	for _, p := range prefixes {
		if len(p) == 1 && p[0] == 0x00 { // compact empty prefix
			hasEmptyPrefix = true
			break
		}
	}
	require.True(t, hasEmptyPrefix, "should have empty prefix (root)")
}

func TestCollectBranchPrefixes_ModeDirect(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Create updates in ModeDirect - this should return nil for prefixes
	updates := commitment.NewUpdates(commitment.ModeDirect, t.TempDir(), func(key []byte) []byte {
		result := make([]byte, 64)
		for i := 0; i < len(result) && i < len(key)*2; i++ {
			if i%2 == 0 {
				result[i] = (key[i/2] >> 4) & 0x0f
			} else {
				result[i] = key[i/2] & 0x0f
			}
		}
		return result
	})
	defer updates.Close()

	// Add a storage key
	key1 := make([]byte, 52)
	key1[0] = 0x01
	updates.TouchPlainKey(string(key1), []byte{0xaa, 0xbb}, updates.TouchStorage)

	// Should return nil because ModeDirect doesn't support ForEachHashedKey
	prefixes, err := CollectBranchPrefixes(ctx, updates, 2)
	require.NoError(t, err)
	require.Nil(t, prefixes)
}
