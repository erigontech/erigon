package commitment

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

// mockPatriciaContext implements PatriciaContext for testing.
type mockPatriciaContext struct {
	branches map[string][]byte
}

func (m *mockPatriciaContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	// Return stub branch data (>=4 bytes) so prefetchBranches doesn't stop early.
	// Use the map when present, otherwise return a generic stub.
	if data, ok := m.branches[string(prefix)]; ok {
		return data, 0, nil
	}
	return []byte("stub-branch-data"), 0, nil
}

func (m *mockPatriciaContext) Account(plainKey []byte) (*Update, error) { return nil, nil }
func (m *mockPatriciaContext) Storage(plainKey []byte) (*Update, error) { return nil, nil }
func (m *mockPatriciaContext) PutBranch(prefix []byte, data []byte, prevData []byte) error {
	return nil
}
func (m *mockPatriciaContext) TxNum() uint64        { return 0 }
func (m *mockPatriciaContext) Variant() TrieVariant { return VariantHexPatriciaTrie }

func TestBranchPrefetcher_StartStop(t *testing.T) {
	cache := NewBranchCache(1024)
	factory := func() (PatriciaContext, func()) {
		return &mockPatriciaContext{branches: map[string][]byte{}}, nil
	}

	ctx := context.Background()
	p := NewBranchPrefetcher(ctx, cache, factory, 2, 4)
	p.Start()
	p.Stop()
	// Double-stop must be a no-op.
	p.Stop()
}

func TestBranchPrefetcher_PopulatesCache(t *testing.T) {
	rootKey := HexNibblesToCompactBytes(nil)         // depth-0 compact key
	depth1Key := HexNibblesToCompactBytes([]byte{3}) // nibble 3

	branchData := []byte("some-branch-data-long-enough")

	factory := func() (PatriciaContext, func()) {
		ctx := &mockPatriciaContext{
			branches: map[string][]byte{
				string(rootKey):   branchData,
				string(depth1Key): branchData,
			},
		}
		return ctx, nil
	}

	cache := NewBranchCache(1024)
	ctx := context.Background()
	p := NewBranchPrefetcher(ctx, cache, factory, 2, 4)
	p.Start()

	// Submit a plain key — prefetcher will hash it and walk ancestor compact keys.
	// Use an all-zero 32-byte key whose first nibble is 0 → depth-1 compact key [0x10].
	plainKey := make([]byte, 32)
	p.SubmitPlainKey(plainKey)

	// Give workers time to process.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.Prefetched() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	p.Stop()
	require.Positive(t, p.Prefetched(), "expected at least one branch prefetched")
}

func TestBranchPrefetcher_ContextCancellation(t *testing.T) {
	factory := func() (PatriciaContext, func()) {
		return &mockPatriciaContext{branches: map[string][]byte{}}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cache := NewBranchCache(1024)
	p := NewBranchPrefetcher(ctx, cache, factory, 2, 4)
	p.Start()

	// Cancel parent context — workers should exit without an explicit Stop().
	cancel()

	// Stop should still be safe to call after context cancellation.
	p.Stop()
}

func TestBranchPrefetcher_SubmitAfterStop(t *testing.T) {
	factory := func() (PatriciaContext, func()) {
		return &mockPatriciaContext{branches: map[string][]byte{}}, nil
	}

	cache := NewBranchCache(1024)
	p := NewBranchPrefetcher(context.Background(), cache, factory, 1, 4)
	p.Start()
	p.Stop()

	// Submit after Stop must not panic.
	p.Submit(make([]byte, 8))
	p.SubmitPlainKey(make([]byte, 32))
}
