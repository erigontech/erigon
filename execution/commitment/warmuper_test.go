// Copyright 2026 The Erigon Authors
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

package commitment

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

// warmupTestCtx extends trieReaderTestCtx with account/storage data and call tracking.
type warmupTestCtx struct {
	trieReaderTestCtx
	accounts map[string]*Update
	storage  map[string]*Update

	accountCalls atomic.Int64
	storageCalls atomic.Int64
}

func newWarmupTestCtx() *warmupTestCtx {
	return &warmupTestCtx{
		trieReaderTestCtx: trieReaderTestCtx{branches: make(map[string][]byte)},
		accounts:          make(map[string]*Update),
		storage:           make(map[string]*Update),
	}
}

func (tc *warmupTestCtx) Account(plainKey []byte) (*Update, error) {
	tc.accountCalls.Add(1)
	u, ok := tc.accounts[string(plainKey)]
	if !ok {
		return nil, nil
	}
	return u, nil
}

func (tc *warmupTestCtx) Storage(plainKey []byte) (*Update, error) {
	tc.storageCalls.Add(1)
	u, ok := tc.storage[string(plainKey)]
	if !ok {
		return nil, nil
	}
	return u, nil
}

// TestWarmuper_BespokeWalk verifies that the bespoke depth-walk warmuper
// traverses the trie and populates the shared CachingPatriciaContext with branch
// and account data via extractBranchCellAddresses.
func TestWarmuper_BespokeWalk(t *testing.T) {
	t.Parallel()

	ctx := newWarmupTestCtx()

	// Build a single-level trie: root branch at nibble hashedKey[0] -> account leaf.
	addr := bytes.Repeat([]byte{0xAB}, 20)
	hashedKey := KeyToHexNibbleHash(addr)

	ctx.accounts[string(addr)] = &Update{Flags: BalanceUpdate}

	var rootCells [16]*cell
	rootCells[hashedKey[0]] = makeAccountCell(addr, dummyHash())
	ctx.putBranch(nil, rootCells)

	cfg := WarmupConfig{
		Enabled:    true,
		CtxFactory: func() (PatriciaContext, func()) { return ctx, nil },
		NumWorkers: 1,

		LogPrefix: "test",
	}

	warmuper := NewWarmuper(context.Background(), cfg)
	warmuper.Start()

	warmuper.WarmKey(hashedKey, 0)

	err := warmuper.Wait()
	require.NoError(t, err)

	// Verify the cache was populated with branch data.
	cache := warmuper.SharedCache()
	require.NotNil(t, cache)

	view := cache.Wrap(&noopPatriciaContext{})
	// Branch for root prefix should be cached.
	branchData, _, err := view.Branch([]byte{0x00}) // compact encoding of empty prefix
	require.NoError(t, err)
	require.NotEmpty(t, branchData, "root branch should be cached")

	// Account should have been fetched by the visitor.
	require.GreaterOrEqual(t, ctx.accountCalls.Load(), int64(1),
		"visitor should have called Account()")

	// Verify that the keysProcessed counter was incremented.
	stats := warmuper.Stats()
	require.Equal(t, uint64(1), stats.KeysProcessed)
}

// TestWarmuper_BespokeWalk_StorageKey verifies that warmup also prefetches storage.
func TestWarmuper_BespokeWalk_StorageKey(t *testing.T) {
	t.Parallel()

	ctx := newWarmupTestCtx()

	// Build a trie with a storage leaf.
	storageAddr := bytes.Repeat([]byte{0xCD}, 52) // 20-byte account + 32-byte storage
	hashedKey := KeyToHexNibbleHash(storageAddr)

	ctx.storage[string(storageAddr)] = &Update{Flags: StorageUpdate, StorageLen: 3}
	copy(ctx.storage[string(storageAddr)].Storage[:], "val")

	var rootCells [16]*cell
	rootCells[hashedKey[0]] = makeStorageCell(storageAddr, dummyHash())
	ctx.putBranch(nil, rootCells)

	cfg := WarmupConfig{
		Enabled:    true,
		CtxFactory: func() (PatriciaContext, func()) { return ctx, nil },
		NumWorkers: 1,

		LogPrefix: "test",
	}

	warmuper := NewWarmuper(context.Background(), cfg)
	warmuper.Start()

	warmuper.WarmKey(hashedKey, 0)

	err := warmuper.Wait()
	require.NoError(t, err)

	// Verify storage was fetched by the visitor.
	require.GreaterOrEqual(t, ctx.storageCalls.Load(), int64(1),
		"visitor should have called Storage()")
}

// TestWarmuper_ConcurrentWorkers_RaceDetector runs 16 concurrent warmup workers
// against a shared CachingPatriciaContext to verify there are no data races.
func TestWarmuper_ConcurrentWorkers_RaceDetector(t *testing.T) {
	t.Parallel()

	ctx := newWarmupTestCtx()

	// Build a trie with several account leaves at different nibbles.
	// All accounts share a single root branch so lookups go through the same node.
	const numAccounts = 8
	hashedKeys := make([][]byte, numAccounts)
	var rootCells [16]*cell

	for i := range numAccounts {
		addr := make([]byte, 20)
		for j := range addr {
			addr[j] = byte(i*17 + j)
		}
		hashedKey := KeyToHexNibbleHash(addr)
		hashedKeys[i] = hashedKey
		ctx.accounts[string(addr)] = &Update{Flags: BalanceUpdate}
		rootCells[hashedKey[0]] = makeAccountCell(addr, dummyHash())
	}
	ctx.putBranch(nil, rootCells)

	cfg := WarmupConfig{
		Enabled: true,
		CtxFactory: func() (PatriciaContext, func()) {
			return ctx, nil
		},
		NumWorkers: 16,

		LogPrefix: "test-concurrent",
	}

	warmuper := NewWarmuper(context.Background(), cfg)
	warmuper.Start()

	// Submit all keys.
	for _, hk := range hashedKeys {
		warmuper.WarmKey(hk, 0)
	}

	err := warmuper.Wait()
	require.NoError(t, err)

	stats := warmuper.Stats()
	require.Equal(t, uint64(numAccounts), stats.KeysProcessed)
}

// TestWarmuper_EmptyTrie verifies that warmup on an empty trie completes without error.
func TestWarmuper_EmptyTrie(t *testing.T) {
	t.Parallel()

	ctx := newWarmupTestCtx()

	cfg := WarmupConfig{
		Enabled:    true,
		CtxFactory: func() (PatriciaContext, func()) { return ctx, nil },
		NumWorkers: 2,

		LogPrefix: "test",
	}

	warmuper := NewWarmuper(context.Background(), cfg)
	warmuper.Start()

	hashedKey := make([]byte, 64)
	hashedKey[0] = 0x5
	warmuper.WarmKey(hashedKey, 0)

	err := warmuper.Wait()
	require.NoError(t, err)
	require.Equal(t, uint64(1), warmuper.Stats().KeysProcessed)
}

// TestWarmuper_SharedCachePopulated verifies that the shared cache contains entries
// populated by warmup workers, accessible through a fresh view.
func TestWarmuper_SharedCachePopulated(t *testing.T) {
	t.Parallel()

	mock := newWarmupTestCtx()

	// Build a trie with an account leaf.
	addr := bytes.Repeat([]byte{0x42}, 20)
	hashedKey := KeyToHexNibbleHash(addr)

	mock.accounts[string(addr)] = &Update{Flags: BalanceUpdate}

	var rootCells [16]*cell
	rootCells[hashedKey[0]] = makeAccountCell(addr, dummyHash())
	mock.putBranch(nil, rootCells)

	cfg := WarmupConfig{
		Enabled:    true,
		CtxFactory: func() (PatriciaContext, func()) { return mock, nil },
		NumWorkers: 1,

		LogPrefix: "test",
	}

	warmuper := NewWarmuper(context.Background(), cfg)
	warmuper.Start()
	warmuper.WarmKey(hashedKey, 0)
	require.NoError(t, warmuper.Wait())

	// Use a separate noop context to prove reads come from cache, not DB.
	cache := warmuper.SharedCache()
	noop := &noopPatriciaContext{}
	view := cache.Wrap(noop)

	// The account was cached by the visitor — reading it from the cached view
	// with a noop underlying context should still return the account data.
	acc, err := view.Account(addr)
	require.NoError(t, err)
	require.NotNil(t, acc, "account should be in cache from warmup visitor")
	require.Equal(t, BalanceUpdate, acc.Flags)
}

// TestWarmuper_DepthBound verifies that MaxDepth limits how deep the walk goes.
func TestWarmuper_DepthBound(t *testing.T) {
	t.Parallel()

	ctx := newWarmupTestCtx()

	addr := bytes.Repeat([]byte{0x11}, 20)
	hashedKey := KeyToHexNibbleHash(addr)
	ctx.accounts[string(addr)] = &Update{Flags: BalanceUpdate}

	// Build a root branch and a second-level branch so the walk could
	// go at least 2 levels deep.
	var rootCells [16]*cell
	rootCells[hashedKey[0]] = makeBranchCell(dummyHash())
	ctx.putBranch(nil, rootCells)

	var level1Cells [16]*cell
	level1Cells[hashedKey[1]] = makeAccountCell(addr, dummyHash())
	ctx.putBranch(hashedKey[:1], level1Cells)

	cfg := WarmupConfig{
		Enabled:    true,
		CtxFactory: func() (PatriciaContext, func()) { return ctx, nil },
		NumWorkers: 1,
		MaxDepth:   1, // limit to depth 1, so level-1 branch should not be visited
		LogPrefix:  "test",
	}

	warmuper := NewWarmuper(context.Background(), cfg)
	warmuper.Start()
	warmuper.WarmKey(hashedKey, 0)
	require.NoError(t, warmuper.Wait())

	// With MaxDepth=1 the walk reads root branch (depth=0) but stops before
	// depth=1, so the account at level 1 should NOT have been fetched.
	require.Equal(t, int64(0), ctx.accountCalls.Load(),
		"MaxDepth=1 should prevent walk from reaching level-1 account")
}

// TestWarmuper_BitmapMissingChild verifies early termination when the bitmap
// does not contain the child nibble.
func TestWarmuper_BitmapMissingChild(t *testing.T) {
	t.Parallel()

	ctx := newWarmupTestCtx()

	addr := bytes.Repeat([]byte{0x22}, 20)
	hashedKey := KeyToHexNibbleHash(addr)
	ctx.accounts[string(addr)] = &Update{Flags: BalanceUpdate}

	// Build a root branch with a cell at a DIFFERENT nibble than hashedKey[0].
	var rootCells [16]*cell
	differentNibble := (hashedKey[0] + 1) % 16
	rootCells[differentNibble] = makeAccountCell(addr, dummyHash())
	ctx.putBranch(nil, rootCells)

	cfg := WarmupConfig{
		Enabled:    true,
		CtxFactory: func() (PatriciaContext, func()) { return ctx, nil },
		NumWorkers: 1,
		LogPrefix:  "test",
	}

	warmuper := NewWarmuper(context.Background(), cfg)
	warmuper.Start()
	warmuper.WarmKey(hashedKey, 0)
	require.NoError(t, warmuper.Wait())

	// The walk should have terminated at the root because the child nibble
	// is not in the bitmap. The account address IS extracted by
	// extractBranchCellAddresses (sibling without stateHash), so Account()
	// may be called, but the walk should NOT descend further.
	require.Equal(t, uint64(1), warmuper.Stats().KeysProcessed)
}

// errorPatriciaContext returns an error from Branch to test error handling.
type errorPatriciaContext struct {
	noopPatriciaContext
	branchErr error
}

func (e *errorPatriciaContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	return nil, 0, e.branchErr
}

// TestWarmuper_BranchError verifies that a Branch() error doesn't crash
// the worker and the key is still counted.
func TestWarmuper_BranchError(t *testing.T) {
	t.Parallel()

	errCtx := &errorPatriciaContext{
		branchErr: fmt.Errorf("simulated branch error"),
	}

	cfg := WarmupConfig{
		Enabled:    true,
		CtxFactory: func() (PatriciaContext, func()) { return errCtx, nil },
		NumWorkers: 1,
		LogPrefix:  "test",
	}

	warmuper := NewWarmuper(context.Background(), cfg)
	warmuper.Start()

	hashedKey := make([]byte, 64)
	hashedKey[0] = 0x5
	warmuper.WarmKey(hashedKey, 0)

	err := warmuper.Wait()
	require.NoError(t, err)
	require.Equal(t, uint64(1), warmuper.Stats().KeysProcessed)
}

// TestWarmuper_StatsPopulatedAfterCycle verifies that both WarmupStats and
// CacheStats are populated after a complete warmup cycle (start, warm keys, wait).
func TestWarmuper_StatsPopulatedAfterCycle(t *testing.T) {
	t.Parallel()

	ctx := newWarmupTestCtx()

	// Build a trie with several account leaves.
	const numKeys = 4
	hashedKeys := make([][]byte, numKeys)
	var rootCells [16]*cell

	for i := range numKeys {
		addr := make([]byte, 20)
		for j := range addr {
			addr[j] = byte(i*31 + j)
		}
		hashedKey := KeyToHexNibbleHash(addr)
		hashedKeys[i] = hashedKey
		ctx.accounts[string(addr)] = &Update{Flags: BalanceUpdate}
		rootCells[hashedKey[0]] = makeAccountCell(addr, dummyHash())
	}
	ctx.putBranch(nil, rootCells)

	cfg := WarmupConfig{
		Enabled:    true,
		CtxFactory: func() (PatriciaContext, func()) { return ctx, nil },
		NumWorkers: 2,
		LogPrefix:  "test-stats",
	}

	warmuper := NewWarmuper(context.Background(), cfg)
	warmuper.Start()

	for _, hk := range hashedKeys {
		warmuper.WarmKey(hk, 0)
	}

	require.NoError(t, warmuper.Wait())

	// Verify WarmupStats.
	wStats := warmuper.Stats()
	require.Equal(t, uint64(numKeys), wStats.KeysProcessed,
		"all submitted keys should be processed")
	require.Greater(t, wStats.Duration, time.Duration(0),
		"duration should be positive after a cycle")

	// Verify CacheStats — at minimum, branch misses should be non-zero since
	// every key triggers at least one Branch() call that populates the cache.
	cache := warmuper.SharedCache()
	cStats := cache.Stats()
	require.Greater(t, cStats.BranchHits+cStats.BranchMisses, uint64(0),
		"branch lookups should have occurred")
	require.Greater(t, cStats.HitRate()+0.01, 0.0,
		"hit rate should be computable (non-NaN)")
}

// noopPatriciaContext is defined in commitment_test.go
