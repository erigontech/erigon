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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
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

// TestWarmuper_TrieReaderWarmup verifies that the new TrieReader-based warmuper
// traverses the trie and populates the shared CachingPatriciaContext with branch,
// account, and storage data from the visitor callback.
func TestWarmuper_TrieReaderWarmup(t *testing.T) {
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
		Enabled:       true,
		CtxFactory:    func() (PatriciaContext, func()) { return ctx, nil },
		NumWorkers:    1,
		AccountKeyLen: length.Addr,
		LogPrefix:     "test",
	}

	warmuper := NewWarmuper(context.Background(), cfg)
	warmuper.Start()

	warmuper.WarmKey(hashedKey)

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

// TestWarmuper_TrieReaderWarmup_StorageKey verifies that warmup also prefetches storage.
func TestWarmuper_TrieReaderWarmup_StorageKey(t *testing.T) {
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
		Enabled:       true,
		CtxFactory:    func() (PatriciaContext, func()) { return ctx, nil },
		NumWorkers:    1,
		AccountKeyLen: length.Addr,
		LogPrefix:     "test",
	}

	warmuper := NewWarmuper(context.Background(), cfg)
	warmuper.Start()

	warmuper.WarmKey(hashedKey)

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
		NumWorkers:    16,
		AccountKeyLen: length.Addr,
		LogPrefix:     "test-concurrent",
	}

	warmuper := NewWarmuper(context.Background(), cfg)
	warmuper.Start()

	// Submit all keys.
	for _, hk := range hashedKeys {
		warmuper.WarmKey(hk)
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
		Enabled:       true,
		CtxFactory:    func() (PatriciaContext, func()) { return ctx, nil },
		NumWorkers:    2,
		AccountKeyLen: length.Addr,
		LogPrefix:     "test",
	}

	warmuper := NewWarmuper(context.Background(), cfg)
	warmuper.Start()

	hashedKey := make([]byte, 64)
	hashedKey[0] = 0x5
	warmuper.WarmKey(hashedKey)

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
		Enabled:       true,
		CtxFactory:    func() (PatriciaContext, func()) { return mock, nil },
		NumWorkers:    1,
		AccountKeyLen: length.Addr,
		LogPrefix:     "test",
	}

	warmuper := NewWarmuper(context.Background(), cfg)
	warmuper.Start()
	warmuper.WarmKey(hashedKey)
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

// noopPatriciaContext is defined in commitment_test.go
