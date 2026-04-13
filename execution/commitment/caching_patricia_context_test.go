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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/erigontech/erigon/db/kv"
)

// mockPatriciaContext is a simple in-memory PatriciaContext for testing.
type mockPatriciaContext struct {
	branches map[string]mockBranch
	accounts map[string]*Update
	storage  map[string]*Update

	branchCalls  atomic.Int64
	accountCalls atomic.Int64
	storageCalls atomic.Int64
}

type mockBranch struct {
	data []byte
	step kv.Step
}

func newMockPatriciaContext() *mockPatriciaContext {
	return &mockPatriciaContext{
		branches: make(map[string]mockBranch),
		accounts: make(map[string]*Update),
		storage:  make(map[string]*Update),
	}
}

func (m *mockPatriciaContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	m.branchCalls.Add(1)
	b, ok := m.branches[string(prefix)]
	if !ok {
		return nil, 0, nil
	}
	return b.data, b.step, nil
}

func (m *mockPatriciaContext) PutBranch(prefix []byte, data []byte, prevData []byte) error {
	m.branches[string(prefix)] = mockBranch{data: data, step: 0}
	return nil
}

func (m *mockPatriciaContext) Account(plainKey []byte) (*Update, error) {
	m.accountCalls.Add(1)
	u, ok := m.accounts[string(plainKey)]
	if !ok {
		return nil, nil
	}
	return u, nil
}

func (m *mockPatriciaContext) Storage(plainKey []byte) (*Update, error) {
	m.storageCalls.Add(1)
	u, ok := m.storage[string(plainKey)]
	if !ok {
		return nil, nil
	}
	return u, nil
}

// TestCachingPatriciaContext_MissPopulatesCache verifies that a cache miss
// reads from the underlying context and subsequent reads are cache hits.
func TestCachingPatriciaContext_MissPopulatesCache(t *testing.T) {
	mock := newMockPatriciaContext()
	mock.branches["pfx1"] = mockBranch{data: []byte("branchdata"), step: 42}
	mock.accounts["acc1"] = &Update{Flags: BalanceUpdate}
	mock.storage["sto1"] = &Update{Flags: StorageUpdate, StorageLen: 3}
	copy(mock.storage["sto1"].Storage[:], "abc")

	cache := NewCachingPatriciaContext()
	view := cache.Wrap(mock)

	// Branch: first call is a miss, second is a hit.
	data, step, err := view.Branch([]byte("pfx1"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, []byte("branchdata")) || step != 42 {
		t.Fatalf("unexpected branch: data=%x step=%d", data, step)
	}
	if mock.branchCalls.Load() != 1 {
		t.Fatalf("expected 1 underlying branch call, got %d", mock.branchCalls.Load())
	}

	// Second read should be a cache hit.
	data2, step2, err := view.Branch([]byte("pfx1"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data2, []byte("branchdata")) || step2 != 42 {
		t.Fatalf("unexpected cached branch: data=%x step=%d", data2, step2)
	}
	if mock.branchCalls.Load() != 1 {
		t.Fatalf("expected still 1 underlying branch call, got %d", mock.branchCalls.Load())
	}

	// Account: miss then hit.
	acc, err := view.Account([]byte("acc1"))
	if err != nil {
		t.Fatal(err)
	}
	if acc == nil || acc.Flags != BalanceUpdate {
		t.Fatalf("unexpected account: %+v", acc)
	}
	if mock.accountCalls.Load() != 1 {
		t.Fatalf("expected 1 underlying account call, got %d", mock.accountCalls.Load())
	}

	acc2, err := view.Account([]byte("acc1"))
	if err != nil {
		t.Fatal(err)
	}
	if acc2 == nil || acc2.Flags != BalanceUpdate {
		t.Fatalf("unexpected cached account: %+v", acc2)
	}
	if mock.accountCalls.Load() != 1 {
		t.Fatalf("expected still 1 underlying account call, got %d", mock.accountCalls.Load())
	}

	// Storage: miss then hit.
	sto, err := view.Storage([]byte("sto1"))
	if err != nil {
		t.Fatal(err)
	}
	if sto == nil || sto.Flags != StorageUpdate {
		t.Fatalf("unexpected storage: %+v", sto)
	}
	if mock.storageCalls.Load() != 1 {
		t.Fatalf("expected 1 underlying storage call, got %d", mock.storageCalls.Load())
	}

	sto2, err := view.Storage([]byte("sto1"))
	if err != nil {
		t.Fatal(err)
	}
	if sto2 == nil || sto2.Flags != StorageUpdate {
		t.Fatalf("unexpected cached storage: %+v", sto2)
	}
	if mock.storageCalls.Load() != 1 {
		t.Fatalf("expected still 1 underlying storage call, got %d", mock.storageCalls.Load())
	}
}

// TestCachingPatriciaContext_PutBranchInvalidates verifies that PutBranch
// invalidates the cached branch entry so the next read fetches fresh data.
func TestCachingPatriciaContext_PutBranchInvalidates(t *testing.T) {
	mock := newMockPatriciaContext()
	mock.branches["pfx"] = mockBranch{data: []byte("old"), step: 1}

	cache := NewCachingPatriciaContext()
	view := cache.Wrap(mock)

	// Populate cache.
	data, _, err := view.Branch([]byte("pfx"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, []byte("old")) {
		t.Fatalf("expected old data, got %x", data)
	}
	if mock.branchCalls.Load() != 1 {
		t.Fatal("expected 1 call")
	}

	// PutBranch updates cache with new data.
	if err := view.PutBranch([]byte("pfx"), []byte("new"), []byte("old")); err != nil {
		t.Fatal(err)
	}

	// Next read should be a cache hit returning the new data.
	data2, _, err := view.Branch([]byte("pfx"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data2, []byte("new")) {
		t.Fatalf("expected new data after PutBranch, got %x", data2)
	}
	// Only 1 underlying Branch call (the initial miss) — PutBranch updated
	// the cache so the second Branch was a hit.
	if mock.branchCalls.Load() != 1 {
		t.Fatalf("expected 1 underlying call (cache hit after PutBranch), got %d", mock.branchCalls.Load())
	}
}

// TestCachingPatriciaContext_Reset verifies Reset clears all entries.
func TestCachingPatriciaContext_Reset(t *testing.T) {
	mock := newMockPatriciaContext()
	mock.branches["pfx"] = mockBranch{data: []byte("b"), step: 1}
	mock.accounts["acc"] = &Update{Flags: BalanceUpdate}
	mock.storage["sto"] = &Update{Flags: StorageUpdate}

	cache := NewCachingPatriciaContext()
	view := cache.Wrap(mock)

	// Populate cache.
	view.Branch([]byte("pfx"))
	view.Account([]byte("acc"))
	view.Storage([]byte("sto"))

	if mock.branchCalls.Load() != 1 || mock.accountCalls.Load() != 1 || mock.storageCalls.Load() != 1 {
		t.Fatal("expected 1 call each to populate cache")
	}

	// Reset the cache.
	cache.Reset()

	// All reads should now miss and go to the underlying context again.
	view.Branch([]byte("pfx"))
	view.Account([]byte("acc"))
	view.Storage([]byte("sto"))

	if mock.branchCalls.Load() != 2 {
		t.Fatalf("expected 2 branch calls after reset, got %d", mock.branchCalls.Load())
	}
	if mock.accountCalls.Load() != 2 {
		t.Fatalf("expected 2 account calls after reset, got %d", mock.accountCalls.Load())
	}
	if mock.storageCalls.Load() != 2 {
		t.Fatalf("expected 2 storage calls after reset, got %d", mock.storageCalls.Load())
	}
}

// TestCachingPatriciaContext_ConcurrentReads verifies no data races when
// multiple goroutines read/write through the shared cache concurrently.
func TestCachingPatriciaContext_ConcurrentReads(t *testing.T) {
	mock := newMockPatriciaContext()
	const numKeys = 100
	for i := range numKeys {
		key := fmt.Sprintf("key-%03d", i)
		mock.branches[key] = mockBranch{data: []byte(key), step: kv.Step(i)}
		mock.accounts[key] = &Update{Flags: BalanceUpdate}
		mock.storage[key] = &Update{Flags: StorageUpdate}
	}

	cache := NewCachingPatriciaContext()
	const numWorkers = 16
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := range numWorkers {
		go func(workerID int) {
			defer wg.Done()
			// Each worker wraps the same mock (safe because mock reads are
			// from an immutable map populated before goroutines start, and
			// the atomic counters are safe for concurrent increment).
			view := cache.Wrap(mock)
			for i := range numKeys {
				key := fmt.Appendf(nil, "key-%03d", (i+workerID)%numKeys)
				if _, _, err := view.Branch(key); err != nil {
					t.Errorf("worker %d Branch error: %v", workerID, err)
				}
				if _, err := view.Account(key); err != nil {
					t.Errorf("worker %d Account error: %v", workerID, err)
				}
				if _, err := view.Storage(key); err != nil {
					t.Errorf("worker %d Storage error: %v", workerID, err)
				}
			}
		}(w)
	}
	wg.Wait()

	// Verify the underlying was called at most numKeys times per type
	// (once per unique key, even though 16 workers accessed them).
	// Due to races between cache check and set, it may be slightly more,
	// but should be far less than numKeys*numWorkers.
	if mock.branchCalls.Load() > int64(numKeys*2) {
		t.Errorf("too many underlying branch calls: %d (expected <= %d)", mock.branchCalls.Load(), numKeys*2)
	}
	if mock.accountCalls.Load() > int64(numKeys*2) {
		t.Errorf("too many underlying account calls: %d (expected <= %d)", mock.accountCalls.Load(), numKeys*2)
	}
	if mock.storageCalls.Load() > int64(numKeys*2) {
		t.Errorf("too many underlying storage calls: %d (expected <= %d)", mock.storageCalls.Load(), numKeys*2)
	}
}

// TestCachingPatriciaContext_NilUpdate verifies that nil Update results are cached.
func TestCachingPatriciaContext_NilUpdate(t *testing.T) {
	mock := newMockPatriciaContext()
	// "missing" key not in mock -> Account/Storage return nil

	cache := NewCachingPatriciaContext()
	view := cache.Wrap(mock)

	acc, err := view.Account([]byte("missing"))
	if err != nil {
		t.Fatal(err)
	}
	if acc != nil {
		t.Fatalf("expected nil account, got %+v", acc)
	}
	if mock.accountCalls.Load() != 1 {
		t.Fatalf("expected 1 call, got %d", mock.accountCalls.Load())
	}

	// Second call should still hit cache (nil is a valid cached value).
	acc2, err := view.Account([]byte("missing"))
	if err != nil {
		t.Fatal(err)
	}
	if acc2 != nil {
		t.Fatalf("expected nil cached account, got %+v", acc2)
	}
	if mock.accountCalls.Load() != 1 {
		t.Fatalf("expected still 1 call after cache hit, got %d", mock.accountCalls.Load())
	}
}

// TestCachingPatriciaContext_MultipleViews verifies that multiple views
// sharing the same cache see each other's writes.
func TestCachingPatriciaContext_MultipleViews(t *testing.T) {
	mock := newMockPatriciaContext()
	mock.accounts["shared"] = &Update{Flags: BalanceUpdate}

	cache := NewCachingPatriciaContext()
	view1 := cache.Wrap(mock)
	view2 := cache.Wrap(mock)

	// view1 populates the cache.
	acc1, err := view1.Account([]byte("shared"))
	if err != nil {
		t.Fatal(err)
	}
	if acc1 == nil || acc1.Flags != BalanceUpdate {
		t.Fatalf("unexpected: %+v", acc1)
	}
	if mock.accountCalls.Load() != 1 {
		t.Fatal("expected 1 call")
	}

	// view2 should see the cached entry.
	acc2, err := view2.Account([]byte("shared"))
	if err != nil {
		t.Fatal(err)
	}
	if acc2 == nil || acc2.Flags != BalanceUpdate {
		t.Fatalf("unexpected: %+v", acc2)
	}
	if mock.accountCalls.Load() != 1 {
		t.Fatalf("expected still 1 call, got %d", mock.accountCalls.Load())
	}
}
