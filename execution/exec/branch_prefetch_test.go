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

package exec

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
)

type fakeTx struct {
	kv.TemporalTx
	db        *fakeDB
	view      uint64
	mu        sync.Mutex
	rollbacks int
}

func (tx *fakeTx) ViewID() uint64 { return tx.view }

func (tx *fakeTx) GetLatest(_ kv.Domain, key []byte, _ kv.GetLatestOptions) ([]byte, kv.Step, error) {
	if tx.db.onRead != nil {
		tx.db.onRead(key)
	}
	tx.db.mu.Lock()
	tx.db.reads = append(tx.db.reads, bytes.Clone(key))
	tx.db.mu.Unlock()
	return nil, 0, nil
}

func (tx *fakeTx) Rollback() {
	tx.mu.Lock()
	tx.rollbacks++
	tx.mu.Unlock()
}

func (tx *fakeTx) rolledBack() int {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.rollbacks
}

type fakeDB struct {
	kv.RoDB
	mu     sync.Mutex
	begins int
	limit  int
	view   uint64
	odd    map[int]uint64
	txs    []*fakeTx
	reads  [][]byte
	onRead func(key []byte)
}

func newFakeDB(limit int) *fakeDB { return &fakeDB{limit: limit, view: 5} }

func (d *fakeDB) BeginRo(context.Context) (kv.Tx, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.begins++
	if d.limit >= 0 && d.begins > d.limit {
		return nil, errors.New("no more read transactions")
	}
	view := d.view
	if v, ok := d.odd[d.begins]; ok {
		view = v
	}
	tx := &fakeTx{db: d, view: view}
	d.txs = append(d.txs, tx)
	return tx, nil
}

func (d *fakeDB) beginCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.begins
}

func (d *fakeDB) readKeys() [][]byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.reads)
}

func (d *fakeDB) rollbackCounts() []int {
	d.mu.Lock()
	defer d.mu.Unlock()
	counts := make([]int, len(d.txs))
	for i, tx := range d.txs {
		counts[i] = tx.rolledBack()
	}
	return counts
}

func storageTouch(n int) PathTouch {
	return PathTouch{Account: [32]byte{byte(n >> 8), byte(n)}, Slot: [32]byte{1}, Storage: true}
}

func firstKey(t PathTouch) []byte { return v3.StorageNodeKey(t.Account, nil, nil) }

func resetPublishedPrefetcher(t *testing.T) {
	t.Cleanup(func() {
		closePublishedBranchPrefetcher()
		TakeBranchPrefetcher(0, common.Hash{}, 0)
	})
}

func returnsWithin(d time.Duration, f func()) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

func TestBranchPrefetcherOpensItsReadTxsOnce(t *testing.T) {
	db := newFakeDB(-1)
	p := NewBranchPrefetcher(t.Context(), db)
	require.Equal(t, branchPrefetchTxs, db.beginCount())
	touches := make([]PathTouch, 200)
	for i := range touches {
		touches[i] = storageTouch(i)
	}
	p.Add(touches...)
	p.Drain()
	require.Equal(t, branchPrefetchTxs, db.beginCount())
	got := db.readKeys()
	require.Len(t, got, len(touches))
	for _, touch := range touches {
		require.Contains(t, got, firstKey(touch))
	}
	p.Close()
	counts := db.rollbackCounts()
	require.Len(t, counts, branchPrefetchTxs)
	for i, c := range counts {
		require.Equal(t, 1, c, "tx %d", i)
	}
}

func TestBranchPrefetcherDrainWalksSortedUniqueBacklog(t *testing.T) {
	db := newFakeDB(1)
	p := NewBranchPrefetcher(t.Context(), db)
	defer p.Close()
	tx := p.borrow()
	require.NotNil(t, tx)
	touches := []PathTouch{storageTouch(9), storageTouch(3), storageTouch(7), storageTouch(3), storageTouch(1)}
	p.Add(touches...)
	require.Empty(t, db.readKeys())
	p.giveBack(tx)
	p.Drain()

	unique := slices.CompactFunc(slices.SortedFunc(slices.Values(touches), ComparePathTouch), func(a, b PathTouch) bool { return a == b })
	want := make([][]byte, len(unique))
	for i, touch := range unique {
		want[i] = firstKey(touch)
	}
	require.Equal(t, want, db.readKeys())
	require.Equal(t, len(unique), p.Stats().Backlog)
}

func TestBranchPrefetcherDrainWaitsForRunningWorker(t *testing.T) {
	db := newFakeDB(-1)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	db.onRead = func([]byte) {
		once.Do(func() { close(entered) })
		<-release
	}
	p := NewBranchPrefetcher(t.Context(), db)
	defer p.Close()
	p.Add(storageTouch(1))
	<-entered

	drained := make(chan struct{})
	go func() {
		p.Drain()
		close(drained)
	}()
	select {
	case <-drained:
		t.Fatal("Drain returned while a worker was still reading")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("Drain did not return after the worker finished")
	}
}

func TestBranchPrefetcherCloseDropsBacklog(t *testing.T) {
	db := newFakeDB(1)
	p := NewBranchPrefetcher(t.Context(), db)
	tx := p.borrow()
	require.NotNil(t, tx)
	p.Add(storageTouch(1), storageTouch(2))
	p.Close()
	p.giveBack(tx)
	require.Empty(t, db.readKeys())
	require.Equal(t, []int{1}, db.rollbackCounts())
}

func TestBranchPrefetcherWithoutReadTxsQueuesNothing(t *testing.T) {
	for name, db := range map[string]kv.RoDB{"nil db": nil, "failing BeginRo": newFakeDB(0)} {
		p := NewBranchPrefetcher(t.Context(), db)
		require.True(t, returnsWithin(time.Second, func() {
			p.Add(storageTouch(1))
			p.Drain()
		}), name)
		require.Zero(t, p.Stats().Queued, name)
		p.Close()
	}
}

func TestBranchPrefetcherDoesNotPoolTxOfAnotherView(t *testing.T) {
	db := newFakeDB(-1)
	db.odd = map[int]uint64{3: 99}
	p := NewBranchPrefetcher(t.Context(), db)
	require.Equal(t, 2, p.pool)
	require.Equal(t, uint64(5), p.ViewID)
	require.Equal(t, []int{0, 0, 1}, db.rollbackCounts())
	p.Close()
	require.Equal(t, []int{1, 1, 1}, db.rollbackCounts())
}

func TestBranchPrefetcherStopsStoringAtByteCap(t *testing.T) {
	p := NewBranchPrefetcher(t.Context(), nil)
	p.Put([]byte("kept"), []byte("value"), 0)
	p.bytes.Store(branchPrefetchMaxBytes)
	p.Put([]byte("overflow"), []byte("dropped"), 0)
	_, _, ok := p.Get([]byte("overflow"))
	require.False(t, ok)
	data, _, ok := p.Get([]byte("kept"))
	require.True(t, ok)
	require.Equal(t, []byte("value"), data)
}

func TestBranchPrefetcherLeafRefsOnlyForTheStoredRecord(t *testing.T) {
	p := NewBranchPrefetcher(t.Context(), nil)
	key, data := []byte("key"), []byte("data")
	refs := &commitment.LeafRefs{}
	p.shard(key).records[string(key)] = prefetchedRecord{data: data, refs: refs}
	require.Same(t, refs, p.LeafRefs(key, data))
	require.Nil(t, p.LeafRefs(key, bytes.Clone(data)))
}

func TestBranchPrefetcherFoldGetCountsHitsAndMisses(t *testing.T) {
	p := NewBranchPrefetcher(t.Context(), nil)
	_, _, ok := p.FoldGet([]byte("a"))
	require.False(t, ok)
	p.Put([]byte("a"), []byte("v"), 3)
	data, step, ok := p.FoldGet([]byte("a"))
	require.True(t, ok)
	require.Equal(t, []byte("v"), data)
	require.Equal(t, kv.Step(3), step)
	_, _, _ = p.FoldGet([]byte("b"))
	stats := p.Stats()
	require.Equal(t, uint64(1), stats.FoldHits)
	require.Equal(t, uint64(2), stats.FoldMisses)
}

func publishedFake(t *testing.T, num uint64, parent common.Hash) (*BranchPrefetcher, *fakeDB) {
	db := newFakeDB(-1)
	p := NewBranchPrefetcher(t.Context(), db)
	p.BlockNum, p.ParentHash = num, parent
	t.Cleanup(p.Close)
	return p, db
}

func TestBranchPrefetcherTakeHandsOverOnce(t *testing.T) {
	resetPublishedPrefetcher(t)
	p, _ := publishedFake(t, 7, common.Hash{1})
	require.True(t, publishBranchPrefetcher(p))
	require.Same(t, p, TakeBranchPrefetcher(7, common.Hash{1}, p.ViewID))
	require.Nil(t, TakeBranchPrefetcher(7, common.Hash{1}, p.ViewID))
}

func TestBranchPrefetcherTakeRefusesAnotherView(t *testing.T) {
	resetPublishedPrefetcher(t)
	p, db := publishedFake(t, 7, common.Hash{1})
	require.True(t, publishBranchPrefetcher(p))
	require.Nil(t, TakeBranchPrefetcher(7, common.Hash{1}, p.ViewID+1))
	for i, c := range db.rollbackCounts() {
		require.Equal(t, 1, c, "tx %d", i)
	}
}

func TestBranchPrefetcherPublishAfterClaimIsRefused(t *testing.T) {
	resetPublishedPrefetcher(t)
	require.Nil(t, TakeBranchPrefetcher(9, common.Hash{2}, 0))
	p, _ := publishedFake(t, 9, common.Hash{2})
	require.False(t, publishBranchPrefetcher(p))
}

func TestBranchPrefetcherPublishClosesUnclaimedPrevious(t *testing.T) {
	resetPublishedPrefetcher(t)
	first, firstDB := publishedFake(t, 7, common.Hash{1})
	second, _ := publishedFake(t, 8, common.Hash{1})
	require.True(t, publishBranchPrefetcher(first))
	require.True(t, publishBranchPrefetcher(second))
	for i, c := range firstDB.rollbackCounts() {
		require.Equal(t, 1, c, "tx %d", i)
	}
}

func TestBranchPrefetcherCloseStopsARunningWorkerAtItsBatch(t *testing.T) {
	db := newFakeDB(1)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	db.onRead = func([]byte) {
		once.Do(func() { close(entered) })
		<-release
	}
	p := NewBranchPrefetcher(t.Context(), db)
	p.Add(storageTouch(1))
	<-entered
	for i := range 10 * branchPrefetchBatch {
		p.Add(storageTouch(i + 2))
	}
	closed := make(chan struct{})
	go func() {
		p.Close()
		close(closed)
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)
	<-closed
	require.Len(t, db.readKeys(), 1, "a closed prefetcher reads nothing past the batch a worker holds")
	require.Equal(t, []int{1}, db.rollbackCounts())
}
