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

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
)

type fakeTx struct {
	kv.TemporalTx
	db        *fakeDB
	mu        sync.Mutex
	rollbacks int
}

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
	kv.TemporalRoDB
	mu     sync.Mutex
	begins int
	limit  int
	txs    []*fakeTx
	reads  [][]byte
	onRead func(key []byte)
}

func newFakeDB(limit int) *fakeDB { return &fakeDB{limit: limit} }

func (d *fakeDB) BeginTemporalRo(context.Context) (kv.TemporalTx, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.begins++
	if d.limit >= 0 && d.begins > d.limit {
		return nil, errors.New("no more read transactions")
	}
	tx := &fakeTx{db: d}
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

func TestBranchPrefetcherOpensItsReadTxsOnce(t *testing.T) {
	db := newFakeDB(-1)
	p := NewBranchPrefetcher(t.Context(), db)
	require.Equal(t, BranchPrefetchTxs, db.beginCount())
	touches := make([]PathTouch, 200)
	for i := range touches {
		touches[i] = storageTouch(i)
	}
	p.Add(touches...)
	require.Eventually(t, func() bool { return len(db.readKeys()) == len(touches) }, 10*time.Second, time.Millisecond)
	require.Equal(t, BranchPrefetchTxs, db.beginCount())
	got := db.readKeys()
	require.Len(t, got, len(touches))
	for _, touch := range touches {
		require.Contains(t, got, firstKey(touch))
	}
	p.Close()
	counts := db.rollbackCounts()
	require.Len(t, counts, BranchPrefetchTxs)
	for i, c := range counts {
		require.Equal(t, 1, c, "tx %d", i)
	}
}

func TestBranchPrefetcherWithoutReadTxsQueuesNothing(t *testing.T) {
	for name, db := range map[string]kv.TemporalRoDB{"nil db": nil, "failing BeginTemporalRo": newFakeDB(0)} {
		p := NewBranchPrefetcher(t.Context(), db)
		p.Add(storageTouch(1))
		require.Zero(t, p.Stats().Queued, name)
		p.Close()
	}
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

func TestBranchPrefetcherKeepsTheFirstRecordOfAKey(t *testing.T) {
	p := NewBranchPrefetcher(t.Context(), nil)
	key := []byte{0x42, 0x00}
	first := p.Put(key, []byte{1, 2, 3}, 4)
	size := p.bytes.Load()
	second := p.Put(key, []byte{1, 2, 3}, 4)
	require.Same(t, &first[0], &second[0], "a second read of a key must not replace the record a fold may already hold")
	got, _, ok := p.Get(key)
	require.True(t, ok)
	require.Same(t, &first[0], &got[0])
	require.Equal(t, size, p.bytes.Load())
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
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.closed
	}, 10*time.Second, time.Millisecond)
	close(release)
	<-closed
	require.Len(t, db.readKeys(), 1, "a closed prefetcher reads nothing past the batch a worker holds")
	require.Equal(t, []int{1}, db.rollbackCounts())
}

func TestBranchPrefetcherBoundsItsBacklog(t *testing.T) {
	db := newFakeDB(1)
	p := NewBranchPrefetcher(t.Context(), db)
	defer p.Close()
	items := make([]PathTouch, branchPrefetchBacklog+5)
	p.Add(items...)
	st := p.Stats()
	require.Equal(t, branchPrefetchBacklog, st.Queued)
	require.Equal(t, 5, st.Dropped)
}

func TestBranchPrefetcherLeafRefsOfAnEmptyRecord(t *testing.T) {
	p := NewBranchPrefetcher(t.Context(), nil)
	p.Put([]byte{1}, nil, 0)
	data, _, ok := p.Get([]byte{1})
	require.True(t, ok)
	require.Nil(t, p.LeafRefs([]byte{1}, data))
}
