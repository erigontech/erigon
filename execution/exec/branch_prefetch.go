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
	"hash/maphash"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
)

const (
	BranchPrefetchTxs      = 8
	branchPrefetchBatch    = 8
	branchPrefetchBacklog  = 1 << 16
	branchPrefetchShards   = 64
	branchPrefetchMaxBytes = 1 << 30
)

type PathTouch struct {
	Account, Slot [32]byte
	Storage       bool
}

func ComparePathTouch(a, b PathTouch) int {
	if c := bytes.Compare(a.Account[:], b.Account[:]); c != 0 {
		return c
	}
	if a.Storage != b.Storage {
		if a.Storage {
			return 1
		}
		return -1
	}
	return bytes.Compare(a.Slot[:], b.Slot[:])
}

type prefetchedRecord struct {
	data []byte
	step kv.Step
	refs *commitment.LeafRefs
}

func (r prefetchedRecord) size(key string) int64 {
	n := len(key) + len(r.data)
	if r.refs != nil {
		n += 32 * len(r.refs.Refs)
	}
	return int64(n)
}

type prefetchedShard struct {
	mu      sync.RWMutex
	records map[string]prefetchedRecord
}

type BranchPrefetchStats struct {
	Bytes                int64
	Queued, Dropped      int
	FoldHits, FoldMisses uint64
}

type BranchPrefetcher struct {
	seed   maphash.Seed
	shards [branchPrefetchShards]prefetchedShard
	bytes  atomic.Int64

	foldHits, foldMisses atomic.Uint64

	workers errgroup.Group
	mu      sync.Mutex
	txs     []kv.TemporalTx
	backlog []PathTouch
	head    int
	running int
	closed  bool

	queued, dropped int
}

func NewBranchPrefetcher(ctx context.Context, db kv.RoDB) *BranchPrefetcher {
	p := &BranchPrefetcher{seed: maphash.MakeSeed()}
	for i := range p.shards {
		p.shards[i].records = make(map[string]prefetchedRecord)
	}
	if db == nil {
		return p
	}
	ctx = kv.WithNonBlockingAcquire(ctx)
	for range BranchPrefetchTxs {
		tx, err := db.BeginRo(ctx) //nolint:gocritic
		if err != nil {
			break
		}
		ttx, ok := tx.(kv.TemporalTx)
		if !ok {
			tx.Rollback()
			break
		}
		p.txs = append(p.txs, ttx)
	}
	return p
}

func (p *BranchPrefetcher) Add(items ...PathTouch) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || len(p.txs)+p.running == 0 {
		return
	}
	if p.head*2 >= len(p.backlog) {
		p.backlog, p.head = p.backlog[:copy(p.backlog, p.backlog[p.head:])], 0
	}
	if room := branchPrefetchBacklog - len(p.backlog); len(items) > room {
		p.dropped += len(items) - room
		items = items[:room]
	}
	p.backlog = append(p.backlog, items...)
	p.queued += len(items)
	p.spawn()
}

func (p *BranchPrefetcher) spawn() {
	for p.running*branchPrefetchBatch < len(p.backlog)-p.head && len(p.txs) > 0 {
		tx := p.txs[len(p.txs)-1]
		p.txs = p.txs[:len(p.txs)-1]
		p.running++
		p.workers.Go(func() error {
			p.work(tx)
			return nil
		})
	}
}

func (p *BranchPrefetcher) work(tx kv.TemporalTx) {
	read := func(key []byte) []byte {
		if data, _, ok := p.Get(key); ok {
			return data
		}
		data, step, err := tx.GetLatest(kv.CommitmentDomain, key, kv.GetLatestOptions{})
		if err != nil {
			return nil
		}
		return p.Put(key, data, step)
	}
	var batch [branchPrefetchBatch]PathTouch
	for n := p.take(tx, batch[:]); n > 0; n = p.take(tx, batch[:]) {
		for i := range batch[:n] {
			var slot []byte
			if batch[i].Storage {
				slot = batch[i].Slot[:]
			}
			v3.PrefetchPath(read, batch[i].Account[:], slot)
		}
	}
}

func (p *BranchPrefetcher) take(tx kv.TemporalTx, batch []PathTouch) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := copy(batch, p.backlog[p.head:])
	if p.head += n; p.head == len(p.backlog) {
		p.backlog, p.head = p.backlog[:0], 0
	}
	if n == 0 {
		p.running--
		if p.closed {
			tx.Rollback()
		} else {
			p.txs = append(p.txs, tx)
		}
	}
	return n
}

func (p *BranchPrefetcher) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		p.backlog, p.head = nil, 0
		for _, tx := range p.txs {
			tx.Rollback()
		}
		p.txs = nil
	}
	p.mu.Unlock()
	_ = p.workers.Wait()
}

func (p *BranchPrefetcher) Stats() BranchPrefetchStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return BranchPrefetchStats{
		Bytes: p.bytes.Load(), Queued: p.queued, Dropped: p.dropped,
		FoldHits: p.foldHits.Load(), FoldMisses: p.foldMisses.Load(),
	}
}

func (p *BranchPrefetcher) shard(key []byte) *prefetchedShard {
	return &p.shards[maphash.Bytes(p.seed, key)%branchPrefetchShards]
}

func (p *BranchPrefetcher) lookup(key []byte) (prefetchedRecord, bool) {
	s := p.shard(key)
	s.mu.RLock()
	r, ok := s.records[string(key)]
	s.mu.RUnlock()
	return r, ok
}

func (p *BranchPrefetcher) Get(key []byte) ([]byte, kv.Step, bool) {
	r, ok := p.lookup(key)
	return r.data, r.step, ok
}

func (p *BranchPrefetcher) FoldGet(key []byte) ([]byte, kv.Step, bool) {
	data, step, ok := p.Get(key)
	if ok {
		p.foldHits.Add(1)
	} else {
		p.foldMisses.Add(1)
	}
	return data, step, ok
}

func (p *BranchPrefetcher) LeafRefs(key, data []byte) *commitment.LeafRefs {
	r, ok := p.lookup(key)
	if !ok || len(data) == 0 || len(r.data) != len(data) || &r.data[0] != &data[0] {
		return nil
	}
	return r.refs
}

func (p *BranchPrefetcher) Put(key, data []byte, step kv.Step) []byte {
	if p.bytes.Load() >= branchPrefetchMaxBytes {
		return data
	}
	data = bytes.Clone(data)
	refs, _ := v3.ComputeLeafRefs(key, data)
	s := p.shard(key)
	s.mu.Lock()
	if old, ok := s.records[string(key)]; ok {
		s.mu.Unlock()
		return old.data
	}
	r := prefetchedRecord{data: data, step: step, refs: refs}
	s.records[string(key)] = r
	p.bytes.Add(r.size(string(key)))
	s.mu.Unlock()
	return data
}
