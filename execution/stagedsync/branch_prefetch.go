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

package stagedsync

import (
	"bytes"
	"context"
	"hash/maphash"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
)

var prefetchDrain = dbg.EnvBool("V3_PREFETCH_DRAIN", false)

const (
	branchPrefetchWorkers  = 8
	branchPrefetchBurst    = 8
	branchPrefetchDrainMax = 100 * time.Millisecond
	branchPrefetchQueue    = 1 << 16
	branchPrefetchPerTx    = 256
	branchPrefetchShards   = 64
	branchPrefetchMaxBytes = 1 << 30
)

type prefetchItem struct {
	account [32]byte
	slot    [32]byte
	storage bool
}

func comparePrefetchItems(a, b prefetchItem) int {
	return commitmentdb.ComparePathTouch(commitmentdb.PathTouch{Account: a.account, Slot: a.slot, Storage: a.storage}, commitmentdb.PathTouch{Account: b.account, Slot: b.slot, Storage: b.storage})
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

type branchPrefetcher struct {
	work   chan prefetchItem
	wg     sync.WaitGroup
	gate   sync.RWMutex
	seed   maphash.Seed
	shards [branchPrefetchShards]prefetchedShard
	bytes  atomic.Int64

	pausing          atomic.Bool
	dropped, drained atomic.Uint64

	ctx        context.Context
	db         kv.TemporalRoDB
	blockNum   uint64
	parentHash common.Hash
	stop       chan struct{}
	stopOnce   sync.Once
	feed       sync.WaitGroup

	pending                            atomic.Int64
	idle                               chan struct{}
	seenMu                             sync.Mutex
	seen                               map[prefetchItem]struct{}
	queued, deduped, followed          atomic.Uint64
	foldHits, foldMisses, drainTimeout atomic.Uint64
	backlog, drainWait                 atomic.Int64
}

func (p *branchPrefetcher) done(n int) {
	if p.pending.Add(-int64(n)) == 0 {
		select {
		case p.idle <- struct{}{}:
		default:
		}
	}
}

func (p *branchPrefetcher) foldGet(key []byte) ([]byte, kv.Step, bool) {
	data, step, ok := p.get(key)
	if ok {
		p.foldHits.Add(1)
	} else {
		p.foldMisses.Add(1)
	}
	return data, step, ok
}

func newBranchPrefetcher(ctx context.Context, db kv.TemporalRoDB) *branchPrefetcher {
	p := newPrefetchRecords()
	p.work = make(chan prefetchItem, branchPrefetchQueue)
	p.ctx, p.db = kv.WithNonBlockingAcquire(ctx), db
	for range branchPrefetchWorkers {
		p.wg.Go(p.run)
	}
	return p
}

func newPrefetchRecords() *branchPrefetcher {
	p := &branchPrefetcher{seed: maphash.MakeSeed(), ctx: context.Background(), stop: make(chan struct{}), idle: make(chan struct{}, 1)}
	for i := range p.shards {
		p.shards[i].records = make(map[string]prefetchedRecord)
	}
	return p
}

func (p *branchPrefetcher) add(it prefetchItem) {
	p.seenMu.Lock()
	_, dup := p.seen[it]
	if p.seen == nil {
		p.seen = make(map[prefetchItem]struct{})
	}
	p.seen[it] = struct{}{}
	p.seenMu.Unlock()
	if dup {
		p.deduped.Add(1)
		return
	}
	p.queued.Add(1)
	p.pending.Add(1)
	p.send(it)
}

func (p *branchPrefetcher) send(it prefetchItem) {
	select {
	case p.work <- it:
	default:
		p.dropped.Add(1)
		p.done(1)
	}
}

func (p *branchPrefetcher) pause() {
	if p == nil {
		return
	}
	p.pausing.Store(true)
	p.gate.Lock()
	p.drained.Add(uint64(p.drain()))
	p.seenMu.Lock()
	clear(p.seen)
	p.seenMu.Unlock()
}

func (p *branchPrefetcher) drain() (n int) {
	for {
		select {
		case <-p.work:
			n++
		default:
			p.done(n)
			return n
		}
	}
}

func (p *branchPrefetcher) resume() {
	if p != nil {
		p.pausing.Store(false)
		p.gate.Unlock()
	}
}

func (p *branchPrefetcher) close() {
	p.stopFollow()
	p.drain()
	close(p.work)
	p.wg.Wait()
}

func (p *branchPrefetcher) shard(key []byte) *prefetchedShard {
	return &p.shards[maphash.Bytes(p.seed, key)%branchPrefetchShards]
}

func (p *branchPrefetcher) lookup(key []byte) (prefetchedRecord, bool) {
	s := p.shard(key)
	s.mu.RLock()
	r, ok := s.records[string(key)]
	s.mu.RUnlock()
	return r, ok
}

func (p *branchPrefetcher) get(key []byte) ([]byte, kv.Step, bool) {
	r, ok := p.lookup(key)
	return r.data, r.step, ok
}

func (p *branchPrefetcher) leafRefs(key, data []byte) *commitment.LeafRefs {
	r, ok := p.lookup(key)
	if !ok || len(r.data) != len(data) || &r.data[0] != &data[0] {
		return nil
	}
	return r.refs
}

func (p *branchPrefetcher) put(key, data []byte, step kv.Step) []byte {
	if p.bytes.Load() >= branchPrefetchMaxBytes {
		return data
	}
	data = bytes.Clone(data)
	refs, _ := v3.ComputeLeafRefs(key, data)
	s := p.shard(key)
	s.mu.Lock()
	if old, ok := s.records[string(key)]; ok {
		p.bytes.Add(-old.size(string(key)))
	}
	r := prefetchedRecord{data: data, step: step, refs: refs}
	s.records[string(key)] = r
	p.bytes.Add(r.size(string(key)))
	s.mu.Unlock()
	return data
}

func (p *branchPrefetcher) run() {
	for it := range p.work {
		p.chunk(it)
	}
}

func (p *branchPrefetcher) chunk(it prefetchItem) {
	n := 1
	defer func() { p.done(n) }()
	p.gate.RLock()
	defer p.gate.RUnlock()
	tx, err := p.db.BeginTemporalRo(p.ctx) //nolint:gocritic
	if err != nil {
		p.dropped.Add(1)
		return
	}
	defer tx.Rollback()
	n = p.walkChunk(it, func(key []byte) []byte {
		if data, _, ok := p.get(key); ok {
			return data
		}
		data, step, err := tx.GetLatest(kv.CommitmentDomain, key, kv.GetLatestOptions{})
		if err != nil {
			return nil
		}
		return p.put(key, data, step)
	})
}

func (p *branchPrefetcher) follow() {
	p.feed.Go(func() {
		forward := func(batch []commitmentdb.PathTouch) {
			p.followed.Add(uint64(len(batch)))
			for _, it := range batch {
				p.add(prefetchItem{account: it.Account, slot: it.Slot, storage: it.Storage})
			}
		}
		republished := time.NewTicker(time.Millisecond)
		defer republished.Stop()
		for {
			var batches chan []commitmentdb.PathTouch
			if t := commitmentdb.PreExecTouchesFor(p.blockNum, p.parentHash); t != nil {
				batches = t.C
			}
			select {
			case batch := <-batches:
				forward(batch)
			case <-republished.C:
			case <-p.stop:
				for {
					select {
					case batch := <-batches:
						forward(batch)
					default:
						return
					}
				}
			}
		}
	})
}

func (p *branchPrefetcher) preExecLost() (droppedPaths int64, unreadBatches int) {
	if t := commitmentdb.PreExecTouchesFor(p.blockNum, p.parentHash); t != nil {
		return t.Dropped.Load(), len(t.C)
	}
	return 0, 0
}

func (p *branchPrefetcher) stopFollow() {
	p.stopOnce.Do(func() { close(p.stop) })
	p.feed.Wait()
}

func (p *branchPrefetcher) drainSorted() {
	start := time.Now()
	defer func() { p.drainWait.Store(int64(time.Since(start))) }()
	p.backlog.Store(int64(len(p.work)))
	backlog := make([]prefetchItem, 0, len(p.work))
collect:
	for len(backlog) < cap(backlog) {
		select {
		case it, ok := <-p.work:
			if !ok {
				break collect
			}
			backlog = append(backlog, it)
		default:
			break collect
		}
	}
	slices.SortFunc(backlog, comparePrefetchItems)
	for _, it := range backlog {
		p.send(it)
	}
	for range branchPrefetchBurst {
		p.wg.Go(func() {
			for {
				select {
				case it, ok := <-p.work:
					if !ok {
						return
					}
					p.chunk(it)
				default:
					return
				}
			}
		})
	}
	limit := time.NewTimer(branchPrefetchDrainMax)
	defer limit.Stop()
	for p.pending.Load() != 0 {
		select {
		case <-p.idle:
		case <-limit.C:
			p.drainTimeout.Add(1)
			return
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *branchPrefetcher) walkChunk(it prefetchItem, read func(key []byte) []byte) (n int) {
	it.touch(read)
	for n = 1; n < branchPrefetchPerTx && !p.pausing.Load(); n++ {
		select {
		case next, ok := <-p.work:
			if !ok {
				return n
			}
			next.touch(read)
		default:
			return n
		}
	}
	return n
}

func (it *prefetchItem) touch(read func(key []byte) []byte) {
	var slot []byte
	if it.storage {
		slot = it.slot[:]
	}
	v3.PrefetchPath(read, it.account[:], slot)
}
