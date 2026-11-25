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
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
)

const (
	branchPrefetchWorkers  = 8
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
	seed   maphash.Seed
	shards [branchPrefetchShards]prefetchedShard
	bytes  atomic.Int64

	dropped atomic.Uint64
}

func newBranchPrefetcher(ctx context.Context, db kv.TemporalRoDB) *branchPrefetcher {
	p := newPrefetchRecords()
	p.work = make(chan prefetchItem, branchPrefetchQueue)
	ctx = kv.WithNonBlockingAcquire(ctx)
	for range branchPrefetchWorkers {
		p.wg.Go(func() { p.run(ctx, db) })
	}
	return p
}

func newPrefetchRecords() *branchPrefetcher {
	p := &branchPrefetcher{seed: maphash.MakeSeed()}
	for i := range p.shards {
		p.shards[i].records = make(map[string]prefetchedRecord)
	}
	return p
}

func (p *branchPrefetcher) add(it prefetchItem) {
	select {
	case p.work <- it:
	default:
		p.dropped.Add(1)
	}
}

func (p *branchPrefetcher) close() {
	for {
		select {
		case <-p.work:
		default:
			close(p.work)
			p.wg.Wait()
			return
		}
	}
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
		s.mu.Unlock()
		return old.data
	}
	r := prefetchedRecord{data: data, step: step, refs: refs}
	s.records[string(key)] = r
	p.bytes.Add(r.size(string(key)))
	s.mu.Unlock()
	return data
}

type latestPrefetcher interface {
	PrefetchLatest(ctx context.Context, domain kv.Domain, keys [][]byte) (coldPages int, err error)
}

func (p *branchPrefetcher) run(ctx context.Context, db kv.TemporalRoDB) {
	for it := range p.work {
		tx, err := db.BeginTemporalRo(ctx) //nolint:gocritic
		if err != nil {
			continue
		}
		batch := func([][]byte) {}
		if b, ok := tx.AggTx().(latestPrefetcher); ok {
			batch = func(keys [][]byte) { _, _ = b.PrefetchLatest(ctx, kv.CommitmentDomain, keys) } // reads stay correct without it
		}
		read := func(key []byte) []byte {
			data, step, err := tx.GetLatest(kv.CommitmentDomain, key, kv.GetLatestOptions{})
			if err != nil {
				return nil
			}
			return p.put(key, data, step)
		}
		p.walkChunk(p.chunk(it), batch, read)
		tx.Rollback()
	}
}

func (p *branchPrefetcher) chunk(it prefetchItem) []prefetchItem {
	items := []prefetchItem{it}
	for len(items) < branchPrefetchPerTx {
		select {
		case next, ok := <-p.work:
			if !ok {
				return items
			}
			items = append(items, next)
		default:
			return items
		}
	}
	return items
}

// walkChunk walks the paths of items one trie level at a time: batch gets the node keys of a level before read is
// called on them, so a batched read keeps one read per path in flight instead of one per worker.
func (p *branchPrefetcher) walkChunk(items []prefetchItem, batch func(keys [][]byte), read func(key []byte) []byte) {
	done := map[string][]byte{}
	for len(items) > 0 {
		var level [][]byte
		wanted := map[string]bool{}
		live := items[:0]
		for _, it := range items {
			missed := false
			it.touch(func(key []byte) []byte {
				if data, _, ok := p.get(key); ok {
					return data
				}
				if data, ok := done[string(key)]; ok {
					return data
				}
				missed = true
				if !wanted[string(key)] {
					wanted[string(key)] = true
					level = append(level, bytes.Clone(key))
				}
				return nil
			})
			if missed {
				live = append(live, it)
			}
		}
		if len(level) == 0 {
			return
		}
		batch(level)
		for _, key := range level {
			done[string(key)] = read(key)
		}
		items = live
	}
}

func (it *prefetchItem) touch(read func(key []byte) []byte) {
	var slot []byte
	if it.storage {
		slot = it.slot[:]
	}
	v3.PrefetchPath(read, it.account[:], slot)
}
