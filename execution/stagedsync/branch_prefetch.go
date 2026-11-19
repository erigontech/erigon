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

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
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

var prefetchSeqSteps = dbg.EnvInt("V3_PREFETCH_SEQ_STEPS", 0)

type prefetchItem struct {
	account [32]byte
	slot    [32]byte
	storage bool
}

func newPrefetchItem(account, slot []byte) prefetchItem {
	it := prefetchItem{account: [32]byte(account)}
	if slot != nil {
		it.slot, it.storage = [32]byte(slot), true
	}
	return it
}

func comparePrefetchItems(a, b prefetchItem) int {
	if c := bytes.Compare(a.account[:], b.account[:]); c != 0 {
		return c
	}
	if a.storage != b.storage {
		if a.storage {
			return 1
		}
		return -1
	}
	return bytes.Compare(a.slot[:], b.slot[:])
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

	pausing            atomic.Bool
	dropped, drained   atomic.Uint64
	seqNexts, seqSeeks atomic.Uint64
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

func (p *branchPrefetcher) pause() {
	if p == nil {
		return
	}
	p.pausing.Store(true)
	p.gate.Lock()
	p.drained.Add(uint64(p.drain()))
}

func (p *branchPrefetcher) drain() (n int) {
	for {
		select {
		case <-p.work:
			n++
		default:
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

func (p *branchPrefetcher) run(ctx context.Context, db kv.TemporalRoDB) {
	for it := range p.work {
		p.gate.RLock()
		tx, err := db.BeginTemporalRo(ctx) //nolint:gocritic
		if err != nil {
			p.gate.RUnlock()
			continue
		}
		var seq *dbstate.AggregatorRoTx
		if prefetchSeqSteps > 0 {
			if seq, _ = tx.AggTx().(*dbstate.AggregatorRoTx); seq != nil {
				seq.SetSeqLookup(kv.CommitmentDomain, prefetchSeqSteps)
			}
		}
		read := func(key []byte) []byte {
			if data, _, ok := p.get(key); ok {
				return data
			}
			data, step, err := tx.GetLatest(kv.CommitmentDomain, key, kv.GetLatestOptions{})
			if err != nil {
				return nil
			}
			return p.put(key, data, step)
		}
		p.walkChunk(it, read)
		if seq != nil {
			nexts, seeks := seq.SeqLookupStats(kv.CommitmentDomain)
			p.seqNexts.Add(nexts)
			p.seqSeeks.Add(seeks)
		}
		tx.Rollback()
		p.gate.RUnlock()
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
