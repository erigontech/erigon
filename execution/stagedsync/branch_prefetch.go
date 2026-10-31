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

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/execution/types"
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
	gate   sync.RWMutex
	seed   maphash.Seed
	shards [branchPrefetchShards]prefetchedShard
	bytes  atomic.Int64

	pausing                        atomic.Bool
	hits, misses, dropped, drained atomic.Uint64
}

func newBranchPrefetcher(ctx context.Context, db kv.TemporalRoDB) *branchPrefetcher {
	return newPrefetchRecords().start(ctx, db)
}

func newPrefetchRecords() *branchPrefetcher {
	p := &branchPrefetcher{seed: maphash.MakeSeed()}
	for i := range p.shards {
		p.shards[i].records = make(map[string]prefetchedRecord)
	}
	return p
}

func (p *branchPrefetcher) start(ctx context.Context, db kv.TemporalRoDB) *branchPrefetcher {
	p.work = make(chan prefetchItem, branchPrefetchQueue)
	ctx = kv.WithNonBlockingAcquire(ctx)
	for range branchPrefetchWorkers {
		p.wg.Go(func() { p.run(ctx, db) })
	}
	return p
}

func (p *branchPrefetcher) add(it prefetchItem) {
	if p == nil {
		return
	}
	select {
	case p.work <- it:
	default:
		p.dropped.Add(1)
	}
}

func (p *branchPrefetcher) addBAL(bal types.BlockAccessList) {
	if p == nil {
		return
	}
	for i := range bal {
		ac := &bal[i]
		if len(ac.BalanceChanges)+len(ac.NonceChanges)+len(ac.CodeChanges)+len(ac.StorageChanges) == 0 {
			continue
		}
		account := keccak.Sum256(ac.Address[:])
		p.add(prefetchItem{account: account})
		for _, sc := range ac.StorageChanges {
			slot := sc.Slot.Value()
			p.add(prefetchItem{account: account, slot: keccak.Sum256(slot[:]), storage: true})
		}
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
	refs := v3.ComputeLeafRefs(key, data)
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
	if it.storage {
		v3.PrefetchPath(read, it.account[:], it.slot[:], 64)
		return
	}
	v3.PrefetchPath(read, it.account[:], nil, 0)
}
