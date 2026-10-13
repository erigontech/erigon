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
	"fmt"
	"hash/maphash"
	"sync"
	"sync/atomic"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/diagnostics/metrics"
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
	branchCarryBlocks      = 64
)

type prefetchItem struct {
	account [32]byte
	slot    [32]byte
	storage bool
}

type prefetchedRecord struct {
	data  []byte
	step  kv.Step
	refs  *commitment.LeafRefs
	block uint64
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
	byBlock map[uint64][]string
}

type branchPrefetcher struct {
	work   chan prefetchItem
	wg     sync.WaitGroup
	gate   sync.RWMutex
	seed   maphash.Seed
	shards [branchPrefetchShards]prefetchedShard
	bytes  atomic.Int64
	block  uint64
	gen    atomic.Uint64

	refless [][]commitment.BranchDelta

	pausing                        atomic.Bool
	hits, misses, dropped, drained atomic.Uint64
}

func newBranchPrefetcher(ctx context.Context, db kv.TemporalRoDB, block uint64) *branchPrefetcher {
	return newPrefetchRecords().start(ctx, db, block)
}

func newPrefetchRecords() *branchPrefetcher {
	p := &branchPrefetcher{seed: maphash.MakeSeed()}
	for i := range p.shards {
		p.shards[i].records = make(map[string]prefetchedRecord)
		p.shards[i].byBlock = make(map[uint64][]string)
	}
	return p
}

func (p *branchPrefetcher) start(ctx context.Context, db kv.TemporalRoDB, block uint64) *branchPrefetcher {
	p.work, p.block = make(chan prefetchItem, branchPrefetchQueue), block
	p.hits.Store(0)
	p.misses.Store(0)
	p.dropped.Store(0)
	p.drained.Store(0)
	ctx = kv.WithNonBlockingAcquire(ctx)
	for range branchPrefetchWorkers {
		p.wg.Go(func() { p.run(ctx, db) })
	}
	if deltas := p.refless; deltas != nil {
		p.refless = nil
		p.wg.Go(func() { p.fillRefs(deltas) })
	}
	return p
}

func (p *branchPrefetcher) fillRefs(deltas [][]commitment.BranchDelta) {
	for _, part := range deltas {
		for i := range part {
			if p.pausing.Load() {
				return
			}
			key := part[i].Key
			s := p.shard(key)
			s.mu.RLock()
			r, ok := s.records[string(key)]
			s.mu.RUnlock()
			if !ok || r.refs != nil || len(r.data) == 0 {
				continue
			}
			refs := v3.ComputeLeafRefs(key, r.data)
			if refs == nil {
				continue
			}
			s.mu.Lock()
			if cur, ok := s.records[string(key)]; ok && cur.refs == nil && len(cur.data) == len(r.data) && &cur.data[0] == &r.data[0] {
				p.bytes.Add(-cur.size(string(key)))
				cur.refs = refs
				s.records[string(key)] = cur
				p.bytes.Add(cur.size(string(key)))
				mxBranchCarryRefsFilled.Inc()
			}
			s.mu.Unlock()
		}
	}
}

func (p *branchPrefetcher) store(s *prefetchedShard, key string, r prefetchedRecord) {
	if old, ok := s.records[key]; ok {
		p.bytes.Add(-old.size(key))
	}
	s.records[key] = r
	s.byBlock[r.block] = append(s.byBlock[r.block], key)
	p.bytes.Add(r.size(key))
}

func (p *branchPrefetcher) adopt(deltas [][]commitment.BranchDelta, block uint64, step kv.Step) {
	p.gen.Add(1)
	for _, part := range deltas {
		for i := range part {
			s := p.shard(part[i].Key)
			s.mu.Lock()
			p.store(s, string(part[i].Key), prefetchedRecord{data: bytes.Clone(part[i].Data), step: step, block: block})
			s.mu.Unlock()
		}
	}
	p.evictBefore(block + 1 - min(block+1, branchCarryBlocks))
	p.refless = deltas
}

func (p *branchPrefetcher) evictBefore(block uint64) {
	for i := range p.shards {
		s := &p.shards[i]
		s.mu.Lock()
		for b, keys := range s.byBlock {
			if b >= block {
				continue
			}
			for _, k := range keys {
				if r, ok := s.records[k]; ok && r.block == b {
					p.bytes.Add(-r.size(k))
					delete(s.records, k)
				}
			}
			delete(s.byBlock, b)
		}
		s.mu.Unlock()
	}
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

func (p *branchPrefetcher) get(key []byte) ([]byte, kv.Step, bool) {
	s := p.shard(key)
	s.mu.RLock()
	r, ok := s.records[string(key)]
	s.mu.RUnlock()
	return r.data, r.step, ok
}

func (p *branchPrefetcher) leafRefs(key, data []byte) *commitment.LeafRefs {
	s := p.shard(key)
	s.mu.RLock()
	r, ok := s.records[string(key)]
	s.mu.RUnlock()
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
	p.store(s, string(key), prefetchedRecord{data: data, step: step, refs: refs, block: p.block})
	s.mu.Unlock()
	return data
}

func (p *branchPrefetcher) verifyAgainst(tx kv.TemporalTx) error {
	for i := range p.shards {
		s := &p.shards[i]
		s.mu.RLock()
		for k, r := range s.records {
			v, _, err := tx.GetLatest(kv.CommitmentDomain, []byte(k), kv.GetLatestOptions{})
			if err != nil || !bytes.Equal(v, r.data) {
				s.mu.RUnlock()
				return fmt.Errorf("carried branch record %x diverges from the committed state: %x vs %x (err %v)", k, r.data, v, err)
			}
		}
		s.mu.RUnlock()
	}
	return nil
}

func (p *branchPrefetcher) getAt(key []byte, gen uint64) ([]byte, kv.Step, bool) {
	s := p.shard(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p.gen.Load() != gen {
		return nil, 0, false
	}
	r, ok := s.records[string(key)]
	return r.data, r.step, ok
}

var (
	mxBranchCarryAdopted    = metrics.GetOrCreateCounter("commitment_branch_carry_adopted_total")
	mxBranchCarryDropped    = metrics.GetOrCreateCounter("commitment_branch_carry_dropped_total")
	mxBranchCarryRefsFilled = metrics.GetOrCreateCounter("commitment_branch_carry_refs_filled_total")
)

var branchCarry struct {
	sync.Mutex
	db     kv.TemporalRoDB
	p      *branchPrefetcher
	head   commitTarget
	deltas [][]commitment.BranchDelta
	step   kv.Step
}

func storeBranchCarry(db kv.TemporalRoDB, p *branchPrefetcher, head commitTarget, deltas [][]commitment.BranchDelta, step kv.Step) {
	branchCarry.Lock()
	defer branchCarry.Unlock()
	branchCarry.db, branchCarry.p, branchCarry.head, branchCarry.deltas, branchCarry.step = db, p, head, deltas, step
}

func takeBranchCarry(db kv.TemporalRoDB, headNum uint64, headHash common.Hash) *branchPrefetcher {
	branchCarry.Lock()
	defer branchCarry.Unlock()
	p, head, deltas, step := branchCarry.p, branchCarry.head, branchCarry.deltas, branchCarry.step
	match := p != nil && branchCarry.db == db && head.blockNum == headNum && head.blockHash == headHash
	branchCarry.db, branchCarry.p, branchCarry.deltas = nil, nil, nil
	if !match {
		if p != nil {
			mxBranchCarryDropped.Inc()
		}
		return nil
	}
	p.adopt(deltas, headNum, step)
	mxBranchCarryAdopted.Inc()
	return p
}

func dropBranchCarry() {
	branchCarry.Lock()
	defer branchCarry.Unlock()
	if branchCarry.p != nil {
		mxBranchCarryDropped.Inc()
	}
	branchCarry.db, branchCarry.p, branchCarry.deltas = nil, nil, nil
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
