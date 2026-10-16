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
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
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
	account     [32]byte
	slot        [32]byte
	address     [20]byte
	plainSlot   [32]byte
	codeHash    [32]byte
	codeChunks  int
	codeWritten bool
	storage     bool
}

type prefetchedRecord struct {
	data  []byte
	step  kv.Step
	refs  *commitment.LeafRefs
	block uint64
}

func (r prefetchedRecord) size(key string) int64 {
	return int64(len(key) + len(r.data) + leafRefsSize(r.refs))
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

	domains []kv.Domain
	bin     map[kv.Domain]bool

	overlay      atomic.Pointer[map[string]prefetchedRecord]
	overlayBlock uint64

	pausing                        atomic.Bool
	hits, misses, dropped, drained atomic.Uint64
}

func newBranchPrefetcher(ctx context.Context, db kv.TemporalRoDB, block uint64, domains []kv.Domain, binDomains map[kv.Domain]bool) *branchPrefetcher {
	p := newPrefetchRecords()
	if len(domains) != 0 {
		p.domains, p.bin = domains, binDomains
	}
	return p.start(ctx, db, block)
}

func hexOnlyPrefetch(domains []kv.Domain, binDomains map[kv.Domain]bool) bool {
	return len(domains) == 1 && domains[0] == kv.CommitmentDomain && !binDomains[kv.CommitmentDomain]
}

func newPrefetchRecords() *branchPrefetcher {
	p := &branchPrefetcher{seed: maphash.MakeSeed(), domains: []kv.Domain{kv.CommitmentDomain}}
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
	if p.overlay.Load() != nil {
		p.wg.Go(p.mergeOverlay)
	}
	return p
}

func overlayOf(deltas [][]commitment.BranchDelta, block uint64, step kv.Step) map[string]prefetchedRecord {
	n := 0
	for _, part := range deltas {
		n += len(part)
	}
	ov := make(map[string]prefetchedRecord, n)
	for _, part := range deltas {
		for i := range part {
			data := bytes.Clone(part[i].Data)
			refs := v3.ComputeLeafRefs(part[i].Key, data)
			if refs != nil {
				mxBranchCarryRefsFilled.Inc()
			}
			ov[prefetchRecordKey(kv.CommitmentDomain, part[i].Key)] = prefetchedRecord{data: data, step: step, refs: refs, block: block}
		}
	}
	return ov
}

func (p *branchPrefetcher) mergeOverlay() {
	ov := p.overlay.Load()
	if ov == nil {
		return
	}
	p.gen.Add(1)
	for k, r := range *ov {
		s := p.shard(k)
		s.mu.Lock()
		p.store(s, k, r)
		s.mu.Unlock()
	}
	p.overlay.Store(nil)
	p.evictBefore(p.overlayBlock + 1 - min(p.overlayBlock+1, branchCarryBlocks))
}

func (p *branchPrefetcher) store(s *prefetchedShard, key string, r prefetchedRecord) {
	if old, ok := s.records[key]; ok {
		p.bytes.Add(-old.size(key))
	}
	s.records[key] = r
	s.byBlock[r.block] = append(s.byBlock[r.block], key)
	p.bytes.Add(r.size(key))
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
		item := prefetchItem{account: account, address: ac.Address}
		var codeChange *types.CodeChange
		for _, change := range ac.CodeChanges {
			if change != nil && (codeChange == nil || change.Index > codeChange.Index) {
				codeChange = change
			}
		}
		if codeChange != nil {
			item.codeHash = keccak.Sum256(codeChange.Bytecode)
			item.codeChunks = (len(codeChange.Bytecode) + eip8297.ChunkDataLen - 1) / eip8297.ChunkDataLen
			item.codeWritten = true
		}
		p.add(item)
		for _, sc := range ac.StorageChanges {
			slot := sc.Slot.Value()
			p.add(prefetchItem{account: account, slot: keccak.Sum256(slot[:]), address: ac.Address, plainSlot: slot, storage: true})
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

func (p *branchPrefetcher) shard(rk string) *prefetchedShard {
	return &p.shards[maphash.String(p.seed, rk)%branchPrefetchShards]
}

func prefetchRecordKey(domain kv.Domain, key []byte) string {
	return string(append([]byte{byte(domain)}, key...))
}

func (p *branchPrefetcher) record(rk string) (prefetchedRecord, bool) {
	if ov := p.overlay.Load(); ov != nil {
		if r, ok := (*ov)[rk]; ok {
			return r, true
		}
	}
	s := p.shard(rk)
	s.mu.RLock()
	r, ok := s.records[rk]
	s.mu.RUnlock()
	return r, ok
}

func (p *branchPrefetcher) get(key []byte) ([]byte, kv.Step, bool) {
	return p.getDomain(kv.CommitmentDomain, key)
}

func (p *branchPrefetcher) getDomain(domain kv.Domain, key []byte) ([]byte, kv.Step, bool) {
	r, ok := p.record(prefetchRecordKey(domain, key))
	return r.data, r.step, ok
}

func (p *branchPrefetcher) leafRefs(key, data []byte) *commitment.LeafRefs {
	return p.leafRefsDomain(kv.CommitmentDomain, key, data)
}

func (p *branchPrefetcher) leafRefsDomain(domain kv.Domain, key, data []byte) *commitment.LeafRefs {
	r, ok := p.record(prefetchRecordKey(domain, key))
	if !ok || !bytes.Equal(r.data, data) {
		return nil
	}
	return r.refs
}

func (p *branchPrefetcher) put(key, data []byte, step kv.Step) []byte {
	return p.putDomain(kv.CommitmentDomain, key, data, step)
}

func (p *branchPrefetcher) putDomain(domain kv.Domain, key, data []byte, step kv.Step) []byte {
	if p.bytes.Load() >= branchPrefetchMaxBytes {
		return data
	}
	data = bytes.Clone(data)
	var refs *commitment.LeafRefs
	if p.bin[domain] {
		refs = pbt.ComputeLeafRefs(key, data)
	} else {
		refs = v3.ComputeLeafRefs(key, data)
	}
	rk := prefetchRecordKey(domain, key)
	s := p.shard(rk)
	s.mu.Lock()
	p.store(s, rk, prefetchedRecord{data: data, step: step, refs: refs, block: p.block})
	s.mu.Unlock()
	return data
}

func leafRefsSize(refs *commitment.LeafRefs) int {
	if refs == nil {
		return 0
	}
	size := 32 * len(refs.Refs)
	for _, prefix := range refs.Prefixes {
		size += len(prefix)
	}
	for i := range refs.PBinInternal {
		ref := &refs.PBinInternal[i]
		size += 2 + 2 + 2 + 32 + 32 + 32 + len(ref.Prefix)
	}
	return size
}

func (p *branchPrefetcher) verifyAgainst(tx kv.TemporalTx) error {
	check := func(k string, data []byte) error {
		v, _, err := tx.GetLatest(kv.Domain(k[0]), []byte(k[1:]), kv.GetLatestOptions{})
		if err != nil {
			return fmt.Errorf("carried branch record %x: %w", k, err)
		}
		if !bytes.Equal(v, data) {
			return fmt.Errorf("carried branch record %x diverges from the committed state: %x vs %x", k, data, v)
		}
		return nil
	}
	var ov map[string]prefetchedRecord
	if o := p.overlay.Load(); o != nil {
		ov = *o
	}
	for k, r := range ov {
		if err := check(k, r.data); err != nil {
			return err
		}
	}
	for i := range p.shards {
		s := &p.shards[i]
		s.mu.RLock()
		for k, r := range s.records {
			if _, shadowed := ov[k]; shadowed {
				continue
			}
			if err := check(k, r.data); err != nil {
				s.mu.RUnlock()
				return err
			}
		}
		s.mu.RUnlock()
	}
	return nil
}

func (p *branchPrefetcher) getAt(key []byte, gen uint64) ([]byte, kv.Step, bool) {
	rk := prefetchRecordKey(kv.CommitmentDomain, key)
	s := p.shard(rk)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p.gen.Load() != gen {
		return nil, 0, false
	}
	r, ok := s.records[rk]
	return r.data, r.step, ok
}

var (
	mxBranchCarryAdopted    = metrics.GetOrCreateCounter("commitment_branch_carry_adopted_total")
	mxBranchCarryDropped    = metrics.GetOrCreateCounter("commitment_branch_carry_dropped_total")
	mxBranchCarryRefsFilled = metrics.GetOrCreateCounter("commitment_branch_carry_refs_filled_total")
)

type carriedRecords struct {
	db   kv.TemporalRoDB
	p    *branchPrefetcher
	head commitTarget
	ov   map[string]prefetchedRecord
	done chan struct{}
}

var branchCarry struct {
	sync.Mutex
	c *carriedRecords
}

func storeBranchCarry(db kv.TemporalRoDB, p *branchPrefetcher, head commitTarget, deltas [][]commitment.BranchDelta, step kv.Step) {
	c := &carriedRecords{db: db, p: p, head: head, done: make(chan struct{})}
	go func() {
		defer close(c.done)
		c.ov = overlayOf(deltas, head.blockNum, step)
	}()
	branchCarry.Lock()
	defer branchCarry.Unlock()
	branchCarry.c = c
}

func takeBranchCarry(db kv.TemporalRoDB, headNum uint64, headHash common.Hash) *branchPrefetcher {
	branchCarry.Lock()
	c := branchCarry.c
	branchCarry.c = nil
	branchCarry.Unlock()
	if c == nil {
		return nil
	}
	if c.db != db || c.head.blockNum != headNum || c.head.blockHash != headHash {
		mxBranchCarryDropped.Inc()
		return nil
	}
	<-c.done
	c.p.overlayBlock = headNum
	c.p.overlay.Store(&c.ov)
	mxBranchCarryAdopted.Inc()
	return c.p
}

func dropBranchCarry() {
	branchCarry.Lock()
	defer branchCarry.Unlock()
	if branchCarry.c != nil {
		mxBranchCarryDropped.Inc()
	}
	branchCarry.c = nil
}

func (p *branchPrefetcher) run(ctx context.Context, db kv.TemporalRoDB) {
	for it := range p.work {
		p.gate.RLock()
		tx, err := db.BeginTemporalRo(ctx) //nolint:gocritic
		if err != nil {
			p.gate.RUnlock()
			continue
		}
		p.walkChunk(it, func(it prefetchItem) { p.touch(tx, it) })
		tx.Rollback()
		p.gate.RUnlock()
	}
}

func (p *branchPrefetcher) walkChunk(it prefetchItem, touch func(prefetchItem)) (n int) {
	touch(it)
	for n = 1; n < branchPrefetchPerTx && !p.pausing.Load(); n++ {
		select {
		case next, ok := <-p.work:
			if !ok {
				return n
			}
			touch(next)
		default:
			return n
		}
	}
	return n
}

func (p *branchPrefetcher) touch(tx kv.TemporalTx, it prefetchItem) {
	for _, domain := range p.domains {
		read := func(key []byte) []byte {
			if data, _, ok := p.getDomain(domain, key); ok {
				return data
			}
			data, step, err := tx.GetLatest(domain, key, kv.GetLatestOptions{})
			if err != nil {
				return nil
			}
			return p.putDomain(domain, key, data, step)
		}
		if p.bin[domain] {
			address := it.address[:]
			pbt.PrefetchPath(read, eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey))
			pbt.PrefetchPath(read, eip8297.TreeKeyAccount(address, eip8297.CodeHashLeafKey))
			pbt.PrefetchPath(read, eip8297.TreeKeyAccount(address, eip8297.DelegationLeafKey))
			if it.storage {
				pbt.PrefetchPath(read, eip8297.TreeKeyStorage(address, it.plainSlot[:]))
			}
			if it.codeWritten {
				for chunk := range it.codeChunks {
					pbt.PrefetchPath(read, eip8297.TreeKeyCodeChunk(common.BytesToHash(it.codeHash[:]), chunk))
				}
			}
		} else {
			it.touch(read)
		}
	}
}

func (it *prefetchItem) touch(read func(key []byte) []byte) {
	if it.storage {
		v3.PrefetchPath(read, it.account[:], it.slot[:], 64)
		return
	}
	v3.PrefetchPath(read, it.account[:], nil, 0)
}
