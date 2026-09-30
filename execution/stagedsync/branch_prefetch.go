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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types"
)

const (
	branchPrefetchWorkers  = 8
	branchPrefetchQueue    = 1 << 16
	branchPrefetchPerTx    = 256
	branchPrefetchShards   = 64
	branchPrefetchMaxBytes = 1 << 30
)

var branchPrefetchSeed = maphash.MakeSeed()

type prefetchItem struct {
	key     [length.Addr + length.Hash]byte
	storage bool
}

func accountPrefetch(addr common.Address) (it prefetchItem) {
	copy(it.key[:], addr[:])
	return it
}

func storagePrefetch(addr common.Address, slot common.Hash) prefetchItem {
	it := accountPrefetch(addr)
	copy(it.key[length.Addr:], slot[:])
	it.storage = true
	return it
}

func (it *prefetchItem) walk(read func(key []byte) []byte) {
	key := it.key[:length.Addr]
	if it.storage {
		key = it.key[:]
	}
	commitment.PrefetchBranchPath(commitment.KeyToHexNibbleHash(key), read)
}

type prefetchedRecord struct {
	data []byte
	step kv.Step
}

type prefetchedShard struct {
	mu      sync.RWMutex
	records map[string]prefetchedRecord
}

type branchPrefetcher struct {
	work   chan prefetchItem
	wg     sync.WaitGroup
	shards [branchPrefetchShards]prefetchedShard
	bytes  atomic.Int64

	hits, misses, dropped, drained atomic.Uint64
}

func newBranchPrefetcher(ctx context.Context, db kv.TemporalRoDB) *branchPrefetcher {
	p := &branchPrefetcher{work: make(chan prefetchItem, branchPrefetchQueue)}
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
	for i := range bal {
		ac := &bal[i]
		if len(ac.BalanceChanges)+len(ac.NonceChanges)+len(ac.CodeChanges) > 0 {
			p.add(accountPrefetch(ac.Address))
		}
		for _, sc := range ac.StorageChanges {
			p.add(storagePrefetch(ac.Address, sc.Slot.Value()))
		}
	}
}

func (p *branchPrefetcher) drain() {
	if p == nil {
		return
	}
	for {
		select {
		case <-p.work:
			p.drained.Add(1)
		default:
			return
		}
	}
}

func (p *branchPrefetcher) close() {
	p.drain()
	close(p.work)
	p.wg.Wait()
}

func (p *branchPrefetcher) shard(key []byte) *prefetchedShard {
	return &p.shards[maphash.Bytes(branchPrefetchSeed, key)%branchPrefetchShards]
}

func (p *branchPrefetcher) get(key []byte) ([]byte, kv.Step, bool) {
	s := p.shard(key)
	s.mu.RLock()
	r, ok := s.records[string(key)]
	s.mu.RUnlock()
	return r.data, r.step, ok
}

func (p *branchPrefetcher) put(key, data []byte, step kv.Step) []byte {
	if p.bytes.Load() >= branchPrefetchMaxBytes {
		return data
	}
	data = bytes.Clone(data)
	s := p.shard(key)
	s.mu.Lock()
	if s.records == nil {
		s.records = make(map[string]prefetchedRecord)
	}
	s.records[string(key)] = prefetchedRecord{data: data, step: step}
	s.mu.Unlock()
	p.bytes.Add(int64(len(key) + len(data)))
	return data
}

func (p *branchPrefetcher) run(ctx context.Context, db kv.TemporalRoDB) {
	for it := range p.work {
		tx, err := db.BeginTemporalRo(ctx) //nolint:gocritic
		if err != nil {
			p.dropped.Add(1)
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
		it.walk(read)
	chunk:
		for range branchPrefetchPerTx - 1 {
			select {
			case next, ok := <-p.work:
				if !ok {
					break chunk
				}
				next.walk(read)
			default:
				break chunk
			}
		}
		tx.Rollback()
	}
}
