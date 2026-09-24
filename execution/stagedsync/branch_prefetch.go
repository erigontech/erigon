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
	v4 "github.com/erigontech/erigon/execution/commitment/v4"
)

var branchPrefetchEnabled = dbg.EnvBool("COMMITMENT_V4_PREFETCH", true)

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
}

func newBranchPrefetcher(ctx context.Context, db kv.TemporalRoDB) *branchPrefetcher {
	p := &branchPrefetcher{work: make(chan prefetchItem, branchPrefetchQueue), seed: maphash.MakeSeed()}
	for i := range p.shards {
		p.shards[i].records = make(map[string]prefetchedRecord)
	}
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
	}
}

func (p *branchPrefetcher) pause() {
	if p == nil {
		return
	}
	p.gate.Lock()
	for {
		select {
		case <-p.work:
		default:
			return
		}
	}
}

func (p *branchPrefetcher) resume() {
	if p != nil {
		p.gate.Unlock()
	}
}

func (p *branchPrefetcher) close() {
	if p == nil {
		return
	}
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

func (p *branchPrefetcher) put(key, data []byte, step kv.Step) []byte {
	if p.bytes.Load() >= branchPrefetchMaxBytes {
		return data
	}
	data = bytes.Clone(data)
	s := p.shard(key)
	s.mu.Lock()
	s.records[string(key)] = prefetchedRecord{data: data, step: step}
	s.mu.Unlock()
	p.bytes.Add(int64(len(key) + len(data)))
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
			if err != nil || len(data) == 0 {
				return nil
			}
			return p.put(key, data, step)
		}
		it.touch(read)
	chunk:
		for range branchPrefetchPerTx - 1 {
			select {
			case next, ok := <-p.work:
				if !ok {
					break chunk
				}
				next.touch(read)
			default:
				break chunk
			}
		}
		tx.Rollback()
		p.gate.RUnlock()
	}
}

func (it *prefetchItem) touch(read func(key []byte) []byte) {
	if it.storage {
		v4.PrefetchPath(read, it.account[:], it.slot[:], 64)
		return
	}
	v4.PrefetchPath(read, it.account[:], nil, 0)
}
