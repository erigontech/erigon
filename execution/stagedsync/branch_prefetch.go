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
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

var branchPrefetchEnabled = dbg.EnvBool("COMMITMENT_V3_PREFETCH", true)

const (
	branchPrefetchWorkers  = 8
	branchPrefetchQueue    = 1 << 16
	branchPrefetchPerTx    = 256
	branchPrefetchShards   = 64
	branchPrefetchMaxBytes = 1 << 30
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
	data []byte
	step kv.Step
	refs *commitment.LeafRefs
}

type prefetchedShard struct {
	mu      sync.RWMutex
	records map[string]prefetchedRecord
}

type branchPrefetcher struct {
	work    chan prefetchItem
	wg      sync.WaitGroup
	gate    sync.RWMutex
	seed    maphash.Seed
	shards  [branchPrefetchShards]prefetchedShard
	bytes   atomic.Int64
	domains []kv.Domain
	bin     map[kv.Domain]bool
}

func newBranchPrefetcher(ctx context.Context, db kv.TemporalRoDB, domains []kv.Domain, binDomains map[kv.Domain]bool) *branchPrefetcher {
	if len(domains) == 0 {
		domains = []kv.Domain{kv.CommitmentDomain}
	}
	p := &branchPrefetcher{work: make(chan prefetchItem, branchPrefetchQueue), seed: maphash.MakeSeed(), domains: domains, bin: binDomains}
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
	p.drain()
}

func (p *branchPrefetcher) drain() {
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
	p.drain()
	close(p.work)
	p.wg.Wait()
}

func (p *branchPrefetcher) shard(key []byte) *prefetchedShard {
	return &p.shards[maphash.Bytes(p.seed, key)%branchPrefetchShards]
}

func (p *branchPrefetcher) getDomain(domain kv.Domain, key []byte) ([]byte, kv.Step, bool) {
	s := p.shard(key)
	s.mu.RLock()
	r, ok := s.records[prefetchRecordKey(domain, key)]
	s.mu.RUnlock()
	return r.data, r.step, ok
}

func (p *branchPrefetcher) leafRefs(key, data []byte) *commitment.LeafRefs {
	return p.leafRefsDomain(kv.CommitmentDomain, key, data)
}

func (p *branchPrefetcher) leafRefsDomain(domain kv.Domain, key, data []byte) *commitment.LeafRefs {
	s := p.shard(key)
	s.mu.RLock()
	r, ok := s.records[prefetchRecordKey(domain, key)]
	s.mu.RUnlock()
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
	s := p.shard(key)
	s.mu.Lock()
	s.records[prefetchRecordKey(domain, key)] = prefetchedRecord{data: data, step: step, refs: refs}
	s.mu.Unlock()
	size := len(key) + len(data)
	if refs != nil {
		size += 32 * len(refs.Refs)
	}
	p.bytes.Add(int64(size))
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
		p.touch(tx, it)
	chunk:
		for range branchPrefetchPerTx - 1 {
			select {
			case next, ok := <-p.work:
				if !ok {
					break chunk
				}
				p.touch(tx, next)
			default:
				break chunk
			}
		}
		tx.Rollback()
		p.gate.RUnlock()
	}
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

func prefetchRecordKey(domain kv.Domain, key []byte) string {
	return string(append([]byte{byte(domain)}, key...))
}

func (it *prefetchItem) touch(read func(key []byte) []byte) {
	if it.storage {
		v3.PrefetchPath(read, it.account[:], it.slot[:], 64)
		return
	}
	v3.PrefetchPath(read, it.account[:], nil, 0)
}
