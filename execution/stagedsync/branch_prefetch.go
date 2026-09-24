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
	"context"
	"sync"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	v4 "github.com/erigontech/erigon/execution/commitment/v4"
)

var branchPrefetchEnabled = dbg.EnvBool("COMMITMENT_V4_PREFETCH", true)

const (
	branchPrefetchWorkers = 8
	branchPrefetchQueue   = 1 << 16
	branchPrefetchPerTx   = 256
)

type prefetchItem struct {
	account [32]byte
	slot    [32]byte
	storage bool
}

type branchPrefetcher struct {
	work chan prefetchItem
	wg   sync.WaitGroup
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
	}
}

func (p *branchPrefetcher) drain() {
	if p == nil {
		return
	}
	for {
		select {
		case <-p.work:
		default:
			return
		}
	}
}

func (p *branchPrefetcher) close() {
	if p == nil {
		return
	}
	close(p.work)
	p.wg.Wait()
}

func (p *branchPrefetcher) run(ctx context.Context, db kv.TemporalRoDB) {
	for it := range p.work {
		tx, err := db.BeginTemporalRo(ctx) //nolint:gocritic
		if err != nil {
			continue
		}
		read := func(key []byte) []byte {
			v, _, err := tx.GetLatest(kv.CommitmentDomain, key, kv.GetLatestOptions{})
			if err != nil {
				return nil
			}
			return v
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
	}
}

func (it *prefetchItem) touch(read func(key []byte) []byte) {
	if it.storage {
		v4.PrefetchPath(read, it.account[:], it.slot[:], 64)
		return
	}
	v4.PrefetchPath(read, it.account[:], nil, 0)
}
