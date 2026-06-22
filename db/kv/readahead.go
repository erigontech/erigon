// Copyright 2024 The Erigon Authors
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

package kv

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/c2h5oh/datasize"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/order"
)

type BucketSplitter interface {
	// DistributeCursors partitions table into n approximately equal-COUNT key
	// ranges using mdbx's b-tree distribution.
	// It's fast on `Table >> RAM` case because touching only bran-nodes of b-tree
	// Returned keys valid until tx end
	DistributeCursors(table string, from []byte, n int) ([][]byte, error)
}

// ReadAhead keeps a bounded window of pages warm just ahead of a forward table scan
type ReadAhead struct {
	bounds        atomic.Pointer[[][]byte]
	consumerChunk atomic.Int64
	cancel        context.CancelFunc
	done          chan struct{}
	label         string
	full          bool // warm every chunk ignoring consumer position (whole-table warmup)
}

// NewReadAhead starts `workers` background prefetchers over `table` from `from`
// (nil = table start). Call SetPos as the consumer advances, and Close when done.
func NewReadAhead(ctx context.Context, db RoDB, table string, from []byte, workers int) *ReadAhead {
	return newReadAhead(ctx, db, table, from, workers, "read-ahead", false)
}
func newReadAhead(ctx context.Context, db RoDB, table string, from []byte, workers int, label string, full bool) *ReadAhead {
	if workers < 1 {
		workers = 1
	}
	ctx, cancel := context.WithCancel(ctx)
	r := &ReadAhead{cancel: cancel, done: make(chan struct{}), label: label, full: full}
	go r.run(ctx, db, table, from, workers)
	return r
}

func (r *ReadAhead) run(ctx context.Context, db RoDB, table string, from []byte, workers int) {
	defer close(r.done)

	bounds, ahead := r.plan(ctx, db, table, from)
	if len(bounds) < 2 {
		return
	}
	if r.full {
		ahead = int64(len(bounds)) // no consumer to follow: keep every chunk in the window
	}
	r.bounds.Store(&bounds)

	var doneChunks, alive atomic.Int64
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	var g errgroup.Group // not WithContext: one chunk's read error must not cancel the rest
	g.SetLimit(workers)
	for idx := 0; idx+1 < len(bounds) && ctx.Err() == nil; idx++ {
		g.Go(func() error {
			alive.Add(1)
			defer alive.Add(-1)
			if !r.waitTurn(ctx, idx, ahead) { // consumer passed it, or ctx cancelled
				return nil
			}
			r.warm(ctx, db, table, bounds[idx], bounds[idx+1])
			doneChunks.Add(1) // count only chunks actually warmed, not the skipped ones
			select {
			case <-logEvery.C:
				log.Info("["+r.label+"]", "table", table, "consumer-chunk", r.consumerChunk.Load(), "done", doneChunks.Load(), "chunks", len(bounds)-1, "alive", alive.Load(), "workers", workers)
			default:
			}
			return nil
		})
	}
	_ = g.Wait()
}

// plan splits the table into ~chunkSize chunks and returns the boundaries (nil if
// it can't prefetch) plus how many chunks to keep warm ahead of the consumer.
func (r *ReadAhead) plan(ctx context.Context, db RoDB, table string, from []byte) (bounds [][]byte, ahead int64) {
	const aheadBytes = 1 * datasize.GB
	const chunkSize = 32 * datasize.MB // chunk size — by table size, not worker count
	var tableSize uint64
	err := db.View(ctx, func(tx Tx) error {
		s, ok := tx.(BucketSplitter)
		if !ok { // engine can't count-split (memdb has nothing to warm) -> skip prefetch
			log.Warn("[read-ahead] disabled: tx is not a BucketSplitter", "table", table, "tx", fmt.Sprintf("%T", tx))
			return nil
		}
		tableSize, _ = tx.BucketSize(table)
		b, err := s.DistributeCursors(table, from, int(tableSize/chunkSize.Bytes()))
		if err != nil {
			return err
		}
		bounds = make([][]byte, len(b)) // b's interior keys are zero-copy, valid only until this tx ends
		for i, k := range b {
			bounds[i] = bytes.Clone(k)
		}
		return nil
	})
	if err != nil || len(bounds) < 2 {
		return nil, 0
	}
	// ~aheadBytes worth of chunks: a fixed amount of data, not scaled by workers,
	// so prefetchers cluster just ahead of the consumer instead of flooding the
	// table and starving it of disk.
	ahead = max(int64(aheadBytes.Bytes()*uint64(len(bounds)-1)/max(tableSize, 1)), 1)
	return bounds, ahead
}

// waitTurn blocks until chunk idx is within `ahead` of the consumer; returns
// false if the consumer already passed it or ctx was cancelled.
func (r *ReadAhead) waitTurn(ctx context.Context, idx int, ahead int64) bool {
	for {
		cc := r.consumerChunk.Load()
		switch {
		case int64(idx) < cc:
			return false // consumer already passed this chunk - don't warm behind it
		case int64(idx) <= cc+ahead:
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(time.Millisecond):
		}
	}
}

// warm reads [from,to) so the OS faults each key/value page into cache.
func (r *ReadAhead) warm(ctx context.Context, db RoDB, table string, from, to []byte) {
	_ = db.View(ctx, func(tx Tx) error {
		it, err := tx.Range(table, from, to, order.Asc, -1)
		if err != nil {
			return err
		}
		defer it.Close()
		for n := 0; it.HasNext(); n++ {
			k, v, err := it.Next()
			if err != nil {
				return err
			}
			if len(v) > 0 {
				_, _ = v[0], v[len(v)-1] // fault the value page into the OS cache
			}
			if len(k) > 0 {
				_, _ = k[0], k[len(k)-1]
			}
			if n%128 == 0 && ctx.Err() != nil {
				return ctx.Err()
			}
		}
		return nil
	})
}

// SetPos reports the consumer's position so prefetchers throttle to a bounded
// distance ahead; cheap (one binary search), nil-safe, call every few thousand keys.
func (r *ReadAhead) SetPos(key []byte) {
	if r == nil {
		return
	}
	p := r.bounds.Load()
	if p == nil {
		return
	}
	bounds := *p
	idx := sort.Search(len(bounds)-1, func(i int) bool {
		b := bounds[i+1]
		return b == nil || bytes.Compare(b, key) > 0
	})
	r.consumerChunk.Store(int64(idx))
}

// Close stops the prefetchers and waits for their read txs to be released.
// Safe to call on a nil *ReadAhead.
func (r *ReadAhead) Close() {
	if r == nil {
		return
	}
	r.cancel()
	<-r.done
}
