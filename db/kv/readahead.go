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

type DBWithDistributionSupport interface {
	// DistributeCursors partitions table into n approximately equal-count key
	// ranges using mdbx's b-tree distribution. Fast on Table >> RAM: it touches
	// only the b-tree branch nodes. Returned keys are valid until tx end.
	DistributeCursors(table string, from []byte, n int) ([][]byte, error)
}

// ReadAhead keeps a bounded window of pages warm just ahead of a forward table scan
type ReadAhead struct {
	bounds        atomic.Pointer[[][]byte]
	consumerChunk atomic.Int64
	logLvl        atomic.Int32
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
	r.logLvl.Store(int32(log.LvlInfo))
	go r.run(ctx, db, table, from, workers)
	return r
}

// SetLogLevel sets the level of the periodic progress line (default Info); nil-safe.
func (r *ReadAhead) SetLogLevel(lvl log.Lvl) {
	if r == nil {
		return
	}
	r.logLvl.Store(int32(lvl))
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

	var doneChunks, warming, idle atomic.Int64

	// log from a dedicated goroutine: a worker logging after its own warming.Add(-1) would never count itself
	logDone := make(chan struct{})
	go func() {
		logEvery := time.NewTicker(20 * time.Second)
		defer logEvery.Stop()
		for {
			select {
			case <-logDone:
				return
			case <-ctx.Done():
				return
			case <-logEvery.C:
				log.Log(log.Lvl(r.logLvl.Load()), "["+r.label+"]", "table", table, "progress", fmt.Sprintf("%d/%d", doneChunks.Load(), len(bounds)-1), "warming", warming.Load(), "idle", idle.Load(), "workers", workers)
			}
		}
	}()

	var g errgroup.Group // not WithContext: one chunk's read error must not cancel the rest
	g.SetLimit(workers)
	for idx := 0; idx+1 < len(bounds) && ctx.Err() == nil; idx++ {
		g.Go(func() error {
			idle.Add(1)
			turn := r.waitTurn(ctx, idx, ahead) // throttled here until the consumer is close enough
			idle.Add(-1)
			if !turn { // consumer passed it, or ctx cancelled
				return nil
			}
			warming.Add(1)
			r.warm(ctx, db, table, bounds[idx], bounds[idx+1])
			warming.Add(-1)
			doneChunks.Add(1) // count only chunks actually warmed, not the skipped ones
			return nil
		})
	}
	_ = g.Wait()
	close(logDone)
}

// plan splits the table into ~chunkSize chunks and returns the boundaries (nil if
// it can't prefetch) plus how many chunks to keep warm ahead of the consumer.
func (r *ReadAhead) plan(ctx context.Context, db RoDB, table string, from []byte) (bounds [][]byte, ahead int64) {
	const aheadBytes = 1 * datasize.GB
	const chunkSize = 32 * datasize.MB // chunk size — by table size, not worker count
	var tableSize uint64
	err := db.View(ctx, func(tx Tx) error {
		s, ok := tx.(DBWithDistributionSupport)
		if !ok { // engine can't count-split (memdb has nothing to warm) -> skip prefetch
			log.Warn("[read-ahead] disabled: tx is not a DBWithDistributionSupport", "table", table, "tx", fmt.Sprintf("%T", tx))
			return nil
		}
		var err error
		tableSize, err = tx.BucketSize(table)
		if err != nil {
			return err
		}
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
	var tick *time.Ticker
	for {
		cc := r.consumerChunk.Load()
		switch {
		case int64(idx) < cc:
			return false // consumer already passed this chunk - don't warm behind it
		case int64(idx) <= cc+ahead:
			return true
		}
		if tick == nil { // reused across spins instead of allocating a timer per iteration
			tick = time.NewTicker(time.Millisecond)
			defer tick.Stop()
		}
		select {
		case <-ctx.Done():
			return false
		case <-tick.C:
		}
	}
}

// osPageBytes strides a value one page at a time; 4KiB faults every OS page on
// any platform (16KiB-page systems just get a few redundant hits).
const osPageBytes = 4096

// warmupSink must consume the bytes read during warmup: with the result
// discarded, the compiler dead-code-eliminates the value loads and no page is
// ever faulted in. An atomic keeps the loads live and is race-free across workers.
var warmupSink atomic.Uint64

// touchValue faults every OS page backing v into cache, folding one byte per
// page into sink. Keys and inline values are already faulted by the b-tree
// traversal that yielded them; only large (overflow-page) values need this.
func touchValue(sink byte, v []byte) byte {
	for off := 0; off < len(v); off += osPageBytes {
		sink ^= v[off]
	}
	if n := len(v); n > 0 {
		sink ^= v[n-1]
	}
	return sink
}

// warm reads [from,to) so the OS faults each value page into cache.
func (r *ReadAhead) warm(ctx context.Context, db RoDB, table string, from, to []byte) {
	err := db.View(ctx, func(tx Tx) error {
		it, err := tx.Range(table, from, to, order.Asc, -1)
		if err != nil {
			return err
		}
		defer it.Close()
		var sink byte
		defer func() { warmupSink.Add(uint64(sink)) }()
		n := 0
		for it.HasNext() {
			_, v, err := it.Next()
			if err != nil {
				return err
			}
			sink = touchValue(sink, v)
			n++
			if n%128 == 0 && ctx.Err() != nil {
				return ctx.Err()
			}
		}
		return nil
	})
	if err != nil && ctx.Err() == nil { // best-effort warmup, but a real DB error shouldn't vanish
		log.Warn("["+r.label+"] warmup failed", "table", table, "err", err)
	}
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
