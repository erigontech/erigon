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
	"sync"
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

// WarmupChunkSize is the target byte size of each count-balanced chunk that
// ReadAhead prefetches, and that chunked range-delete callers split a table
// into. One definition keeps both boundary sets aligned.
const WarmupChunkSize = 32 * datasize.MB

const readAheadLabel = "read-ahead"

// ReadAhead keeps a bounded window of pages warm just ahead of a forward table scan
type ReadAhead struct {
	mu       sync.Mutex
	turnCond *sync.Cond // broadcast when the consumer advances (SetPos) or ctx is cancelled

	bounds        atomic.Pointer[[][]byte]
	consumerChunk atomic.Int64
	logLvl        atomic.Int32
	cancel        context.CancelFunc
	done          chan struct{}
	preBounds     [][]byte // caller-supplied boundaries; when nil, run() computes its own
}

// NewReadAhead starts `workers` background prefetchers over `table` from `from`
// (nil = table start). Call SetPos as the consumer advances, and Close when done.
// Returns nil (a valid no-op *ReadAhead) when db is nil.
func NewReadAhead(ctx context.Context, db RoDB, table string, from []byte, workers int) *ReadAhead {
	return newReadAhead(ctx, db, table, from, nil, workers)
}

// NewReadAheadFromBounds is like NewReadAhead but reuses count-balanced
// boundaries the caller already computed (e.g. for a chunked range-delete), so
// the b-tree distribution walk isn't repeated. The boundaries must stay valid
// for the ReadAhead's lifetime — clone tx-owned keys before passing them.
func NewReadAheadFromBounds(ctx context.Context, db RoDB, table string, bounds [][]byte, workers int) *ReadAhead {
	return newReadAhead(ctx, db, table, nil, bounds, workers)
}

func newReadAhead(ctx context.Context, db RoDB, table string, from []byte, bounds [][]byte, workers int) *ReadAhead {
	if db == nil {
		return nil // Close/SetPos/SetLogLevel are all nil-safe
	}
	if workers < 1 {
		workers = 1
	}
	ctx, cancel := context.WithCancel(ctx)
	r := &ReadAhead{cancel: cancel, done: make(chan struct{}), preBounds: bounds}
	r.turnCond = sync.NewCond(&r.mu)
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

	bounds := r.preBounds
	if bounds == nil {
		bounds = r.plan(ctx, db, table, from)
	}
	if len(bounds) < 2 {
		return
	}
	r.bounds.Store(&bounds)

	// Keep ~1GB warm ahead of the consumer, but never fewer chunks than there
	// are workers: otherwise waitTurn parks the surplus workers and a large
	// WARMUP_TABLE_WORKERS silently caps out at the byte-window's chunk count.
	const aheadBytes = 1 * datasize.GB
	ahead := max(int64(aheadBytes/WarmupChunkSize), int64(workers))

	// wake workers parked in waitTurn when ctx is cancelled, so Close() doesn't hang
	go func() {
		<-ctx.Done()
		r.mu.Lock()
		r.turnCond.Broadcast()
		r.mu.Unlock()
	}()

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
				log.Log(log.Lvl(r.logLvl.Load()), "["+readAheadLabel+"]", "table", table, "progress", fmt.Sprintf("%d/%d", doneChunks.Load(), len(bounds)-1), "warming", warming.Load(), "idle", idle.Load(), "workers", workers)
			}
		}
	}()

	var g errgroup.Group // not WithContext: one chunk's read error must not cancel the rest
	g.SetLimit(workers)
	for idx := 0; idx+1 < len(bounds) && ctx.Err() == nil; idx++ {
		g.Go(func() error {
			idle.Add(1)
			turn := r.waitTurn(ctx, idx, ahead) // parked here until the consumer is close enough
			idle.Add(-1)
			if !turn { // consumer already passed it, or ctx cancelled
				doneChunks.Add(1) // count skipped chunks too, so progress tracks the consumer instead of reading as stalled
				return nil
			}
			warming.Add(1)
			r.warm(ctx, db, table, bounds[idx], bounds[idx+1])
			warming.Add(-1)
			doneChunks.Add(1)
			return nil
		})
	}
	_ = g.Wait()
	close(logDone)
}

// plan splits the whole table into ~WarmupChunkSize chunks and returns the
// boundaries (nil if the engine can't count-split, e.g. memdb).
func (r *ReadAhead) plan(ctx context.Context, db RoDB, table string, from []byte) (bounds [][]byte) {
	err := db.View(ctx, func(tx Tx) error {
		s, ok := tx.(DBWithDistributionSupport)
		if !ok { // engine can't count-split (memdb has nothing to warm) -> skip prefetch
			log.Warn("["+readAheadLabel+"] disabled: tx is not a DBWithDistributionSupport", "table", table, "tx", fmt.Sprintf("%T", tx))
			return nil
		}
		tableSize, err := tx.BucketSize(table)
		if err != nil {
			return err
		}
		b, err := s.DistributeCursors(table, from, int(tableSize/WarmupChunkSize.Bytes()))
		if err != nil {
			return err
		}
		bounds = make([][]byte, len(b)) // b's interior keys are zero-copy, valid only until this tx ends
		for i, k := range b {
			bounds[i] = bytes.Clone(k)
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return bounds
}

// waitTurn blocks until chunk idx is within `ahead` of the consumer; returns
// false if the consumer already passed it or ctx was cancelled. It parks on a
// condition variable (woken by SetPos when the consumer advances, or by Close)
// rather than polling a timer.
func (r *ReadAhead) waitTurn(ctx context.Context, idx int, ahead int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		if ctx.Err() != nil {
			return false
		}
		cc := r.consumerChunk.Load()
		switch {
		case int64(idx) < cc:
			return false // consumer already passed this chunk - don't warm behind it
		case int64(idx) <= cc+ahead:
			return true
		}
		r.turnCond.Wait()
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
		it, err := tx.Range(table, from, to, order.Asc, Unlim)
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
		log.Warn("["+readAheadLabel+"] warmup failed", "table", table, "err", err)
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
	if r.consumerChunk.Swap(int64(idx)) != int64(idx) { // only wake parked workers when the window actually moves
		r.mu.Lock()
		r.turnCond.Broadcast()
		r.mu.Unlock()
	}
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
