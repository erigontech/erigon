// Copyright 2022 The Erigon Authors
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

package commitment

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/log/v3"
)

// TrieContextFactory creates new PatriciaContext instances for parallel warmup.
type TrieContextFactory func() (PatriciaContext, func())

// WarmupConfig contains configuration for pre-warming MDBX page cache
// during commitment processing.
type WarmupConfig struct {
	Enabled       bool
	CtxFactory    TrieContextFactory
	NumWorkers    int
	AccountKeyLen int // plain account key length for TrieReader (typically length.Addr = 20)
	LogPrefix     string
}

// WarmupStats contains statistics about the warmup phase.
type WarmupStats struct {
	KeysProcessed uint64
	Duration      time.Duration
}

// Warmuper manages parallel warmup of MDBX page cache by pre-reading trie data
// using TrieReader-based traversal with a shared CachingPatriciaContext.
type Warmuper struct {
	ctx           context.Context
	cancel        context.CancelFunc
	ctxFactory    TrieContextFactory
	cache         *CachingPatriciaContext
	numWorkers    int
	accountKeyLen int
	logPrefix     string

	// Work channel for incoming keys
	work chan []byte
	// Worker group
	g *errgroup.Group

	// Stats
	keysProcessed atomic.Uint64
	startTime     time.Time

	// State
	started atomic.Bool
	closed  atomic.Bool
}

// NewWarmuper creates a new Warmuper instance.
func NewWarmuper(ctx context.Context, cfg WarmupConfig) *Warmuper {
	ctx, cancel := context.WithCancel(ctx)
	w := &Warmuper{
		ctx:           ctx,
		cancel:        cancel,
		ctxFactory:    cfg.CtxFactory,
		cache:         NewCachingPatriciaContext(),
		numWorkers:    cfg.NumWorkers,
		accountKeyLen: cfg.AccountKeyLen,
		logPrefix:     cfg.LogPrefix,
	}
	return w
}

// SharedCache returns the CachingPatriciaContext used by warmup workers.
// The cache is populated during warmup and can be shared with Process().
func (w *Warmuper) SharedCache() *CachingPatriciaContext {
	return w.cache
}

// Start initializes and starts the warmup workers.
func (w *Warmuper) Start() {
	if w.started.Swap(true) {
		return // Already started
	}
	if w.numWorkers <= 0 {
		return
	}

	w.work = make(chan []byte, w.numWorkers*64)
	w.g, w.ctx = errgroup.WithContext(w.ctx)

	for i := 0; i < w.numWorkers; i++ {
		w.g.Go(func() error {
			trieCtx, cleanup := w.ctxFactory()
			if cleanup != nil {
				defer cleanup()
			}

			// Wrap the worker context with the shared cache so all reads
			// are cached and visible to other workers and later to Process().
			view := w.cache.Wrap(trieCtx)
			reader := NewTrieReader(view, w.accountKeyLen)

			// Visitor prefetches account/storage data encountered along
			// the trie path, populating the shared cache.
			visitor := func(_ int, c *cell) error {
				if c.accountAddrLen > 0 {
					_, _ = view.Account(c.GetAccountAddr())
				}
				if c.storageAddrLen > 0 {
					_, _ = view.Storage(c.GetStorageAddr())
				}
				return nil
			}

			for hashedKey := range w.work {
				select {
				case <-w.ctx.Done():
					return w.ctx.Err()
				default:
				}

				if _, _, err := reader.LookupWithVisitor(hashedKey, visitor); err != nil {
					log.Debug(fmt.Sprintf("[%s][warmup] lookup failed", w.logPrefix),
						"error", err)
				}
				w.keysProcessed.Add(1)
			}
			return nil
		})
	}
}

// WarmKey submits a hashed key for warming. Call Start() first.
func (w *Warmuper) WarmKey(hashedKey []byte) {
	if !w.started.Load() || w.numWorkers <= 0 || w.closed.Load() {
		return
	}
	select {
	case w.work <- hashedKey:
	case <-w.ctx.Done():
	default: // non-blocking
	}
}

// Wait closes the work channel so workers drain remaining items, then waits for
// all workers to finish. Unlike Close(), it does not cancel the context, allowing
// workers to process any queued keys before exiting.
func (w *Warmuper) Wait() error {
	if !w.started.Load() || w.numWorkers <= 0 {
		return nil
	}
	if !w.closed.Swap(true) && w.work != nil {
		close(w.work)
	}
	if w.g != nil {
		_ = w.g.Wait()
	}
	w.cancel()
	return nil
}

// Stats returns statistics about the warmup.
func (w *Warmuper) Stats() WarmupStats {
	duration := time.Duration(0)
	if !w.startTime.IsZero() {
		duration = time.Since(w.startTime)
	}
	return WarmupStats{
		KeysProcessed: w.keysProcessed.Load(),
		Duration:      duration,
	}
}

// DrainPending drains all pending work items from the work channel without processing them.
func (w *Warmuper) DrainPending() {
	if !w.started.Load() || w.numWorkers <= 0 {
		return
	}
	for {
		select {
		case <-w.work:
		default:
			return
		}
	}
}

// CloseAndWait cancel and waits for all warmup work
func (w *Warmuper) CloseAndWait() {
	w.Close()
	if w.g != nil {
		_ = w.g.Wait()
	}
}

// Close cancels all warmup work and releases resources.
// Use CloseAndWait() to cancel and wait for workers.
// Use Wait() to let workers finish processing remaining items.
func (w *Warmuper) Close() {
	if w.closed.Swap(true) {
		return // Already closed
	}
	w.cancel()
	if w.work != nil {
		close(w.work)
	}
}

// Closed returns true if the warmuper has been closed.
func (w *Warmuper) Closed() bool {
	return w.closed.Load()
}
