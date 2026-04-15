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
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// TrieContextFactory creates new PatriciaContext instances for parallel warmup.
type TrieContextFactory func() (PatriciaContext, func())

// WarmupConfig contains configuration for pre-warming MDBX page cache
// during commitment processing.
type WarmupConfig struct {
	Enabled    bool
	CtxFactory TrieContextFactory
	NumWorkers int
	MaxDepth   int // caps walk depth per key; 0 means default (128)
	LogPrefix  string
}

type warmupWorkItem struct {
	hashedKey  []byte
	startDepth int
}

// WarmupStats contains statistics about the warmup phase.
type WarmupStats struct {
	KeysProcessed uint64
	Duration      time.Duration
}

// Warmuper manages parallel warmup of MDBX page cache by pre-reading trie data
// using a bespoke depth-walk with a shared CachingPatriciaContext.
type Warmuper struct {
	ctx        context.Context
	cancel     context.CancelFunc
	ctxFactory TrieContextFactory
	cache      *CachingPatriciaContext
	numWorkers int
	maxDepth   int
	logPrefix  string

	// Work channel for incoming keys
	work chan warmupWorkItem
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
	maxDepth := cfg.MaxDepth
	if maxDepth <= 0 {
		maxDepth = 128
	}
	w := &Warmuper{
		ctx:        ctx,
		cancel:     cancel,
		ctxFactory: cfg.CtxFactory,
		cache:      NewCachingPatriciaContext(),
		numWorkers: cfg.NumWorkers,
		maxDepth:   maxDepth,
		logPrefix:  cfg.LogPrefix,
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
	w.startTime = time.Now()

	w.work = make(chan warmupWorkItem, w.numWorkers*64)
	w.g, w.ctx = errgroup.WithContext(w.ctx)

	for i := 0; i < w.numWorkers; i++ {
		w.g.Go(func() error {
			trieCtx, cleanup := w.ctxFactory()
			if cleanup != nil {
				defer cleanup()
			}

			view := w.cache.Wrap(trieCtx)

			for item := range w.work {
				select {
				case <-w.ctx.Done():
					return w.ctx.Err()
				default:
				}

				w.warmupKey(view, item.hashedKey, item.startDepth)
				w.keysProcessed.Add(1)
			}
			return nil
		})
	}
}

// WarmKey submits a hashed key for warming. Call Start() first.
// The key is copied internally so the caller may reuse/overwrite the slice.
// startDepth indicates how many leading nibbles are already warm (from a
// previous key sharing a common prefix); the worker walk begins there.
func (w *Warmuper) WarmKey(hashedKey []byte, startDepth int) {
	if !w.started.Load() || w.numWorkers <= 0 || w.closed.Load() {
		return
	}
	keyCopy := make([]byte, len(hashedKey))
	copy(keyCopy, hashedKey)
	select {
	case w.work <- warmupWorkItem{hashedKey: keyCopy, startDepth: startDepth}:
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
	var err error
	if w.g != nil {
		err = w.g.Wait()
	}
	w.cancel()
	return err
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

// warmupKey performs a bespoke depth-walk for a single hashed key, reading
// branch/account/storage data through the cached view. Much cheaper than
// a full TrieReader.Lookup: no keccak, no full cell parse.
func (w *Warmuper) warmupKey(view PatriciaContext, hashedKey []byte, startDepth int) {
	for depth := startDepth; depth < len(hashedKey) && depth < w.maxDepth; {
		prefix := nibbles.HexToCompact(hashedKey[:depth])
		branchData, _, err := view.Branch(prefix)
		if err != nil {
			log.Debug(fmt.Sprintf("[%s][warmup] branch read failed", w.logPrefix),
				"depth", depth, "error", err)
			return
		}
		if len(branchData) < 4 {
			return
		}

		nextNibble := int(hashedKey[depth])

		cellAccounts, cellStorages := extractBranchCellAddresses(branchData, nextNibble)
		for _, a := range cellAccounts {
			_, _ = view.Account(a)
		}
		for _, s := range cellStorages {
			_, _ = view.Storage(s)
		}

		bitmap := binary.BigEndian.Uint16(branchData[2:4])
		childBit := uint16(1) << nextNibble
		if bitmap&childBit == 0 {
			return
		}

		// Skip sibling cells before the target nibble to find its fieldBits.
		pos := 4
		for n := 0; n < nextNibble; n++ {
			if bitmap&(1<<n) != 0 {
				if pos >= len(branchData) {
					return
				}
				fieldBits := branchData[pos]
				pos = skipCellFields(branchData, pos+1, fieldBits)
			}
		}
		if pos >= len(branchData) {
			return
		}
		fieldBits := branchData[pos]
		pos++

		// Extension handling: advance depth by extension length if present.
		if fieldBits&1 != 0 {
			if extLen, n := binary.Uvarint(branchData[pos:]); n > 0 && extLen > 0 {
				depth += int(extLen)
				continue
			}
		}
		depth++
	}
}
