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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// TrieContextFactory creates new PatriciaContext instances for parallel warmup.
type TrieContextFactory func() (PatriciaContext, func())

// WarmupConfig contains configuration for pre-warming MDBX page cache
// during commitment processing.
type WarmupConfig struct {
	Enabled           bool
	EnableWarmupCache bool // If true, cache warmed data for use during trie processing
	CtxFactory        TrieContextFactory
	NumWorkers        int
	MaxDepth          int
	LogPrefix         string
}

const WarmupMaxDepth = 128 // covers full key paths for both account keys (64 nibbles) and storage keys (128 nibbles)

// WarmupStats contains statistics about the warmup phase.
type WarmupStats struct {
	KeysProcessed uint64
	Duration      time.Duration
}

// Warmuper manages parallel warmup of MDBX page cache by pre-reading trie data.
type Warmuper struct {
	ctx        context.Context
	cancel     context.CancelFunc
	ctxFactory TrieContextFactory
	maxDepth   int
	numWorkers int
	logPrefix  string

	// Work channel for incoming keys
	work chan warmupWorkItem
	// Worker group
	g *errgroup.Group

	// Cache for storing warmed data to be used during trie processing
	cache *WarmupCache

	// Stats
	keysProcessed atomic.Uint64
	startTime     time.Time

	// State
	started atomic.Bool
	closed  atomic.Bool
}

type warmupWorkItem struct {
	hashedKey  []byte
	startDepth int
}

// NewWarmuper creates a new Warmuper instance.
func NewWarmuper(ctx context.Context, cfg WarmupConfig) *Warmuper {
	ctx, cancel := context.WithCancel(ctx)
	w := &Warmuper{
		ctx:        ctx,
		cancel:     cancel,
		ctxFactory: cfg.CtxFactory,
		maxDepth:   cfg.MaxDepth,
		numWorkers: cfg.NumWorkers,
		logPrefix:  cfg.LogPrefix,
	}
	if cfg.EnableWarmupCache {
		w.cache = NewWarmupCache()
	}
	return w
}

// Cache returns the warmup cache, or nil if caching is disabled.
func (w *Warmuper) Cache() *WarmupCache {
	return w.cache
}

// branchFromCacheOrDB reads branch data from cache if available, otherwise from DB and caches it.
func (w *Warmuper) branchFromCacheOrDB(trieCtx PatriciaContext, prefix []byte) ([]byte, error) {
	if w.cache != nil {
		if data, _, found := w.cache.GetBranch(prefix); found {
			return data, nil
		}
	}
	branchData, step, err := trieCtx.Branch(prefix)
	if err != nil {
		return nil, err
	}
	if w.cache != nil && len(branchData) > 0 {
		w.cache.PutBranch(prefix, branchData, step)
	}
	return branchData, nil
}

// accountFromCacheOrDB reads account data from cache if available, otherwise from DB and caches it.
func (w *Warmuper) accountFromCacheOrDB(trieCtx PatriciaContext, plainKey []byte) (*Update, error) {
	if w.cache != nil {
		if update, found := w.cache.GetAccount(plainKey); found {
			return update, nil
		}
	}
	update, err := trieCtx.Account(plainKey)
	if err != nil {
		return nil, err
	}
	if w.cache != nil {
		w.cache.PutAccount(plainKey, update)
	}
	return update, nil
}

// storageFromCacheOrDB reads storage data from cache if available, otherwise from DB and caches it.
func (w *Warmuper) storageFromCacheOrDB(trieCtx PatriciaContext, plainKey []byte) (*Update, error) {
	if w.cache != nil {
		if update, found := w.cache.GetStorage(plainKey); found {
			return update, nil
		}
	}
	update, err := trieCtx.Storage(plainKey)
	if err != nil {
		return nil, err
	}
	if w.cache != nil {
		w.cache.PutStorage(plainKey, update)
	}
	return update, nil
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
	w.work = make(chan warmupWorkItem, 50_000)
	w.g, w.ctx = errgroup.WithContext(w.ctx)

	for i := 0; i < w.numWorkers; i++ {
		w.g.Go(func() error {
			trieCtx, cleanup := w.ctxFactory()
			if cleanup != nil {
				defer cleanup()
			}

			for item := range w.work {
				select {
				case <-w.ctx.Done():
					return w.ctx.Err()
				default:
				}

				w.warmupKey(trieCtx, item.hashedKey, item.startDepth)
				w.keysProcessed.Add(1)
			}
			return nil
		})
	}
}

// warmupKey performs the actual warmup for a single key by reading data to warm MDBX page cache.
// If cache is enabled, the data is also stored in the cache for later use.
func (w *Warmuper) warmupKey(trieCtx PatriciaContext, hashedKey []byte, startDepth int) {
	depth := startDepth
	for depth <= len(hashedKey) && depth <= w.maxDepth {
		prefix := HexNibblesToCompactBytes(hashedKey[:depth])

		// Check cache first, then fall back to DB
		branchData, err := w.branchFromCacheOrDB(trieCtx, prefix)
		if err != nil {
			log.Debug(fmt.Sprintf("[%s][warmup] failed to get branch", w.logPrefix),
				"prefix", common.Bytes2Hex(prefix), "error", err)
		}

		// Branch data format: 2-byte touch map + 2-byte bitmap + per-child data
		if len(branchData) < 4 {
			break
		}

		if depth >= len(hashedKey) {
			break
		}
		nextNibble := int(hashedKey[depth])

		// Extract and prefetch account/storage addresses to warm page cache
		cellAccounts, cellStorages := extractBranchCellAddresses(branchData, nextNibble)
		for _, addr := range cellAccounts {
			_, _ = w.accountFromCacheOrDB(trieCtx, addr)
		}
		for _, addr := range cellStorages {
			_, _ = w.storageFromCacheOrDB(trieCtx, addr)
		}

		branchData = branchData[2:] // skip touch map

		bitmap := binary.BigEndian.Uint16(branchData[0:2])
		childBit := uint16(1) << nextNibble

		if bitmap&childBit == 0 {
			break
		}

		// Find position of our child's data
		pos := 2
		for n := 0; n < nextNibble; n++ {
			if bitmap&(uint16(1)<<n) != 0 {
				if pos >= len(branchData) {
					break
				}
				fieldBits := branchData[pos]
				pos++
				pos = skipCellFields(branchData, pos, fieldBits)
			}
		}

		if pos >= len(branchData) {
			break
		}

		fieldBits := branchData[pos]
		pos++

		// Check if child has extension
		hasExtension := (fieldBits & 1) != 0
		if hasExtension && pos < len(branchData) {
			extLen, n := binary.Uvarint(branchData[pos:])
			if n > 0 && extLen > 0 {
				depth += int(extLen)
				continue
			}
		}

		depth++
	}
}

// WarmKey submits a hashed key for warming. Call Start() first.
// startDepth indicates the depth from which to start warming (based on divergence from previous key).
func (w *Warmuper) WarmKey(hashedKey []byte, startDepth int) {
	if !w.started.Load() || w.numWorkers <= 0 || w.closed.Load() {
		return
	}
	select {
	case w.work <- warmupWorkItem{hashedKey: hashedKey, startDepth: startDepth}:
	case <-w.ctx.Done():
	}
}

// Wait waits for all warmup work to complete.
func (w *Warmuper) wait() {
	if !w.started.Load() || w.numWorkers <= 0 {
		return
	}

	// Only close the channel once
	close(w.work)
	w.g.Wait()

	log.Debug(fmt.Sprintf("[%s][warmup] completed", w.logPrefix),
		"keys", common.PrettyCounter(int(w.keysProcessed.Load())),
		"maxDepth", w.maxDepth,
		"workers", w.numWorkers,
		"spent", time.Since(w.startTime),
	)

	return
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

// WaitAndClose waits for all warmup work to complete and then closes the warmuper.
func (w *Warmuper) WaitAndClose() {
	if w.closed.Swap(true) {
		return // Already closed
	}
	w.wait()
	w.Close()
	return
}

// Close cancels all warmup work and releases resources.
func (w *Warmuper) Close() {
	if w.closed.Swap(true) {
		return // Already closed
	}
	w.cancel()
	close(w.work)
}
