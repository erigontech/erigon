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
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// TrieContextFactory creates new PatriciaContext instances for parallel warmup.
type TrieContextFactory func() (PatriciaContext, func())

// WarmupConfig contains configuration for pre-warming MDBX page cache
// during commitment processing.
type WarmupConfig struct {
	Enabled    bool
	CtxFactory TrieContextFactory
	NumWorkers int
	MaxDepth   int
	LogPrefix  string
}

const WarmupMaxDepth = 128 // covers full key paths for both account keys (64 nibbles) and storage keys (128 nibbles)

// BranchEntry stores branch data along with its step value.
type BranchEntry struct {
	Data []byte
	Step kv.Step
}

// WarmupCache stores prefetched Account, Storage, and Branch data from warmup phase.
// Thread-safe for concurrent writes during warmup.
type WarmupCache struct {
	accounts sync.Map // key: string(address), value: *Update
	storages sync.Map // key: string(address), value: *Update
	branches sync.Map // key: string(prefix), value: *BranchEntry
}

func NewWarmupCache() *WarmupCache {
	return &WarmupCache{}
}

func (c *WarmupCache) SetAccount(addr []byte, update *Update) {
	if update != nil {
		c.accounts.Store(string(addr), update)
	}
}

func (c *WarmupCache) SetStorage(addr []byte, update *Update) {
	if update != nil {
		c.storages.Store(string(addr), update)
	}
}

func (c *WarmupCache) GetAccount(addr []byte) (*Update, bool) {
	if v, ok := c.accounts.Load(string(addr)); ok {
		return v.(*Update), true
	}
	return nil, false
}

func (c *WarmupCache) GetStorage(addr []byte) (*Update, bool) {
	if v, ok := c.storages.Load(string(addr)); ok {
		return v.(*Update), true
	}
	return nil, false
}

func (c *WarmupCache) SetBranch(prefix []byte, data []byte, step kv.Step) {
	if data != nil {
		c.branches.Store(string(prefix), &BranchEntry{Data: data, Step: step})
	}
}

func (c *WarmupCache) GetBranch(prefix []byte) (*BranchEntry, bool) {
	if v, ok := c.branches.Load(string(prefix)); ok {
		return v.(*BranchEntry), true
	}
	return nil, false
}

// Clear removes all entries from the cache.
func (c *WarmupCache) Clear() {
	c.accounts.Clear()
	c.storages.Clear()
	c.branches.Clear()
}

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
	// Result cache
	cache *WarmupCache
	// Worker group
	g *errgroup.Group

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
	return &Warmuper{
		ctx:        ctx,
		cancel:     cancel,
		ctxFactory: cfg.CtxFactory,
		maxDepth:   cfg.MaxDepth,
		numWorkers: cfg.NumWorkers,
		logPrefix:  cfg.LogPrefix,
		cache:      NewWarmupCache(),
	}
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

// warmupKey performs the actual warmup for a single key.
func (w *Warmuper) warmupKey(trieCtx PatriciaContext, hashedKey []byte, startDepth int) {
	depth := startDepth
	for depth <= len(hashedKey) && depth <= w.maxDepth {
		prefix := HexNibblesToCompactBytes(hashedKey[:depth])

		// Check cache first
		var branchData []byte
		var step kv.Step
		if entry, ok := w.cache.GetBranch(prefix); ok {
			branchData = entry.Data
		} else {
			var err error
			branchData, step, err = trieCtx.Branch(prefix)
			if err != nil {
				log.Debug(fmt.Sprintf("[%s][warmup] failed to get branch", w.logPrefix),
					"prefix", common.Bytes2Hex(prefix), "error", err)
			}
			w.cache.SetBranch(prefix, branchData, step)
		}

		// Branch data format: 2-byte touch map + 2-byte bitmap + per-child data
		if len(branchData) < 4 {
			break
		}

		if depth >= len(hashedKey) {
			break
		}
		nextNibble := int(hashedKey[depth])

		// Extract and prefetch account/storage addresses
		// Path nibble's cell will have stateHash cleared - always extract it
		// Memoized siblings are skipped
		cellAccounts, cellStorages := extractBranchCellAddresses(branchData, nextNibble)
		for _, addr := range cellAccounts {
			if _, ok := w.cache.GetAccount(addr); !ok {
				update, err := trieCtx.Account(addr)
				if err != nil {
					log.Debug(fmt.Sprintf("[%s][warmup] failed to get account", w.logPrefix),
						"addr", common.Bytes2Hex(addr), "error", err)
				}
				w.cache.SetAccount(addr, update)
			}
		}
		for _, addr := range cellStorages {
			if _, ok := w.cache.GetStorage(addr); !ok {
				update, err := trieCtx.Storage(addr)
				if err != nil {
					log.Debug(fmt.Sprintf("[%s][warmup] failed to get storage", w.logPrefix),
						"addr", common.Bytes2Hex(addr), "error", err)
				}
				w.cache.SetStorage(addr, update)
			}
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

// Wait waits for all warmup work to complete. Returns the warmup cache.
func (w *Warmuper) Wait() (*WarmupCache, error) {
	if !w.started.Load() || w.numWorkers <= 0 {
		return w.cache, nil
	}

	// Only close the channel once
	close(w.work)
	err := w.g.Wait()

	log.Debug(fmt.Sprintf("[%s][warmup] completed", w.logPrefix),
		"keys", common.PrettyCounter(int(w.keysProcessed.Load())),
		"maxDepth", w.maxDepth,
		"workers", w.numWorkers,
		"spent", time.Since(w.startTime),
	)

	return w.cache, err
}

// Reset closes the current warmuper and reinitializes it with a fresh state.
// This allows reusing the Warmuper instance without creating a new one.
func (w *Warmuper) Reset(ctx context.Context) {
	// Close existing resources
	if !w.closed.Load() {
		w.Close()
	}

	// Wait for workers to finish if they were started
	if w.started.Load() && w.g != nil {
		w.g.Wait()
	}

	// Reuse existing config
	cfg := WarmupConfig{
		Enabled:    w.numWorkers > 0,
		CtxFactory: w.ctxFactory,
		NumWorkers: w.numWorkers,
		MaxDepth:   w.maxDepth,
		LogPrefix:  w.logPrefix,
	}

	// Create a new warmuper and copy all fields individually to avoid copying locks
	w2 := NewWarmuper(ctx, cfg)

	// Copy all fields individually (avoid copying atomic values directly)
	w.ctx = w2.ctx
	w.cancel = w2.cancel
	w.ctxFactory = w2.ctxFactory
	w.maxDepth = w2.maxDepth
	w.numWorkers = w2.numWorkers
	w.logPrefix = w2.logPrefix
	w.work = w2.work
	w.cache = w2.cache
	w.g = w2.g
	w.started.Store(false)
	w.closed.Store(false)
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

// Cache returns the warmup cache. The cache is thread-safe and can be accessed
// while warmup is still in progress.
func (w *Warmuper) Cache() *WarmupCache {
	return w.cache
}

// Close cancels all warmup work and releases resources.
func (w *Warmuper) Close() {
	if w.closed.Swap(true) {
		return // Already closed
	}
	w.cancel()
	close(w.work)
}
