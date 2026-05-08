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
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// Warmer outcome counters — measurement scaffolding to size the
// hit-rate of warmer branch reads. Hit means branchFromCacheOrDB
// returned >= 4 bytes (a real branch). Empty means it returned
// nothing or too short to parse (no branch at this depth in the
// on-disk trie). Used to bound the value of bypassing the xorfilter
// for the warmer specifically — high hit ratio implies the filter
// is mostly overhead in this call path.
var (
	warmerBranchHitCount   atomic.Uint64
	warmerBranchEmptyCount atomic.Uint64
)

// WarmerBranchOutcomeStats returns process-cumulative counts of
// branch-read outcomes from the warmer's depth walk. To get a
// per-block delta, snapshot before/after.
func WarmerBranchOutcomeStats() (hit, empty uint64) {
	return warmerBranchHitCount.Load(), warmerBranchEmptyCount.Load()
}

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
	}
}

// branchFromCacheOrDB reads branch data via ctx.Branch. The Go-side
// warmup cache that previously sat above this has been removed; the
// aggregator-scope BranchCache (reached through SD.GetLatest) is the
// only branch cache. The function is kept as a thin wrapper to
// preserve the warmer-side error handling shape.
func (w *Warmuper) branchFromCacheOrDB(trieCtx PatriciaContext, prefix []byte) ([]byte, error) {
	branchData, _, err := trieCtx.Branch(prefix)
	return branchData, err
}

// Start initializes and starts the warmup workers.
func (w *Warmuper) Start() {
	if w.started.Swap(true) {
		return // Already started
	}
	if w.numWorkers <= 0 {
		return
	}

	w.work = make(chan warmupWorkItem, w.numWorkers*64)
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
		prefix := nibbles.HexToCompact(hashedKey[:depth])

		// Check cache first, then fall back to DB
		branchData, err := w.branchFromCacheOrDB(trieCtx, prefix)
		if err != nil {
			log.Debug(fmt.Sprintf("[%s][warmup] failed to get branch", w.logPrefix),
				"prefix", common.Bytes2Hex(prefix), "error", err)
		}

		// Branch data format: 2-byte touch map + 2-byte bitmap + per-child data
		if len(branchData) < 4 {
			warmerBranchEmptyCount.Add(1)
			break
		}
		warmerBranchHitCount.Add(1)

		if depth >= len(hashedKey) {
			break
		}
		nextNibble := int(hashedKey[depth])

		// Account/storage prefetch removed: the warmer's scope is branch
		// warm-up only. If a leaf hash needs underlying account/storage
		// data during fold, that data is either (a) memoized as the
		// cell's stateHash, (b) carried in the Updates buffer from the
		// executor, or (c) faulted on the trie's own slow path —
		// signalling a memoization gap that wants fixing at the trie
		// layer, not papered over by a separate prefetcher. Counters
		// disk_sto / disk_acc on the [commitment][cache-fp] log line
		// expose any such fall-through.

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

		// If the child cell is a leaf (holds a plain key), there is no
		// branch below it — stop walking. Without this check the warmer
		// pays one extra recsplit + xorfilter lookup per leaf-terminating
		// path, all of which return empty. Mirrors what unfold does
		// implicitly by traversing the actual trie structure.
		if cellFields(fieldBits)&(fieldAccountAddr|fieldStorageAddr) != 0 {
			break
		}

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
	// Blocking By-Design!
	// Speed of system is equal to speed of facing all page-faults during
	// Or warmapers face them or main thread
	// It means doesn't make much sense to unblock main thread if all Warmupers are loaded
	// Anyway main thread can't run ahead of Warmupers (there are page-faults which will stop him)
	select {
	case w.work <- warmupWorkItem{hashedKey: hashedKey, startDepth: startDepth}:
	case <-w.ctx.Done():
	}
}

// Wait waits for all warmup work to complete.
func (w *Warmuper) Wait() error {
	if !w.started.Load() || w.numWorkers <= 0 {
		return nil
	}
	w.Close()
	w.g.Wait()
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
func (w *Warmuper) Close() {
	if w.closed.Swap(true) {
		return // Already closed
	}
	w.cancel()
	if w.work != nil {
		close(w.work)
	}
}
