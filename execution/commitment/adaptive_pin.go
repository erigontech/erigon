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

package commitment

import (
	"context"
	"encoding/hex"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common/log/v3"
)

// AdaptivePinControllerConfig sets the policy knobs for the adaptive
// trunk-pin controller. Defaults target the SSTORE-bloat workload class
// (single contract dominating storage reads).
type AdaptivePinControllerConfig struct {
	PromoteThresholdMisses    uint64
	MaxPromotedContracts      int
	DemoteCooldownBlocks      int
	InitialViewBudgetBytes    int
	ExtensionBudgetBytes      int
	PerContractMaxBudgetBytes int
}

func DefaultAdaptivePinControllerConfig() AdaptivePinControllerConfig {
	return AdaptivePinControllerConfig{
		PromoteThresholdMisses:    100,
		MaxPromotedContracts:      8,
		DemoteCooldownBlocks:      5,
		InitialViewBudgetBytes:    4 << 20,
		ExtensionBudgetBytes:      8 << 20,
		PerContractMaxBudgetBytes: 64 << 20,
	}
}

// AdaptivePinController watches per-contract miss pressure on a
// BranchCache and decides which contracts to pin (with a sync initial
// view), grow (per-block extension), or demote (invalidate the pin
// set after sustained inactivity).
type AdaptivePinController struct {
	cache  *BranchCache
	cfg    AdaptivePinControllerConfig
	logger log.Logger

	misses sync.Map // [32]byte → *atomic.Uint64

	mu     sync.Mutex
	states map[[32]byte]*adaptiveContractState
}

// ParallelResolverFactory builds a fresh BatchBranchResolver for one
// OnBlockComplete call. release() is invoked after the controller is done
// with the resolver. Returning (nil, nil, err) makes the controller fall
// back to the serial-BFS path for this block.
type ParallelResolverFactory func() (resolve BatchBranchResolver, release func(), err error)

// DbBranchesProvider returns the MDBX-resident branch overlay for one
// contract — values shadow file values in the parallel preload's wave.
// Empty/nil result is valid (no overlay; resolver is authoritative).
type DbBranchesProvider func(contractHash []byte) map[string][]byte

type adaptiveContractState struct {
	contractHash     [32]byte
	promotedAtTxNum  uint64
	preload          *ContractTrunkPreload         // serial-BFS path (nil when parallel)
	parallel         *ContractTrunkPreloadParallel // parallel-wave-BFS path (nil when serial)
	coldBlocksInARow int
}

func (s *adaptiveContractState) pinnedTotal() int {
	if s.parallel != nil {
		return s.parallel.PinnedTotal()
	}
	return s.preload.PinnedTotal()
}

func (s *adaptiveContractState) usedBytes() int {
	if s.parallel != nil {
		return s.parallel.UsedBytes()
	}
	return s.preload.UsedBytes()
}

func (s *adaptiveContractState) queueRemaining() int {
	if s.parallel != nil {
		return s.parallel.QueueRemaining()
	}
	return s.preload.QueueRemaining()
}

func (s *adaptiveContractState) pinnedPrefixes() [][]byte {
	if s.parallel != nil {
		return s.parallel.PinnedPrefixes()
	}
	return s.preload.PinnedPrefixes()
}

func NewAdaptivePinController(cache *BranchCache, cfg AdaptivePinControllerConfig, logger log.Logger) *AdaptivePinController {
	if cfg.InitialViewBudgetBytes <= 0 {
		cfg.InitialViewBudgetBytes = 4 << 20
	}
	if cfg.ExtensionBudgetBytes <= 0 {
		cfg.ExtensionBudgetBytes = 8 << 20
	}
	if cfg.PerContractMaxBudgetBytes <= 0 {
		cfg.PerContractMaxBudgetBytes = 64 << 20
	}
	if cfg.MaxPromotedContracts <= 0 {
		cfg.MaxPromotedContracts = 8
	}
	if cfg.DemoteCooldownBlocks <= 0 {
		cfg.DemoteCooldownBlocks = 5
	}
	if cfg.PromoteThresholdMisses == 0 {
		cfg.PromoteThresholdMisses = 100
	}
	return &AdaptivePinController{
		cache:  cache,
		cfg:    cfg,
		logger: logger,
		states: make(map[[32]byte]*adaptiveContractState),
	}
}

// Bind installs the controller's miss-callback on the cache.
// Safe to call multiple times — replaces any prior callback.
func (c *AdaptivePinController) Bind() {
	c.cache.SetMissCallback(c.onCacheMiss)
}

// PerContractBudgetBytes is the per-contract pin ceiling; a dbBranches provider
// need never gather more than this since the preload can't pin beyond it.
func (c *AdaptivePinController) PerContractBudgetBytes() int {
	return c.cfg.PerContractMaxBudgetBytes
}

func (c *AdaptivePinController) onCacheMiss(prefix []byte) {
	hash, ok := ContractHashFromPrefix(prefix)
	if !ok {
		return
	}
	if v, ok := c.misses.Load(hash); ok {
		v.(*atomic.Uint64).Add(1)
		return
	}
	v, _ := c.misses.LoadOrStore(hash, new(atomic.Uint64))
	v.(*atomic.Uint64).Add(1)
}

// OnBlockComplete consumes the per-block miss snapshot and decides
// promotions, extensions, and demotions. Synchronous — preloads run
// inline so the new pin set is available for the next block's reads.
//
// The controller is aggregator-scoped (one owner across SharedDomains) so pin
// residency ages by block-access recency, not SD binds; the tx-scoped reader/
// factory/provider are therefore passed per call rather than stored, and c.mu
// serializes concurrent callers.
func (c *AdaptivePinController) OnBlockComplete(ctx context.Context, txNum uint64, reader CommitmentReader, factory ParallelResolverFactory, provider DbBranchesProvider) {
	misses := c.snapshotMisses()

	c.mu.Lock()
	defer c.mu.Unlock()

	// One factory call per block, shared across all contracts. nil falls back to serial.
	var parallelResolve BatchBranchResolver
	var releaseParallel func()
	if factory != nil {
		r, release, err := factory()
		if err != nil {
			c.warnf("[adaptive-pin] parallel resolver factory failed, falling back to serial", "err", err, "txNum", txNum)
		} else {
			parallelResolve = r
			releaseParallel = release
		}
	}
	if releaseParallel != nil {
		defer releaseParallel()
	}

	var promoted, extended, demoted int

	for hash, state := range c.states {
		n, hadMisses := misses[hash]
		if hadMisses && n > 0 {
			state.coldBlocksInARow = 0
			delete(misses, hash)
			if state.queueRemaining() > 0 && state.usedBytes() < c.cfg.PerContractMaxBudgetBytes {
				remaining := c.cfg.PerContractMaxBudgetBytes - state.usedBytes()
				step := c.cfg.ExtensionBudgetBytes
				if step > remaining {
					step = remaining
				}
				if err := c.runExtensionLocked(ctx, state, txNum, step, parallelResolve, reader, provider); err != nil {
					c.warnf("[adaptive-pin] extend failed", "hash", hex.EncodeToString(hash[:]), "err", err)
				} else {
					extended++
				}
			}
			continue
		}
		state.coldBlocksInARow++
		if state.coldBlocksInARow >= c.cfg.DemoteCooldownBlocks {
			c.demoteLocked(hash, state)
			delete(c.states, hash)
			demoted++
		}
	}

	if len(misses) > 0 && len(c.states) < c.cfg.MaxPromotedContracts {
		candidates := pickPromotionCandidates(misses, c.cfg.PromoteThresholdMisses, c.cfg.MaxPromotedContracts-len(c.states))
		for _, hash := range candidates {
			state, err := c.promoteLocked(ctx, hash, txNum, parallelResolve, reader, provider)
			if err != nil {
				c.warnf("[adaptive-pin] initial-view failed", "hash", hex.EncodeToString(hash[:]), "err", err)
				continue
			}
			c.states[hash] = state
			promoted++
		}
	}

	if promoted > 0 {
		mxAdaptivePromoted.AddUint64(uint64(promoted))
	}
	if extended > 0 {
		mxAdaptiveExtended.AddUint64(uint64(extended))
	}
	if demoted > 0 {
		mxAdaptiveDemoted.AddUint64(uint64(demoted))
	}
	mxAdaptiveActive.SetUint64(uint64(len(c.states)))
	c.cache.PublishMetrics()

	if c.logger != nil && (promoted+extended+demoted > 0 || len(c.states) > 0) {
		c.logger.Info("[adaptive-pin]",
			"txNum", txNum,
			"promoted_total", len(c.states),
			"promoted_this_block", promoted,
			"extended_this_block", extended,
			"demoted_this_block", demoted,
			"cache_pinned_total", c.cache.PinnedCount())
	}
}

func (c *AdaptivePinController) snapshotMisses() map[[32]byte]uint64 {
	out := make(map[[32]byte]uint64)
	c.misses.Range(func(k, v any) bool {
		hash := k.([32]byte)
		if n := v.(*atomic.Uint64).Swap(0); n > 0 {
			out[hash] = n
		}
		return true
	})
	return out
}

// demoteLocked: caller must hold c.mu.
func (c *AdaptivePinController) demoteLocked(hash [32]byte, state *adaptiveContractState) {
	for _, prefix := range state.pinnedPrefixes() {
		c.cache.Invalidate(prefix)
	}
	if c.logger != nil {
		c.logger.Info("[adaptive-pin] demoted",
			"hash", hex.EncodeToString(hash[:]),
			"pinned_was", state.pinnedTotal(),
			"used_mb_was", state.usedBytes()/(1<<20),
			"cold_blocks", state.coldBlocksInARow)
	}
}

// promoteLocked: caller must hold c.mu. On error the partial pin set is rolled back.
func (c *AdaptivePinController) promoteLocked(
	ctx context.Context,
	hash [32]byte,
	txNum uint64,
	parallelResolve BatchBranchResolver,
	reader CommitmentReader,
	provider DbBranchesProvider,
) (*adaptiveContractState, error) {
	if parallelResolve != nil {
		p, err := NewContractTrunkPreloadParallel(hash[:])
		if err != nil {
			return nil, err
		}
		p.pinTxNum = txNum
		var dbBranches map[string][]byte
		if provider != nil {
			dbBranches = provider(hash[:])
		}
		if _, _, err := p.Run(c.cfg.InitialViewBudgetBytes, dbBranches, parallelResolve, c.cache, c.logger); err != nil {
			for _, prefix := range p.PinnedPrefixes() {
				c.cache.Invalidate(prefix)
			}
			return nil, err
		}
		return &adaptiveContractState{
			contractHash:    hash,
			promotedAtTxNum: txNum,
			parallel:        p,
		}, nil
	}
	p, err := NewContractTrunkPreload(hash[:])
	if err != nil {
		return nil, err
	}
	p.pinTxNum = txNum
	if _, _, err := p.Run(c.cfg.InitialViewBudgetBytes, reader, c.cache, c.logger); err != nil {
		for _, prefix := range p.PinnedPrefixes() {
			c.cache.Invalidate(prefix)
		}
		return nil, err
	}
	return &adaptiveContractState{
		contractHash:    hash,
		promotedAtTxNum: txNum,
		preload:         p,
	}, nil
}

// runExtensionLocked: caller must hold c.mu. Uses the saved state's mode
// (parallel vs serial); a serial state with a parallel resolver available
// keeps using serial — switching mid-contract would lose the queue position.
func (c *AdaptivePinController) runExtensionLocked(
	ctx context.Context,
	state *adaptiveContractState,
	txNum uint64,
	stepBudget int,
	parallelResolve BatchBranchResolver,
	reader CommitmentReader,
	provider DbBranchesProvider,
) error {
	if state.parallel != nil {
		if parallelResolve == nil {
			return nil
		}
		var dbBranches map[string][]byte
		if provider != nil {
			dbBranches = provider(state.contractHash[:])
		}
		state.parallel.pinTxNum = txNum
		_, _, err := state.parallel.Run(stepBudget, dbBranches, parallelResolve, c.cache, c.logger)
		return err
	}
	state.preload.pinTxNum = txNum
	_, _, err := state.preload.Run(stepBudget, reader, c.cache, c.logger)
	return err
}

func pickPromotionCandidates(misses map[[32]byte]uint64, threshold uint64, maxN int) [][32]byte {
	if maxN <= 0 {
		return nil
	}
	type cand struct {
		hash [32]byte
		n    uint64
	}
	var pool []cand
	for h, n := range misses {
		if n >= threshold {
			pool = append(pool, cand{h, n})
		}
	}
	if len(pool) > maxN {
		for i := 0; i < maxN; i++ {
			best := i
			for j := i + 1; j < len(pool); j++ {
				if pool[j].n > pool[best].n {
					best = j
				}
			}
			pool[i], pool[best] = pool[best], pool[i]
		}
		pool = pool[:maxN]
	}
	out := make([][32]byte, len(pool))
	for i, c := range pool {
		out[i] = c.hash
	}
	return out
}

func (c *AdaptivePinController) warnf(msg string, kv ...any) {
	if c.logger != nil {
		c.logger.Warn(msg, kv...)
	}
}

var _ = context.Background // reserved for cancellation of in-flight preloads
