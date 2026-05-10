// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package commitment

import (
	"context"
	"encoding/hex"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common/log/v3"
)

// AdaptivePinControllerConfig sets the policy knobs for the adaptive
// trunk-pin controller. Defaults are tuned for the SSTORE-bloat
// workload class (single contract dominating storage reads); other
// workload classes may want different values.
type AdaptivePinControllerConfig struct {
	// PromoteThresholdMisses — minimum cache misses (per block) for a
	// contract to be promoted. Default 100.
	PromoteThresholdMisses uint64

	// MaxPromotedContracts — hard cap on simultaneously-pinned
	// contracts. Bounds total pin RAM = MaxPromotedContracts ×
	// PerContractMaxBudgetBytes. Default 8 (× 64 MiB = 512 MiB max).
	MaxPromotedContracts int

	// DemoteCooldownBlocks — number of consecutive blocks with zero
	// misses for a promoted contract before it gets demoted (and its
	// pin set invalidated). Default 5.
	DemoteCooldownBlocks int

	// InitialViewBudgetBytes — RAM budget for the synchronous
	// initial-view preload at promotion. Default 4 MiB (covers
	// d=64-67 with headroom; ~1.4 s on cold disk).
	InitialViewBudgetBytes int

	// ExtensionBudgetBytes — RAM budget for the per-block extension
	// step on already-promoted contracts. Default 8 MiB (~25 K
	// branches per block at typical entry cost).
	ExtensionBudgetBytes int

	// PerContractMaxBudgetBytes — hard ceiling on the cumulative pin
	// budget per contract. Extensions stop when this is reached even
	// if the BFS queue isn't empty. Default 64 MiB (matches the
	// per-contract max enforced in the env hook).
	PerContractMaxBudgetBytes int
}

// DefaultAdaptivePinControllerConfig returns the production defaults.
// Conservative across the board so default-on shipping doesn't pin
// memory speculatively.
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
// view) and grow (per-block extension) or demote (invalidate the pin
// set after sustained inactivity).
//
// Lifecycle:
//   - Construct with NewAdaptivePinController, wire to a cache via Bind
//   - On every triple-miss, the cache calls back; the controller
//     records a per-contract miss count
//   - The host (SD or stage loop) calls OnBlockComplete at block
//     boundaries with a CommitmentReader factory; the controller
//     then promotes / extends / demotes based on the per-block
//     miss snapshot
type AdaptivePinController struct {
	cache  *BranchCache
	cfg    AdaptivePinControllerConfig
	logger log.Logger

	// misses holds per-contract miss counts since last OnBlockComplete.
	// sync.Map is appropriate: writes are frequent (every triple-miss),
	// reads happen once per block boundary.
	misses sync.Map // [32]byte → *atomic.Uint64

	// states holds per-promoted-contract bookkeeping. Protected by mu.
	mu     sync.Mutex
	states map[[32]byte]*adaptiveContractState
}

type adaptiveContractState struct {
	contractHash     [32]byte
	promotedAtBlock  uint64
	preload          *ContractTrunkPreload
	coldBlocksInARow int
}

// NewAdaptivePinController constructs a controller bound to the given
// cache. Use Bind to install the cache miss-callback (separate so a
// caller can keep a controller for telemetry without wiring it into
// the read path).
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

// Bind installs the controller's miss-callback on the cache. After
// Bind, every triple-miss attributable to a storage-trunk prefix
// (length >= 33 B) is counted toward the corresponding contract.
//
// Safe to call multiple times — Bind replaces any prior callback.
// Call SetMissCallback(nil) on the cache directly to unbind.
func (c *AdaptivePinController) Bind() {
	c.cache.SetMissCallback(c.onCacheMiss)
}

func (c *AdaptivePinController) onCacheMiss(prefix []byte) {
	hash, ok := ContractHashFromPrefix(prefix)
	if !ok {
		return
	}
	v, _ := c.misses.LoadOrStore(hash, new(atomic.Uint64))
	v.(*atomic.Uint64).Add(1)
}

// OnBlockComplete consumes the per-block miss snapshot and decides
// promotions, extensions, and demotions. Called by the host at block
// boundaries (after SD.Flush). The reader is the CommitmentReader
// for the just-committed state, used by initial-view preload and
// per-block extension.
//
// Synchronous: initial-view preloads run inline so the new pin set
// is available for the NEXT block's reads. Extensions also run
// inline; sized so per-block work fits within typical inter-block
// idle (~5 s of preload work for ExtensionBudgetBytes=8 MiB).
//
// Logs a [adaptive-pin] line per block when any state changes or
// promoted contracts exist.
func (c *AdaptivePinController) OnBlockComplete(ctx context.Context, blockNum uint64, reader CommitmentReader) {
	misses := c.snapshotMisses()

	c.mu.Lock()
	defer c.mu.Unlock()

	var promoted, extended, demoted int

	// Already-promoted contracts: extend on hot, demote on cold.
	for hash, state := range c.states {
		n, hadMisses := misses[hash]
		if hadMisses && n > 0 {
			state.coldBlocksInARow = 0
			delete(misses, hash)
			// Extend if budget remains and queue not empty.
			if state.preload.QueueRemaining() > 0 && state.preload.UsedBytes() < c.cfg.PerContractMaxBudgetBytes {
				remaining := c.cfg.PerContractMaxBudgetBytes - state.preload.UsedBytes()
				step := c.cfg.ExtensionBudgetBytes
				if step > remaining {
					step = remaining
				}
				if _, _, err := state.preload.Run(step, reader, c.cache, c.logger); err != nil {
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

	// New promotion candidates: contracts whose miss count crossed the
	// threshold. Cap at MaxPromotedContracts; pick the highest-miss
	// candidates first (greedy, simple, sufficient for v1).
	if len(misses) > 0 && len(c.states) < c.cfg.MaxPromotedContracts {
		candidates := pickPromotionCandidates(misses, c.cfg.PromoteThresholdMisses, c.cfg.MaxPromotedContracts-len(c.states))
		for _, hash := range candidates {
			p, err := NewContractTrunkPreload(hash[:])
			if err != nil {
				continue
			}
			if _, _, err := p.Run(c.cfg.InitialViewBudgetBytes, reader, c.cache, c.logger); err != nil {
				c.warnf("[adaptive-pin] initial-view failed", "hash", hex.EncodeToString(hash[:]), "err", err)
				// Roll back partial pin so we don't leak entries
				// for a contract that won't be in c.states.
				for _, prefix := range p.PinnedPrefixes() {
					c.cache.Invalidate(prefix)
				}
				continue
			}
			c.states[hash] = &adaptiveContractState{
				contractHash:    hash,
				promotedAtBlock: blockNum,
				preload:         p,
			}
			promoted++
		}
	}

	if c.logger != nil && (promoted+extended+demoted > 0 || len(c.states) > 0) {
		c.logger.Info("[adaptive-pin]",
			"block", blockNum,
			"promoted_total", len(c.states),
			"promoted_this_block", promoted,
			"extended_this_block", extended,
			"demoted_this_block", demoted,
			"cache_pinned_total", c.cache.PinnedCount())
	}
}

// snapshotMisses atomically reads + zeros every per-contract miss
// counter. Called once per OnBlockComplete.
func (c *AdaptivePinController) snapshotMisses() map[[32]byte]uint64 {
	out := make(map[[32]byte]uint64)
	c.misses.Range(func(k, v any) bool {
		hash := k.([32]byte)
		n := v.(*atomic.Uint64).Swap(0)
		if n > 0 {
			out[hash] = n
		}
		return true
	})
	return out
}

// demoteLocked invalidates every pinned prefix for the contract.
// Caller must hold c.mu.
func (c *AdaptivePinController) demoteLocked(hash [32]byte, state *adaptiveContractState) {
	for _, prefix := range state.preload.PinnedPrefixes() {
		c.cache.Invalidate(prefix)
	}
	if c.logger != nil {
		c.logger.Info("[adaptive-pin] demoted",
			"hash", hex.EncodeToString(hash[:]),
			"pinned_was", state.preload.PinnedTotal(),
			"used_mb_was", state.preload.UsedBytes()/(1<<20),
			"cold_blocks", state.coldBlocksInARow)
	}
}

// PromotedContracts returns the hashes of currently-pinned contracts.
// Snapshot — the slice may be stale by the time the caller uses it.
// Used by metrics + debug diagnostics.
func (c *AdaptivePinController) PromotedContracts() [][32]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][32]byte, 0, len(c.states))
	for h := range c.states {
		out = append(out, h)
	}
	return out
}

// pickPromotionCandidates selects up to maxN contract hashes from
// misses whose count exceeds threshold, preferring highest-miss first.
// Linear scan — adequate for the small N we expect (typically ≤16
// candidates per block; sort is cheap).
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
		// Partial sort: O(N×maxN) for small maxN; avoids importing sort.
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

// ctx unused for now; reserved for future cancellation of in-flight
// preloads (R3 bulk preload + cancellable extension).
var _ = context.Background
