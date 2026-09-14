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
	"cmp"
	"context"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
)

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
		MaxPromotedContracts:      4,
		DemoteCooldownBlocks:      5,
		InitialViewBudgetBytes:    4 * 1024 * 1024,
		ExtensionBudgetBytes:      8 * 1024 * 1024,
		PerContractMaxBudgetBytes: 32 * 1024 * 1024,
	}
}

type AdaptivePinController struct {
	cache  *BranchCache
	cfg    AdaptivePinControllerConfig
	logger log.Logger

	misses sync.Map // [32]byte → *atomic.Uint64

	mu     sync.Mutex
	states map[[32]byte]*adaptiveContractState
}

type DbBranchesProvider func(contractHash []byte) map[string][]byte

type adaptiveContractState struct {
	contractHash     [32]byte
	promotedAtTxNum  uint64
	parallel         *ContractTrunkPreloadParallel
	coldBlocksInARow int
}

func NewAdaptivePinController(cache *BranchCache, cfg AdaptivePinControllerConfig, logger log.Logger) *AdaptivePinController {
	def := DefaultAdaptivePinControllerConfig()
	cfg.InitialViewBudgetBytes = cmp.Or(cfg.InitialViewBudgetBytes, def.InitialViewBudgetBytes)
	cfg.ExtensionBudgetBytes = cmp.Or(cfg.ExtensionBudgetBytes, def.ExtensionBudgetBytes)
	cfg.PerContractMaxBudgetBytes = cmp.Or(cfg.PerContractMaxBudgetBytes, def.PerContractMaxBudgetBytes)
	cfg.MaxPromotedContracts = cmp.Or(cfg.MaxPromotedContracts, def.MaxPromotedContracts)
	cfg.DemoteCooldownBlocks = cmp.Or(cfg.DemoteCooldownBlocks, def.DemoteCooldownBlocks)
	cfg.PromoteThresholdMisses = cmp.Or(cfg.PromoteThresholdMisses, def.PromoteThresholdMisses)
	return &AdaptivePinController{
		cache:  cache,
		cfg:    cfg,
		logger: logger,
		states: make(map[[32]byte]*adaptiveContractState),
	}
}

// Idempotent.
func (c *AdaptivePinController) Bind() {
	c.cache.SetMissCallback(c.onCacheMiss)
}

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

// Synchronous; aggregator-scoped (shared across tx-scoped binds via c.mu).
func (c *AdaptivePinController) OnBlockComplete(ctx context.Context, txNum uint64, resolve BatchBranchResolver, provider DbBranchesProvider) {
	misses := c.snapshotMisses()

	c.mu.Lock()
	defer c.mu.Unlock()

	var promoted, extended, demoted int

	for hash, state := range c.states {
		n, hadMisses := misses[hash]
		if hadMisses && n > 0 {
			state.coldBlocksInARow = 0
			delete(misses, hash)
			if state.parallel.QueueRemaining() > 0 && state.parallel.UsedBytes() < c.cfg.PerContractMaxBudgetBytes {
				remaining := c.cfg.PerContractMaxBudgetBytes - state.parallel.UsedBytes()
				step := min(c.cfg.ExtensionBudgetBytes, remaining)
				if err := c.runExtensionLocked(ctx, state, txNum, step, resolve, provider); err != nil {
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
			state, err := c.promoteLocked(ctx, hash, txNum, resolve, provider)
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
	for _, prefix := range state.parallel.PinnedPrefixes() {
		c.cache.Invalidate(prefix)
	}
	if c.logger != nil {
		c.logger.Info("[adaptive-pin] demoted",
			"hash", hex.EncodeToString(hash[:]),
			"pinned_was", state.parallel.PinnedTotal(),
			"used_mb_was", state.parallel.UsedBytes()/(1<<20),
			"cold_blocks", state.coldBlocksInARow)
	}
}

// promoteLocked: caller must hold c.mu. On error the partial pin set is rolled back.
func (c *AdaptivePinController) promoteLocked(
	ctx context.Context,
	hash [32]byte,
	txNum uint64,
	resolve BatchBranchResolver,
	provider DbBranchesProvider,
) (*adaptiveContractState, error) {
	p, err := NewContractTrunkPreloadParallel(hash[:])
	if err != nil {
		return nil, err
	}
	p.pinTxNum = txNum
	var dbBranches map[string][]byte
	if provider != nil {
		dbBranches = provider(hash[:])
	}
	started := time.Now()
	if _, _, err := p.Run(c.cfg.InitialViewBudgetBytes, dbBranches, resolve, c.cache, c.logger); err != nil {
		recordPreload(started, 0)
		for _, prefix := range p.PinnedPrefixes() {
			c.cache.Invalidate(prefix)
		}
		return nil, err
	}
	recordPreload(started, p.usedBytes)
	return &adaptiveContractState{
		contractHash:    hash,
		promotedAtTxNum: txNum,
		parallel:        p,
	}, nil
}

// Caller must hold c.mu.
func (c *AdaptivePinController) runExtensionLocked(
	ctx context.Context,
	state *adaptiveContractState,
	txNum uint64,
	stepBudget int,
	resolve BatchBranchResolver,
	provider DbBranchesProvider,
) error {
	var dbBranches map[string][]byte
	if provider != nil {
		dbBranches = provider(state.contractHash[:])
	}
	state.parallel.pinTxNum = txNum
	before, started := state.parallel.usedBytes, time.Now()
	_, _, err := state.parallel.Run(stepBudget, dbBranches, resolve, c.cache, c.logger)
	recordPreload(started, state.parallel.usedBytes-before)
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
		for i := range maxN {
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
