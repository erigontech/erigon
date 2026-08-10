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
	"bytes"
	"encoding/hex"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/cache"
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
		MaxPromotedContracts:      4,
		DemoteCooldownBlocks:      5,
		InitialViewBudgetBytes:    4 * 1024 * 1024,
		ExtensionBudgetBytes:      8 * 1024 * 1024,
		PerContractMaxBudgetBytes: 32 * 1024 * 1024,
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

	mu              sync.Mutex
	states          map[[32]byte]*adaptiveContractState
	cacheClearEpoch uint64
}

// ParallelResolverFactory builds a fresh BatchBranchResolver for one
// PlanBlock call. release() is invoked after the controller is done
// with the resolver. Returning (nil, nil, err) makes the controller fall
// back to the serial-BFS path for this block.
type ParallelResolverFactory func() (resolve BatchBranchResolver, release func(), err error)

// DbBranchesProvider returns the MDBX-resident branch overlay for one
// contract — values shadow file values in the parallel preload's wave.
// Empty/nil result is valid (no overlay; resolver is authoritative).
type DbBranchesProvider func(contractHash []byte) map[string][]byte

type adaptiveCacheMutation struct {
	prefix     []byte
	value      []byte
	step       uint64
	invalidate bool
}

type adaptiveCacheMutations struct {
	entries []adaptiveCacheMutation
}

func (m *adaptiveCacheMutations) PinEntry(prefix, value []byte, step uint64) {
	m.entries = append(m.entries, adaptiveCacheMutation{
		prefix: bytes.Clone(prefix),
		value:  bytes.Clone(value),
		step:   step,
	})
}

func (m *adaptiveCacheMutations) Invalidate(prefix []byte) {
	m.entries = append(m.entries, adaptiveCacheMutation{
		prefix:     bytes.Clone(prefix),
		invalidate: true,
	})
}

func (m *adaptiveCacheMutations) apply(cache *BranchCache) {
	for i := range m.entries {
		entry := &m.entries[i]
		if entry.invalidate {
			cache.Invalidate(entry.prefix)
			continue
		}
		cache.PinEntry(entry.prefix, entry.value, entry.step)
	}
}

// AdaptivePinPlan holds controller state and cache mutations derived from an
// uncommitted transaction. Commit or Abort must be called exactly once.
type AdaptivePinPlan struct {
	controller     *AdaptivePinController
	mutations      adaptiveCacheMutations
	previousStates map[[32]byte]*adaptiveContractState
	observedMisses map[[32]byte]uint64
	// The source token and clear epoch must still match when publication
	// applies the plan. Otherwise its branches came from obsolete backing state.
	source          cache.GenerationView
	cacheClearEpoch uint64
	applied         bool
	txNum           uint64
	promoted        int
	extended        int
	demoted         int
}

type adaptiveContractState struct {
	contractHash     [32]byte
	preload          *ContractTrunkPreload         // serial-BFS path (nil when parallel)
	parallel         *ContractTrunkPreloadParallel // parallel-wave-BFS path (nil when serial)
	coldBlocksInARow int
}

func cloneAdaptiveStateHeaders(states map[[32]byte]*adaptiveContractState) map[[32]byte]*adaptiveContractState {
	cloned := make(map[[32]byte]*adaptiveContractState, len(states))
	for hash, state := range states {
		stateCopy := *state
		cloned[hash] = &stateCopy
	}
	return cloned
}

func cloneByteSlices(values [][]byte) [][]byte {
	cloned := make([][]byte, len(values))
	for i := range values {
		cloned[i] = bytes.Clone(values[i])
	}
	return cloned
}

func cloneSerialPreload(preload *ContractTrunkPreload) *ContractTrunkPreload {
	cloned := *preload
	cloned.contractHash = bytes.Clone(preload.contractHash)
	cloned.queue = make([]pathDepth, len(preload.queue))
	for i := range preload.queue {
		cloned.queue[i] = pathDepth{
			path:  bytes.Clone(preload.queue[i].path),
			depth: preload.queue[i].depth,
		}
	}
	cloned.pinnedPrefixes = cloneByteSlices(preload.pinnedPrefixes)
	return &cloned
}

func clonePathKeys(values []pathKey) []pathKey {
	cloned := make([]pathKey, len(values))
	for i := range values {
		cloned[i] = pathKey{
			path: bytes.Clone(values[i].path),
			key:  bytes.Clone(values[i].key),
		}
	}
	return cloned
}

func cloneParallelPreload(preload *ContractTrunkPreloadParallel) *ContractTrunkPreloadParallel {
	cloned := *preload
	cloned.contractHash = bytes.Clone(preload.contractHash)
	cloned.frontier = clonePathKeys(preload.frontier)
	cloned.pendingChildren = clonePathKeys(preload.pendingChildren)
	cloned.pinnedPrefixes = cloneByteSlices(preload.pinnedPrefixes)
	cloned.scratchDbHits = nil
	cloned.scratchDbVals = nil
	cloned.scratchFileMiss = nil
	return &cloned
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
	def := DefaultAdaptivePinControllerConfig()
	if cfg.InitialViewBudgetBytes <= 0 {
		cfg.InitialViewBudgetBytes = def.InitialViewBudgetBytes
	}
	if cfg.ExtensionBudgetBytes <= 0 {
		cfg.ExtensionBudgetBytes = def.ExtensionBudgetBytes
	}
	if cfg.PerContractMaxBudgetBytes <= 0 {
		cfg.PerContractMaxBudgetBytes = def.PerContractMaxBudgetBytes
	}
	if cfg.MaxPromotedContracts <= 0 {
		cfg.MaxPromotedContracts = def.MaxPromotedContracts
	}
	if cfg.DemoteCooldownBlocks <= 0 {
		cfg.DemoteCooldownBlocks = def.DemoteCooldownBlocks
	}
	if cfg.PromoteThresholdMisses == 0 {
		cfg.PromoteThresholdMisses = def.PromoteThresholdMisses
	}
	return &AdaptivePinController{
		cache:           cache,
		cfg:             cfg,
		logger:          logger,
		states:          make(map[[32]byte]*adaptiveContractState),
		cacheClearEpoch: cache.clearEpoch.Load(),
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

// Reset forgets residency state after BranchCache is cleared. Without this,
// the controller would treat removed pins as live and wait for their normal
// demotion before promoting them again.
func (c *AdaptivePinController) Reset() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetLocked()
}

func (c *AdaptivePinController) resetLocked() {
	c.states = make(map[[32]byte]*adaptiveContractState)
	c.misses.Range(func(key, _ any) bool {
		c.misses.Delete(key)
		return true
	})
	c.cacheClearEpoch = c.cache.clearEpoch.Load()
	mxAdaptiveActive.SetUint64(0)
}

func (c *AdaptivePinController) syncCacheClearLocked() {
	epoch := c.cache.clearEpoch.Load()
	if epoch == c.cacheClearEpoch {
		return
	}
	c.states = make(map[[32]byte]*adaptiveContractState)
	c.cacheClearEpoch = epoch
	mxAdaptiveActive.SetUint64(0)
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

// PlanBlock computes promotions, extensions, and demotions from the
// uncommitted transaction without changing BranchCache. The returned plan
// keeps controller updates serialized until Commit or Abort. Publication
// discards it if sourceGeneration or the cache clear epoch changed meanwhile.
// An already-stale source returns no plan and leaves its misses for a fresh
// transaction instead of doing work that cannot be published.
func (c *AdaptivePinController) PlanBlock(
	txNum uint64,
	sourceGeneration cache.Generation,
	reader CommitmentReader,
	factory ParallelResolverFactory,
	provider DbBranchesProvider,
) *AdaptivePinPlan {
	c.mu.Lock()
	c.syncCacheClearLocked()
	source := c.cache.generation.View(sourceGeneration)
	if !source.Current() {
		c.mu.Unlock()
		return nil
	}
	previousStates := c.states
	c.states = cloneAdaptiveStateHeaders(previousStates)
	misses := c.snapshotMisses()
	observedMisses := make(map[[32]byte]uint64, len(misses))
	maps.Copy(observedMisses, misses)
	plan := &AdaptivePinPlan{
		controller:      c,
		previousStates:  previousStates,
		observedMisses:  observedMisses,
		source:          source,
		cacheClearEpoch: c.cacheClearEpoch,
		txNum:           txNum,
	}

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

	for hash, state := range c.states {
		n, hadMisses := misses[hash]
		if hadMisses && n > 0 {
			state.coldBlocksInARow = 0
			delete(misses, hash)
			if state.queueRemaining() > 0 && state.usedBytes() < c.cfg.PerContractMaxBudgetBytes {
				remaining := c.cfg.PerContractMaxBudgetBytes - state.usedBytes()
				step := min(c.cfg.ExtensionBudgetBytes, remaining)
				if err := c.runExtensionLocked(state, step, parallelResolve, reader, provider, &plan.mutations); err != nil {
					c.warnf("[adaptive-pin] extend failed", "hash", hex.EncodeToString(hash[:]), "err", err)
				} else {
					plan.extended++
				}
			}
			continue
		}
		state.coldBlocksInARow++
		if state.coldBlocksInARow >= c.cfg.DemoteCooldownBlocks {
			c.demoteLocked(state, &plan.mutations)
			delete(c.states, hash)
			plan.demoted++
		}
	}

	if len(misses) > 0 && len(c.states) < c.cfg.MaxPromotedContracts {
		candidates := pickPromotionCandidates(misses, c.cfg.PromoteThresholdMisses, c.cfg.MaxPromotedContracts-len(c.states))
		for _, hash := range candidates {
			state, err := c.promoteLocked(hash, parallelResolve, reader, provider, &plan.mutations)
			if err != nil {
				c.warnf("[adaptive-pin] initial-view failed", "hash", hex.EncodeToString(hash[:]), "err", err)
				continue
			}
			c.states[hash] = state
			plan.promoted++
		}
	}

	return plan
}

func (p *AdaptivePinPlan) apply(publication *cache.GenerationPublication) {
	if p == nil || p.controller == nil {
		return
	}
	if p.cacheClearEpoch != p.controller.cache.clearEpoch.Load() || !publication.StartedFrom(p.source) {
		return
	}
	p.mutations.apply(p.controller.cache)
	p.applied = true
}

// Commit accepts the planned controller state after its cache mutations have
// been published with the database transaction.
func (p *AdaptivePinPlan) Commit() {
	if p == nil || p.controller == nil {
		return
	}
	c := p.controller
	if !p.applied || p.cacheClearEpoch != c.cache.clearEpoch.Load() {
		p.discard()
		return
	}
	if p.promoted > 0 {
		mxAdaptivePromoted.AddUint64(uint64(p.promoted))
	}
	if p.extended > 0 {
		mxAdaptiveExtended.AddUint64(uint64(p.extended))
	}
	if p.demoted > 0 {
		mxAdaptiveDemoted.AddUint64(uint64(p.demoted))
	}
	mxAdaptiveActive.SetUint64(uint64(len(c.states)))
	c.cache.PublishMetrics()

	if c.logger != nil && (p.promoted+p.extended+p.demoted > 0 || len(c.states) > 0) {
		c.logger.Info("[adaptive-pin]",
			"txNum", p.txNum,
			"promoted_total", len(c.states),
			"promoted_this_block", p.promoted,
			"extended_this_block", p.extended,
			"demoted_this_block", p.demoted,
			"cache_pinned_total", c.cache.PinnedCount())
	}
	p.controller = nil
	c.mu.Unlock()
}

// Abort restores the controller state and miss counters from before PlanBlock.
// No BranchCache rollback is needed because planning only records mutations.
func (p *AdaptivePinPlan) Abort() {
	if p == nil || p.controller == nil {
		return
	}
	p.discard()
}

func (p *AdaptivePinPlan) discard() {
	c := p.controller
	epoch := c.cache.clearEpoch.Load()
	if epoch == p.cacheClearEpoch {
		c.states = p.previousStates
	} else {
		c.states = make(map[[32]byte]*adaptiveContractState)
		c.cacheClearEpoch = epoch
	}
	for hash, count := range p.observedMisses {
		value, _ := c.misses.LoadOrStore(hash, new(atomic.Uint64))
		value.(*atomic.Uint64).Add(count)
	}
	mxAdaptiveActive.SetUint64(uint64(len(c.states)))
	p.controller = nil
	c.mu.Unlock()
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
func (c *AdaptivePinController) demoteLocked(state *adaptiveContractState, mutations *adaptiveCacheMutations) {
	for _, prefix := range state.pinnedPrefixes() {
		mutations.Invalidate(prefix)
	}
}

// promoteLocked: caller must hold c.mu.
func (c *AdaptivePinController) promoteLocked(
	hash [32]byte,
	parallelResolve BatchBranchResolver,
	reader CommitmentReader,
	provider DbBranchesProvider,
	mutations *adaptiveCacheMutations,
) (*adaptiveContractState, error) {
	checkpoint := len(mutations.entries)
	if parallelResolve != nil {
		p, err := NewContractTrunkPreloadParallel(hash[:])
		if err != nil {
			return nil, err
		}
		var dbBranches map[string][]byte
		if provider != nil {
			dbBranches = provider(hash[:])
		}
		started := time.Now()
		if _, _, err := p.Run(c.cfg.InitialViewBudgetBytes, dbBranches, parallelResolve, mutations, c.logger); err != nil {
			recordPreload(started, 0)
			mutations.entries = mutations.entries[:checkpoint]
			return nil, err
		}
		recordPreload(started, p.usedBytes)
		return &adaptiveContractState{
			contractHash: hash,
			parallel:     p,
		}, nil
	}
	p, err := NewContractTrunkPreload(hash[:])
	if err != nil {
		return nil, err
	}
	started := time.Now()
	if _, _, err := p.Run(c.cfg.InitialViewBudgetBytes, reader, mutations, c.logger); err != nil {
		recordPreload(started, 0)
		mutations.entries = mutations.entries[:checkpoint]
		return nil, err
	}
	recordPreload(started, p.usedBytes)
	return &adaptiveContractState{
		contractHash: hash,
		preload:      p,
	}, nil
}

// runExtensionLocked: caller must hold c.mu. Uses the saved state's mode
// (parallel vs serial); a serial state with a parallel resolver available
// keeps using serial — switching mid-contract would lose the queue position.
func (c *AdaptivePinController) runExtensionLocked(
	state *adaptiveContractState,
	stepBudget int,
	parallelResolve BatchBranchResolver,
	reader CommitmentReader,
	provider DbBranchesProvider,
	mutations *adaptiveCacheMutations,
) error {
	if state.parallel != nil {
		if parallelResolve == nil {
			return nil
		}
		state.parallel = cloneParallelPreload(state.parallel)
		var dbBranches map[string][]byte
		if provider != nil {
			dbBranches = provider(state.contractHash[:])
		}
		before, started := state.parallel.usedBytes, time.Now()
		_, _, err := state.parallel.Run(stepBudget, dbBranches, parallelResolve, mutations, c.logger)
		recordPreload(started, state.parallel.usedBytes-before)
		return err
	}
	state.preload = cloneSerialPreload(state.preload)
	before, started := state.preload.usedBytes, time.Now()
	_, _, err := state.preload.Run(stepBudget, reader, mutations, c.logger)
	recordPreload(started, state.preload.usedBytes-before)
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
