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

package privatepool

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"

	mapset "github.com/deckarep/golang-set/v2"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

type Bundle struct {
	TargetHash       common.Hash
	TargetParentHash common.Hash
	TargetGeneration uint64
	Transaction      types.Transaction
	TargetSlot       uint64
}

type Pool struct {
	base          txnprovider.TxnProvider
	capacity      int
	slotActive    func(uint64) bool
	contextActive func(uint64, uint64) bool

	mu        sync.RWMutex
	bundles   map[common.Hash]Bundle
	order     []common.Hash
	revision  uint64
	buildSlot uint64
	contexts  map[bundleContextKey]bundleContextState
	snapshots chan struct{}
}

type bundleContextKey struct {
	slot       uint64
	parentHash common.Hash
	generation uint64
}

type bundleContextState struct {
	submitted uint64
	observed  uint64
}

type Option func(*Pool)

func WithSlotActive(active func(uint64) bool) Option {
	return func(pool *Pool) {
		pool.slotActive = active
	}
}

func WithContextActive(active func(uint64, uint64) bool) Option {
	return func(pool *Pool) {
		pool.contextActive = active
	}
}

func New(base txnprovider.TxnProvider, capacity int, opts ...Option) *Pool {
	pool := &Pool{
		base: base, capacity: capacity, bundles: make(map[common.Hash]Bundle),
		contexts: make(map[bundleContextKey]bundleContextState), snapshots: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(pool)
	}
	return pool
}

func (p *Pool) Submit(bundle Bundle) (common.Hash, error) {
	if bundle.Transaction == nil {
		return common.Hash{}, errors.New("private bundle transaction is required")
	}
	if bundle.TargetHash == (common.Hash{}) {
		return common.Hash{}, errors.New("private bundle target transaction hash is required")
	}
	if bundle.TargetSlot == 0 {
		return common.Hash{}, errors.New("private bundle target slot is required")
	}
	if bundle.Transaction.Type() == types.BlobTxType {
		return common.Hash{}, errors.New("private blob transactions are not supported")
	}
	id := bundle.Transaction.Hash()
	if id == bundle.TargetHash {
		return common.Hash{}, errors.New("private transaction cannot be its own public target")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if existing, ok := p.bundles[id]; ok {
		if existing.TargetHash == bundle.TargetHash && existing.TargetSlot == bundle.TargetSlot &&
			existing.TargetParentHash == bundle.TargetParentHash && existing.TargetGeneration == bundle.TargetGeneration {
			return id, nil
		}
		p.bundles[id] = bundle
		p.revision++
		p.markSubmitted(bundle)
		p.pruneContext(existing)
		return id, nil
	}
	if p.capacity <= 0 {
		return common.Hash{}, fmt.Errorf("private bundle pool capacity %d reached", p.capacity)
	}
	if len(p.bundles) >= p.capacity && !p.evictBundleFor(bundle, p.buildSlot) {
		return common.Hash{}, fmt.Errorf("private bundle pool capacity %d reached", p.capacity)
	}
	p.bundles[id] = bundle
	p.order = append(p.order, id)
	p.revision++
	p.markSubmitted(bundle)
	return id, nil
}

func (p *Pool) RebindGeneration(slot uint64, parentHash common.Hash, from, to uint64) {
	if p == nil || from == 0 || to == 0 || from == to {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	changed := false
	for id, bundle := range p.bundles {
		if bundle.TargetSlot != slot || bundle.TargetParentHash != parentHash || bundle.TargetGeneration != from {
			continue
		}
		bundle.TargetGeneration = to
		p.bundles[id] = bundle
		changed = true
	}
	if !changed {
		return
	}
	p.revision++
	delete(p.contexts, bundleContextKey{slot: slot, parentHash: parentHash, generation: from})
	key := bundleContextKey{slot: slot, parentHash: parentHash, generation: to}
	state := p.contexts[key]
	state.submitted = p.revision
	state.observed = 0
	p.contexts[key] = state
	p.notifySnapshotChange()
}

// PinnedGenerations returns build generations that still own private bundles for a slot.
func (p *Pool) PinnedGenerations(slot uint64) map[uint64]struct{} {
	result := make(map[uint64]struct{})
	if p == nil {
		return result
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, bundle := range p.bundles {
		if bundle.TargetSlot == slot && bundle.TargetGeneration != 0 {
			result[bundle.TargetGeneration] = struct{}{}
		}
	}
	return result
}

func contextKey(bundle Bundle) bundleContextKey {
	return bundleContextKey{slot: bundle.TargetSlot, parentHash: bundle.TargetParentHash, generation: bundle.TargetGeneration}
}

func (p *Pool) markSubmitted(bundle Bundle) {
	key := contextKey(bundle)
	state := p.contexts[key]
	state.submitted = p.revision
	p.contexts[key] = state
}

func (p *Pool) pruneContext(removed Bundle) {
	key := contextKey(removed)
	for _, bundle := range p.bundles {
		if contextKey(bundle) == key {
			return
		}
	}
	delete(p.contexts, key)
	p.notifySnapshotChange()
}

func (p *Pool) evictBundleFor(incoming Bundle, buildSlot uint64) bool {
	victimIndex := -1
	var victimSlot uint64
	for i, id := range p.order {
		bundle := p.bundles[id]
		slot := bundle.TargetSlot
		if slot < buildSlot && !p.isBundleContextActive(bundle) && (victimIndex < 0 || slot < victimSlot) {
			victimIndex = i
			victimSlot = slot
		}
	}
	if victimIndex < 0 {
		for i, id := range p.order {
			bundle := p.bundles[id]
			if bundle.TargetSlot == incoming.TargetSlot &&
				(bundle.TargetParentHash != incoming.TargetParentHash || bundle.TargetGeneration != incoming.TargetGeneration) &&
				!p.isBundleContextActive(bundle) {
				victimIndex = i
				break
			}
		}
	}
	if victimIndex < 0 {
		for i, id := range p.order {
			bundle := p.bundles[id]
			slot := bundle.TargetSlot
			if slot > incoming.TargetSlot && !p.isBundleContextActive(bundle) && (victimIndex < 0 || slot > victimSlot) {
				victimIndex = i
				victimSlot = slot
			}
		}
	}
	if victimIndex < 0 {
		return false
	}
	removed := p.bundles[p.order[victimIndex]]
	delete(p.bundles, p.order[victimIndex])
	p.order = append(p.order[:victimIndex], p.order[victimIndex+1:]...)
	p.pruneContext(removed)
	return true
}

func (p *Pool) isSlotActive(slot uint64) bool {
	return p.slotActive != nil && p.slotActive(slot)
}

func (p *Pool) isBundleContextActive(bundle Bundle) bool {
	if p.contextActive != nil && bundle.TargetGeneration != 0 {
		return p.contextActive(bundle.TargetSlot, bundle.TargetGeneration)
	}
	return p.isSlotActive(bundle.TargetSlot)
}

func (p *Pool) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	provideOpts := txnprovider.ApplyProvideOptions(opts...)

	var baseRevision uint64
	baseRevisionObserved := false
	baseCtx := txnprovider.WithTxnRevisionObserver(ctx, func(revision uint64) {
		baseRevision = revision
		baseRevisionObserved = true
	})
	txns, err := p.base.ProvideTxns(baseCtx, opts...)
	if err != nil {
		return nil, err
	}
	if revisioned, ok := p.base.(txnprovider.RevisionedTxnProvider); ok && !baseRevisionObserved {
		baseRevision = revisioned.TransactionSetRevision(provideOpts.BlockTime, provideOpts.ParentBlockNum)
	}
	bundles, privateRevision := p.snapshot(provideOpts.TargetSlot, provideOpts.TargetParentHash, provideOpts.TargetGeneration)

	result, protected := insertBundles(txns, bundles, provideOpts.TxnIdsFilter, provideOpts.IncludedTxnIds)
	result = capBatch(result, max(provideOpts.Amount, 0), protected, provideOpts.TxnIdsFilter)
	txnprovider.ObserveTxnPolicies(ctx, selectedPolicies(result, bundles, provideOpts.IncludedTxnIds))
	txnprovider.ObserveTxnRevision(ctx, combineRevisions(baseRevision, privateRevision))
	txnprovider.ObserveTxnBatchRevision(ctx, privateRevision)
	return result, nil
}

func (p *Pool) TransactionSetRevision(blockTime, parentBlockNum uint64) uint64 {
	var baseRevision uint64
	if revisioned, ok := p.base.(txnprovider.RevisionedTxnProvider); ok {
		baseRevision = revisioned.TransactionSetRevision(blockTime, parentBlockNum)
	}
	p.mu.RLock()
	privateRevision := p.revision
	p.mu.RUnlock()
	return combineRevisions(baseRevision, privateRevision)
}

func (p *Pool) snapshot(targetSlot uint64, targetParentHash common.Hash, targetGeneration uint64) ([]Bundle, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if targetSlot > p.buildSlot {
		p.buildSlot = targetSlot
	}

	selected := make([]Bundle, 0)
	if targetSlot > 0 {
		for _, id := range p.order {
			bundle := p.bundles[id]
			if bundle.TargetSlot == targetSlot && bundle.TargetParentHash == targetParentHash && bundle.TargetGeneration == targetGeneration {
				selected = append(selected, bundle)
			}
		}
	}
	return selected, p.revision
}

func (p *Pool) ObserveProcessedTxns(
	targetSlot uint64,
	targetParentHash common.Hash,
	targetGeneration uint64,
	revision uint64,
) {
	key := bundleContextKey{slot: targetSlot, parentHash: targetParentHash, generation: targetGeneration}
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.contexts[key]
	if state.submitted == 0 || revision <= state.observed {
		return
	}
	state.observed = revision
	p.contexts[key] = state
	p.notifySnapshotChange()
}

func (p *Pool) notifySnapshotChange() {
	close(p.snapshots)
	p.snapshots = make(chan struct{})
}

func (p *Pool) WaitForProcessing(
	ctx context.Context,
	buildDone <-chan struct{},
	targetSlot uint64,
	targetParentHash common.Hash,
	targetGeneration uint64,
) error {
	if p == nil {
		return nil
	}
	key := bundleContextKey{slot: targetSlot, parentHash: targetParentHash, generation: targetGeneration}
	for {
		p.mu.RLock()
		state := p.contexts[key]
		changed := p.snapshots
		p.mu.RUnlock()
		if state.submitted == 0 || state.observed >= state.submitted {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-buildDone:
			p.mu.RLock()
			state = p.contexts[key]
			p.mu.RUnlock()
			if state.observed >= state.submitted {
				return nil
			}
			return errors.New("private bundle payload builder completed before processing admitted transactions")
		case <-changed:
		}
	}
}

func selectedPolicies(txns []types.Transaction, bundles []Bundle, includedTargets mapset.Set[[32]byte]) map[common.Hash]txnprovider.TransactionPolicy {
	included := make(map[common.Hash]struct{}, len(txns))
	for _, txn := range txns {
		included[txn.Hash()] = struct{}{}
	}
	policies := make(map[common.Hash]txnprovider.TransactionPolicy)
	for _, bundle := range bundles {
		_, targetInBatch := included[bundle.TargetHash]
		if !targetInBatch && (includedTargets == nil || !includedTargets.Contains(bundle.TargetHash)) {
			continue
		}
		if _, ok := included[bundle.Transaction.Hash()]; !ok {
			continue
		}
		policies[bundle.Transaction.Hash()] = txnprovider.TransactionPolicy{
			RequiresSuccess: true,
			Dependency:      bundle.TargetHash,
		}
	}
	return policies
}

func insertBundles(public []types.Transaction, bundles []Bundle, yielded, includedTargets mapset.Set[[32]byte]) ([]types.Transaction, map[common.Hash]struct{}) {
	byTarget := make(map[common.Hash][]types.Transaction, len(bundles))
	for _, bundle := range bundles {
		if yielded != nil && yielded.Contains(bundle.Transaction.Hash()) {
			continue
		}
		byTarget[bundle.TargetHash] = append(byTarget[bundle.TargetHash], bundle.Transaction)
	}
	result := make([]types.Transaction, 0, len(public)+len(bundles))
	protected := make(map[common.Hash]struct{}, len(bundles)*2)
	inserted := make(map[common.Hash]struct{}, len(bundles))
	for _, txn := range public {
		result = append(result, txn)
		for _, privateTxn := range byTarget[txn.Hash()] {
			result = append(result, privateTxn)
			inserted[privateTxn.Hash()] = struct{}{}
			protected[txn.Hash()] = struct{}{}
			protected[privateTxn.Hash()] = struct{}{}
			if yielded != nil {
				yielded.Add(privateTxn.Hash())
			}
		}
	}
	for _, bundle := range bundles {
		privateHash := bundle.Transaction.Hash()
		if includedTargets == nil || !includedTargets.Contains(bundle.TargetHash) {
			continue
		}
		if _, ok := inserted[privateHash]; ok {
			continue
		}
		if yielded != nil && yielded.Contains(privateHash) {
			continue
		}
		result = append(result, bundle.Transaction)
		protected[privateHash] = struct{}{}
		if yielded != nil {
			yielded.Add(privateHash)
		}
	}
	return result, protected
}

func capBatch(txns []types.Transaction, amount int, protected map[common.Hash]struct{}, yielded mapset.Set[[32]byte]) []types.Transaction {
	for len(txns) > amount {
		remove := -1
		for i := range slices.Backward(txns) {
			if _, ok := protected[txns[i].Hash()]; !ok {
				remove = i
				break
			}
		}
		if remove < 0 {
			remove = len(txns) - 1
		}
		if yielded != nil {
			yielded.Remove(txns[remove].Hash())
		}
		txns = append(txns[:remove], txns[remove+1:]...)
	}
	return txns
}

func combineRevisions(base, private uint64) uint64 {
	var input [16]byte
	binary.LittleEndian.PutUint64(input[:8], base)
	binary.LittleEndian.PutUint64(input[8:], private)
	digest := sha256.Sum256(input[:])
	return binary.LittleEndian.Uint64(digest[:])
}

var _ txnprovider.RevisionedTxnProvider = (*Pool)(nil)
