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

package forkchoice

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
)

type fastConfirmationTracker struct {
	mu                 sync.Mutex
	rule               *fastConfirmation
	result             atomic.Pointer[common.Hash]
	changed            chan struct{}
	lastSlot           uint64
	attempted          bool
	balances           map[solid.Checkpoint]*fcrBalances
	anchorVotingSource solid.Checkpoint
}

func (f *ForkChoiceStore) GetSafeExecutionHash(head, fallback common.Hash) common.Hash {
	if f.fastConfirmation == nil {
		return fallback
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	root := f.finalizedCheckpoint.Load().(solid.Checkpoint).Root
	if confirmed := f.fastConfirmation.result.Load(); confirmed != nil && f.isAncestor(ForkChoiceNode{Root: head, PayloadStatus: cltypes.PayloadStatusPending}, ForkChoiceNode{Root: *confirmed, PayloadStatus: cltypes.PayloadStatusPending}) {
		confirmedHeader, hasConfirmed := f.forkGraph.GetHeader(*confirmed)
		finalizedHeader, hasFinalized := f.forkGraph.GetHeader(root)
		if hasConfirmed && hasFinalized && confirmedHeader.Slot >= finalizedHeader.Slot {
			root = *confirmed
		}
	}
	return f.finalizedExecutionHashLocked(root)
}

func (f *ForkChoiceStore) EnableFastConfirmation(anchorVotingSource solid.Checkpoint) {
	f.fastConfirmation = &fastConfirmationTracker{rule: newFastConfirmation(f.FinalizedCheckpoint()), changed: make(chan struct{}, 1), anchorVotingSource: anchorVotingSource}
}

func (f *ForkChoiceStore) FastConfirmationChanged() <-chan struct{} {
	if f.fastConfirmation == nil {
		return nil
	}
	return f.fastConfirmation.changed
}

func (f *ForkChoiceStore) OnFastConfirmation(ctx context.Context, nowMs uint64, applyPending func(context.Context) error) (err error) {
	tracker := f.fastConfirmation
	if tracker == nil {
		return nil
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	slot := f.slotAtTimeMs(nowMs)
	if tracker.attempted && tracker.lastSlot == slot {
		return nil
	}
	previousSlot, attempted := tracker.lastSlot, tracker.attempted
	tracker.attempted, tracker.lastSlot = true, slot
	reset := func() {
		tracker.rule = newFastConfirmation(f.FinalizedCheckpoint())
		tracker.balances = nil
		tracker.publish(tracker.rule.confirmedRoot)
	}
	slotStart := (f.genesisTime + slot*f.beaconCfg.SecondsPerSlot) * 1000
	deadline := slotStart + f.getAttestationDueMs(slot/f.beaconCfg.SlotsPerEpoch)
	if !f.synced.Load() || nowMs >= deadline {
		reset()
		return nil
	}
	if attempted && slot != previousSlot+1 {
		reset()
	}
	defer func() {
		if err != nil {
			reset()
		}
	}()
	started := time.Now()
	if applyPending != nil {
		pendingCtx, cancel := context.WithTimeout(ctx, time.Duration(deadline-nowMs)*time.Millisecond)
		err := applyPending(pendingCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("fast confirmation pending attestations: %w", err)
		}
	}
	view, err := f.fastConfirmationView(slot)
	if err != nil {
		return err
	}
	if nowMs+uint64(time.Since(started).Milliseconds()) >= deadline {
		reset()
		return nil
	}
	tracker.rule.updateVariables(view)
	if err := f.loadFastConfirmationStates(view); err != nil {
		return err
	}
	previous := tracker.balances[tracker.rule.previousObserved]
	current := tracker.balances[tracker.rule.currentObserved]
	root := tracker.rule.latestConfirmed(view, previous, current)
	if nowMs+uint64(time.Since(started).Milliseconds()) >= slotStart+f.beaconCfg.SecondsPerSlot*1000 {
		reset()
		return nil
	}
	tracker.rule.confirmedRoot = root
	tracker.balances = map[solid.Checkpoint]*fcrBalances{tracker.rule.previousObserved: previous, tracker.rule.currentObserved: current}
	tracker.publish(root)
	return nil
}

func (f *ForkChoiceStore) loadFastConfirmationStates(v *fcrView) error {
	tracker := f.fastConfirmation
	if tracker.balances == nil {
		tracker.balances = make(map[solid.Checkpoint]*fcrBalances)
	}
	for _, cp := range []solid.Checkpoint{tracker.rule.previousObserved, tracker.rule.currentObserved} {
		if _, ok := tracker.balances[cp]; ok {
			continue
		}
		cs, err := f.getCheckpointState(cp)
		if err != nil {
			return fmt.Errorf("fast confirmation balance source: %w", err)
		}
		b := &fcrBalances{balances: cs.balances, active: make([]bool, len(cs.balances)), slashed: make([]bool, len(cs.balances))}
		for i, balance := range cs.balances {
			b.active[i], b.slashed[i] = readFromBitset(cs.actives, i), readFromBitset(cs.slasheds, i)
			if b.active[i] {
				b.total += balance
			}
		}
		b.total = max(b.total, v.balanceIncrement)
		tracker.balances[cp] = b
	}
	headState, err := f.GetOwnedStateAtBlockRoot(v.head)
	if err != nil {
		return err
	}
	if headState == nil {
		return errors.New("fast confirmation head state unavailable")
	}
	epoch := v.slot / v.slotsPerEpoch
	firstEpoch := uint64(0)
	if epoch > 2 {
		firstEpoch = epoch - 2
	}
	v.committees = make(map[uint64][]uint64)
	for e := firstEpoch; e <= epoch; e++ {
		for slot := e * v.slotsPerEpoch; slot < (e+1)*v.slotsPerEpoch; slot++ {
			for index := uint64(0); index < headState.CommitteeCount(e); index++ {
				committee, err := headState.GetBeaconCommitee(slot, index)
				if err != nil {
					return err
				}
				v.committees[slot] = append(v.committees[slot], committee...)
			}
		}
	}
	if state.Epoch(headState) < epoch {
		if err := transition.DefaultMachine.ProcessSlots(headState, epoch*v.slotsPerEpoch); err != nil {
			return err
		}
	}
	b := &fcrBalances{balances: make([]uint64, headState.ValidatorLength()), active: make([]bool, headState.ValidatorLength()), slashed: make([]bool, headState.ValidatorLength()), total: headState.GetTotalActiveBalance()}
	headState.ForEachValidator(func(validator solid.Validator, index, total int) bool {
		b.balances[index], b.active[index], b.slashed[index] = validator.EffectiveBalance(), validator.Active(epoch), validator.Slashed()
		return true
	})
	v.headBalances = b
	return nil
}

func (t *fastConfirmationTracker) publish(root common.Hash) {
	previous := t.result.Swap(&root)
	if previous != nil && *previous == root {
		return
	}
	select {
	case t.changed <- struct{}{}:
	default:
	}
}

func (f *ForkChoiceStore) fastConfirmationView(slot uint64) (*fcrView, error) {
	head, headSlot, err := f.GetHeadNode()
	if err != nil {
		return nil, err
	}
	epoch := slot / f.beaconCfg.SlotsPerEpoch
	if headSlot/f.beaconCfg.SlotsPerEpoch+1 < epoch {
		return nil, errors.New("fast confirmation head is stale")
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.Slot() != slot || f.headHash != head.Root {
		return nil, errors.New("fork choice changed during fast confirmation snapshot")
	}
	v := &fcrView{
		slot: slot, slotsPerEpoch: f.beaconCfg.SlotsPerEpoch, balanceIncrement: f.beaconCfg.EffectiveBalanceIncrement, proposerBoost: f.beaconCfg.ProposerScoreBoost,
		head: head.Root, finalized: f.finalizedCheckpoint.Load().(solid.Checkpoint), unrealized: f.unrealizedJustifiedCheckpoint.Load().(solid.Checkpoint),
		blocks: make(map[common.Hash]fcrBlock), gloas: f.beaconCfg.GetCurrentStateVersion(epoch) >= clparams.GloasVersion,
	}
	var metadataErr error
	addChain := func(root common.Hash) {
		for root != (common.Hash{}) {
			if _, has := v.blocks[root]; has {
				break
			}
			header, has := f.forkGraph.GetHeader(root)
			if !has {
				break
			}
			unrealized, hasUnrealized := f.getUnrealizedJustification(root)
			if !hasUnrealized && header.Slot > v.finalized.Epoch*v.slotsPerEpoch {
				metadataErr = fmt.Errorf("fast confirmation missing unrealized justification for %x", root)
				return
			}
			source := unrealized
			if header.Slot/f.beaconCfg.SlotsPerEpoch == epoch {
				if root == f.forkGraph.AnchorRoot() {
					source = f.fastConfirmation.anchorVotingSource
				} else {
					var hasSource bool
					source, hasSource = f.forkGraph.GetCurrentJustifiedCheckpoint(root)
					if !hasSource {
						metadataErr = fmt.Errorf("fast confirmation missing voting source for %x", root)
						return
					}
				}
			}
			block := fcrBlock{slot: header.Slot, parent: header.ParentRoot, valid: !f.forkGraph.IsBlockInvalid(root), unrealized: unrealized, votingSource: source}
			if bid := f.executionPayloadBid(root); bid != nil {
				signed, ok := f.forkGraph.GetBlock(root)
				if ok {
					block.parentStatus = f.getParentPayloadStatus(signed.Block)
				}
				block.valid = block.valid && (bid.ParentBlockHash == f.finalizedExecutionHashLocked(v.finalized.Root) || f.payloadValidatedLocked(common.Hash{}, bid.ParentBlockHash))
			} else if f.optimisticStore != nil {
				block.valid = block.valid && !f.optimisticStore.IsOptimistic(root)
			}
			v.blocks[root] = block
			root = header.ParentRoot
		}
	}
	rule := f.fastConfirmation.rule
	for _, root := range []common.Hash{head.Root, v.finalized.Root, rule.confirmedRoot, rule.currentSlotHead, rule.previousSlotHead, rule.currentObserved.Root, rule.previousObserved.Root, rule.greatestUnrealized.Root} {
		addChain(root)
	}
	f.latestMessages.mu.RLock()
	defer f.latestMessages.mu.RUnlock()
	v.votes = make([]fcrVote, len(f.latestMessages.latestMessages))
	for i, message := range f.latestMessages.latestMessages {
		vote := fcrVote{root: message.Root, epoch: message.Epoch, status: cltypes.PayloadStatusPending, equivocating: f.isUnequivocating(uint64(i))}
		if message.Root != (common.Hash{}) {
			addChain(message.Root)
			if v.gloas {
				vote.epoch = message.Slot / v.slotsPerEpoch
				vote.status = f.getSupportedNode(message).PayloadStatus
			}
		}
		v.votes[i] = vote
	}
	if metadataErr != nil {
		return nil, metadataErr
	}
	return v, nil
}
