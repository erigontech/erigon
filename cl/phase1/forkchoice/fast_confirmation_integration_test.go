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
	"testing"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestFastConfirmationSafeHashOptIn(t *testing.T) {
	store, graph, head := executionHeadFixture(t)
	legacy := common.Hash{99}
	require.Equal(t, legacy, store.GetSafeExecutionHash(head.Root, legacy))
	graph.headers = map[common.Hash]*cltypes.BeaconBlockHeader{head.Root: {Slot: 0}}
	store.finalizedCheckpoint.Store(solid.Checkpoint{Root: head.Root})
	store.fastConfirmation = &fastConfirmationTracker{}
	require.Equal(t, common.Hash{2}, store.GetSafeExecutionHash(head.Root, legacy), "enabled startup exposes finalized parent payload, not justified")
}

func TestFastConfirmationMissedDeadlineResetsToFinalized(t *testing.T) {
	store, _, head := executionHeadFixture(t)
	store.finalizedCheckpoint.Store(solid.Checkpoint{Root: head.Root})
	store.EnableFastConfirmation(store.FinalizedCheckpoint())
	previous := common.Hash{90}
	store.fastConfirmation.result.Store(&previous)
	store.SetSynced(true)
	slotStart := store.genesisTime*1000 + store.beaconCfg.SecondsPerSlot*1000
	now := slotStart + store.getAttestationDueMs(0)
	store.timeMs.Store(now)
	require.NoError(t, store.OnFastConfirmation(t.Context(), now, nil))
	require.Equal(t, head.Root, *store.fastConfirmation.result.Load())
	select {
	case <-store.FastConfirmationChanged():
	default:
		t.Fatal("safe change did not wake publication")
	}
	require.NoError(t, store.OnFastConfirmation(t.Context(), now+1, nil))
	select {
	case <-store.FastConfirmationChanged():
		t.Fatal("duplicate update in the same slot")
	default:
	}
}

func TestFastConfirmationSnapshotUsesExactVotesAndOwnedState(t *testing.T) {
	_, anchor, _ := tests.GetBellatrixRandom()
	anchor.SetCurrentJustifiedCheckpoint(solid.Checkpoint{Epoch: 1, Root: common.Hash{42}})
	fixture := newOverlapTestStore(t, anchor, nil, false)
	store := fixture.store
	store.EnableFastConfirmation(anchor.CurrentJustifiedCheckpoint())
	store.OnTick((anchor.GenesisTime() + (anchor.Slot()+1)*anchor.BeaconConfig().SecondsPerSlot) * 1000)
	root := store.AnchorRoot()
	store.latestMessages.set(0, LatestMessage{Root: root, Epoch: anchor.Slot() / anchor.BeaconConfig().SlotsPerEpoch, Slot: anchor.Slot()})
	view, err := store.fastConfirmationView(store.Slot())
	require.NoError(t, err)
	require.NotNil(t, view)
	require.Equal(t, root, view.votes[0].root)
	require.Equal(t, root, view.head)
	require.Equal(t, anchor.Slot(), view.blocks[root].slot)
	require.Equal(t, anchor.CurrentJustifiedCheckpoint(), view.blocks[root].votingSource)
	require.Equal(t, root, store.fastConfirmation.rule.confirmedRoot)
	store.unrealizedJustifications.Delete(root)
	_, err = store.fastConfirmationView(store.Slot())
	require.NoError(t, err, "finalized metadata may be pruned")
}

func TestFastConfirmationRunsOncePerSlot(t *testing.T) {
	_, anchor, _ := tests.GetBellatrixRandom()
	fixture := newOverlapTestStore(t, anchor, nil, false)
	store := fixture.store
	store.EnableFastConfirmation(store.FinalizedCheckpoint())
	slots := anchor.BeaconConfig().SlotsPerEpoch
	slot := (anchor.Slot()/slots+1)*slots - 1
	now := (anchor.GenesisTime() + slot*anchor.BeaconConfig().SecondsPerSlot) * 1000
	store.OnTick(now)
	observed := solid.Checkpoint{Epoch: store.FinalizedCheckpoint().Epoch + 1, Root: store.AnchorRoot()}
	applyCalls := 0
	apply := func(context.Context) error {
		applyCalls++
		store.unrealizedJustifiedCheckpoint.Store(observed)
		return nil
	}
	require.NoError(t, store.OnFastConfirmation(t.Context(), now, apply))
	require.Equal(t, 1, applyCalls)
	require.Equal(t, observed, store.fastConfirmation.rule.greatestUnrealized)
	store.unrealizedJustifiedCheckpoint.Store(solid.Checkpoint{Epoch: 99, Root: common.Hash{99}})
	require.NoError(t, store.OnFastConfirmation(t.Context(), now+1, nil))
	require.Equal(t, observed, store.fastConfirmation.rule.greatestUnrealized)
}

func TestFastConfirmationRetriesSnapshotConflict(t *testing.T) {
	_, anchor, _ := tests.GetBellatrixRandom()
	store := newOverlapTestStore(t, anchor, nil, false).store
	store.EnableFastConfirmation(anchor.CurrentJustifiedCheckpoint())
	now := (anchor.GenesisTime() + (anchor.Slot()+1)*anchor.BeaconConfig().SecondsPerSlot) * 1000
	store.OnTick(now)
	store.SetSynced(true)
	rule := store.fastConfirmation.rule
	calls := 0
	apply := func(context.Context) error {
		calls++
		if calls == 1 {
			return errFastConfirmationSnapshotChanged
		}
		return nil
	}
	require.NoError(t, store.OnFastConfirmation(t.Context(), now, apply))
	require.Same(t, rule, store.fastConfirmation.rule)
	require.False(t, store.fastConfirmation.attempted)
	require.NoError(t, store.OnFastConfirmation(t.Context(), now+2, apply))
	require.Equal(t, 2, calls)
	require.True(t, store.fastConfirmation.attempted)
	require.NoError(t, store.OnFastConfirmation(t.Context(), now+4, apply))
	require.Equal(t, 2, calls)
}

func TestFastConfirmationWaitsForClock(t *testing.T) {
	_, anchor, _ := tests.GetBellatrixRandom()
	store := newOverlapTestStore(t, anchor, nil, false).store
	store.EnableFastConfirmation(store.FinalizedCheckpoint())
	store.SetSynced(true)
	now := (anchor.GenesisTime() + (anchor.Slot()+1)*anchor.BeaconConfig().SecondsPerSlot) * 1000
	calls := 0
	apply := func(context.Context) error {
		calls++
		return context.Canceled
	}
	require.NoError(t, store.OnFastConfirmation(t.Context(), now, apply))
	require.Zero(t, calls)
	store.OnTick(now)
	require.ErrorIs(t, store.OnFastConfirmation(t.Context(), now, apply), context.Canceled)
	require.Equal(t, 1, calls)
}

func TestFastConfirmationSafeHashNeverPrecedesFinalized(t *testing.T) {
	store, graph, old := executionHeadFixture(t)
	newer := common.Hash{4}
	block := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.GloasVersion)
	block.Block.Slot = 1
	block.Block.ParentRoot = old.Root
	block.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash = common.Hash{20}
	graph.blocks[newer] = block
	graph.headers = map[common.Hash]*cltypes.BeaconBlockHeader{old.Root: {Slot: 0}, newer: {Slot: 1, ParentRoot: old.Root}}
	store.finalizedCheckpoint.Store(solid.Checkpoint{Root: newer})
	store.EnableFastConfirmation(store.FinalizedCheckpoint())
	store.fastConfirmation.result.Store(&old.Root)
	require.Equal(t, common.Hash{20}, store.GetSafeExecutionHash(newer, common.Hash{99}))
}

func TestFastConfirmationAcrossImportedEpochs(t *testing.T) {
	blocks, anchor, _ := tests.GetBellatrixRandom()
	fixture := newOverlapTestStore(t, anchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), false)
	store := fixture.store
	store.EnableFastConfirmation(anchor.CurrentJustifiedCheckpoint())
	advanced := false
	previousRoot := store.AnchorRoot()
	for i, block := range blocks {
		now := (anchor.GenesisTime() + block.Block.Slot*anchor.BeaconConfig().SecondsPerSlot) * 1000
		store.OnTick(now)
		previousSlot := block.Block.Slot - 1
		headState, err := store.GetOwnedStateAtBlockRoot(previousRoot)
		require.NoError(t, err)
		epoch := previousSlot / anchor.BeaconConfig().SlotsPerEpoch
		for committeeIndex := uint64(0); committeeIndex < headState.CommitteeCount(epoch); committeeIndex++ {
			indices, err := headState.GetBeaconCommitee(previousSlot, committeeIndex)
			require.NoError(t, err)
			store.ProcessAttestingIndicies(&solid.Attestation{Data: &solid.AttestationData{Slot: previousSlot, BeaconBlockRoot: previousRoot, Target: solid.Checkpoint{Epoch: epoch, Root: store.Ancestor(previousRoot, epoch*anchor.BeaconConfig().SlotsPerEpoch).Root}}}, indices)
		}
		require.NoError(t, store.OnBlock(t.Context(), block, false, true, false))
		if i == 1 {
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			metadata, ok := store.unrealizedJustifications.LoadAndDelete(common.Hash(root))
			require.True(t, ok)
			_, err = store.fastConfirmationView(store.Slot())
			require.ErrorContains(t, err, "unrealized justification")
			store.unrealizedJustifications.Store(common.Hash(root), metadata)
		}
		require.NoError(t, store.OnFastConfirmation(t.Context(), now, nil), "slot %d", block.Block.Slot)
		confirmed := store.fastConfirmation.rule.confirmedRoot
		previousRoot, err = block.Block.HashSSZ()
		require.NoError(t, err)
		header, ok := store.GetHeader(confirmed)
		require.True(t, ok)
		if header.Slot > store.FinalizedSlot() {
			advanced = true
			require.Equal(t, store.GetFinalizedExecutionHash(confirmed), store.GetSafeExecutionHash(previousRoot, common.Hash{99}))
		}
	}
	require.True(t, advanced, "FCR never advanced beyond finalized across three imported epochs")
}

func TestFastConfirmationConcurrentReaders(t *testing.T) {
	_, anchor, _ := tests.GetBellatrixRandom()
	fixture := newOverlapTestStore(t, anchor, nil, false)
	store := fixture.store
	store.EnableFastConfirmation(anchor.CurrentJustifiedCheckpoint())
	root := store.AnchorRoot()
	now := (anchor.GenesisTime() + (anchor.Slot()+1)*anchor.BeaconConfig().SecondsPerSlot) * 1000
	store.OnTick(now)
	results := make(chan error, 4)
	for range 4 {
		go func() {
			for range 16 {
				if err := store.OnFastConfirmation(t.Context(), now, nil); err != nil {
					results <- err
					return
				}
				_ = store.GetSafeExecutionHash(root, common.Hash{})
			}
			results <- nil
		}()
	}
	for range 4 {
		require.NoError(t, <-results)
	}
}
