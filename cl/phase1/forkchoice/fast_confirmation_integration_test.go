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
	"fmt"
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
	finalized, safe := store.GetExecutionFinalityHashes(head.Root, common.Hash{98}, legacy)
	require.Equal(t, common.Hash{98}, finalized)
	require.Equal(t, legacy, safe)
	graph.headers = map[common.Hash]*cltypes.BeaconBlockHeader{head.Root: {Slot: 0}}
	store.finalizedCheckpoint.Store(solid.Checkpoint{Root: head.Root})
	store.fastConfirmation = &fastConfirmationTracker{}
	finalized, safe = store.GetExecutionFinalityHashes(head.Root, common.Hash{98}, legacy)
	require.Equal(t, common.Hash{2}, finalized)
	require.Equal(t, finalized, safe, "enabled startup exposes finalized parent payload, not justified")
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
	finalized, safe := store.GetExecutionFinalityHashes(newer, common.Hash{98}, common.Hash{99})
	require.Equal(t, common.Hash{20}, finalized)
	require.Equal(t, finalized, safe)
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
		require.NoError(t, store.OnBlock(t.Context(), block, true, true, false))
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
			_, safe := store.GetExecutionFinalityHashes(previousRoot, common.Hash{}, common.Hash{99})
			require.Equal(t, store.GetFinalizedExecutionHash(confirmed), safe)
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
				_, _ = store.GetExecutionFinalityHashes(root, common.Hash{}, common.Hash{})
			}
			results <- nil
		}()
	}
	for range 4 {
		require.NoError(t, <-results)
	}
}

func TestFastConfirmationRequiresExecutionValidation(t *testing.T) {
	blocks, anchor, _ := tests.GetBellatrixRandom()
	store := newOverlapTestStore(t, anchor, nil, false).store
	store.EnableFastConfirmation(anchor.CurrentJustifiedCheckpoint())
	roots := make([]common.Hash, 3)
	for i, block := range blocks[:3] {
		store.OnTick((anchor.GenesisTime() + block.Block.Slot*anchor.BeaconConfig().SecondsPerSlot) * 1000)
		require.NoError(t, store.OnBlock(t.Context(), block, false, true, false))
		var err error
		roots[i], err = block.Block.HashSSZ()
		require.NoError(t, err)
	}
	view, err := store.fastConfirmationView(store.Slot())
	require.NoError(t, err)
	for _, root := range roots {
		require.False(t, view.blocks[root].valid, "unvalidated forward-sync block must not be confirmed")
	}
	store.MarkPayloadVerified(roots[2], blocks[2].Block.Body.ExecutionPayload.BlockHash)
	view, err = store.fastConfirmationView(store.Slot())
	require.NoError(t, err)
	for _, root := range roots {
		require.True(t, view.blocks[root].valid, "a validated descendant validates its execution ancestors")
	}
}

func TestFastConfirmationExcludesEarlyVotes(t *testing.T) {
	for _, offset := range []uint64{0, 1} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			_, anchor, _ := tests.GetBellatrixRandom()
			store := newOverlapTestStore(t, anchor, nil, false).store
			store.EnableFastConfirmation(anchor.CurrentJustifiedCheckpoint())
			slot := anchor.Slot() + 1
			store.OnTick((anchor.GenesisTime() + slot*anchor.BeaconConfig().SecondsPerSlot) * 1000)
			root := store.AnchorRoot()
			store.ProcessAttestingIndicies(&solid.Attestation{Data: &solid.AttestationData{
				Slot: slot + offset, BeaconBlockRoot: root,
				Target: solid.Checkpoint{Epoch: (slot + offset) / store.beaconCfg.SlotsPerEpoch, Root: root},
			}}, []uint64{0, 1})
			store.setUnequivocating(1)
			view, err := store.fastConfirmationView(slot)
			require.NoError(t, err)
			require.Empty(t, view.votes[0].root, "early gossip votes must not contribute to FCR")
			require.Empty(t, view.votes[1].root)
			require.True(t, view.votes[1].equivocating, "vote filtering must preserve equivocation evidence")
		})
	}
}

func TestFastConfirmationEnablesExecutionStatusReporting(t *testing.T) {
	direct := &execution_client.ExecutionClientDirect{}
	remote := &execution_client.ExecutionClientEngine{}
	for _, tc := range []struct {
		engine  execution_client.ExecutionEngine
		enabled *bool
	}{{direct, &direct.EnableFastConfirmation}, {remote, &remote.EnableFastConfirmation}} {
		store, _, head := executionHeadFixture(t)
		store.finalizedCheckpoint.Store(solid.Checkpoint{Root: head.Root})
		store.engine = tc.engine
		require.False(t, *tc.enabled)
		store.EnableFastConfirmation(store.FinalizedCheckpoint())
		require.True(t, *tc.enabled)
	}
}

func TestFastConfirmationHeaderOnlyAnchorVotes(t *testing.T) {
	_, anchor, _ := tests.GetBellatrixRandom()
	cfg := *anchor.BeaconConfig()
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch = 0, 0, 0
	cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch, cfg.GloasForkEpoch = 0, 0, 0, 0
	cfg.InitializeForkSchedule()
	store := newOverlapTestStore(t, anchor, nil, false).store
	store.beaconCfg = &cfg
	store.EnableFastConfirmation(anchor.CurrentJustifiedCheckpoint())
	root := store.AnchorRoot()
	_, hasBody := store.forkGraph.GetBlock(root)
	require.False(t, hasBody)
	slot := anchor.Slot()
	store.OnTick((anchor.GenesisTime() + (slot+3)*cfg.SecondsPerSlot) * 1000)
	balances := &fcrBalances{balances: []uint64{100, 100, 100, 100}, active: []bool{true, true, true, true}, slashed: make([]bool, 4), total: 400}
	for _, tc := range []struct {
		name    string
		offset  uint64
		present bool
		status  cltypes.PayloadStatus
	}{
		{"same slot empty", 0, false, cltypes.PayloadStatusPending},
		{"same slot full", 0, true, cltypes.PayloadStatusPending},
		{"later empty", 1, false, cltypes.PayloadStatusEmpty},
		{"later full", 1, true, cltypes.PayloadStatusFull},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for i := range 4 {
				store.latestMessages.set(i, LatestMessage{Root: root, Slot: slot + tc.offset, PayloadPresent: tc.present})
			}
			view, err := store.fastConfirmationView(store.Slot())
			require.NoError(t, err)
			require.True(t, view.gloas)
			for _, vote := range view.votes[:4] {
				require.Equal(t, root, vote.root)
				require.Equal(t, tc.status, vote.status)
			}
			if tc.offset == 0 {
				return
			}
			view.balanceIncrement = 1
			view.committees = map[uint64][]uint64{slot + 1: {0, 1, 2, 3}}
			for _, parentStatus := range []cltypes.PayloadStatus{cltypes.PayloadStatusEmpty, cltypes.PayloadStatusFull} {
				child := common.Hash{99, byte(parentStatus)}
				view.blocks[child] = fcrBlock{slot: slot + 2, parent: root, parentStatus: parentStatus}
				var discount uint64
				if parentStatus == tc.status {
					discount = 300
				}
				require.Equal(t, discount, view.emptySlotDiscount(balances, child))
			}
		})
	}
}

func TestFastConfirmationGloasInvalidPayloadKeepsEmptyPath(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprintf("full=%t", full), func(t *testing.T) {
			store, base, anchor := executionHeadFixture(t)
			graph := &invalidTrackingGraph{getFinalizedExecutionHashForkGraph: base.getFinalizedExecutionHashForkGraph, invalid: make(map[common.Hash]bool)}
			store.forkGraph = graph
			graph.headers = map[common.Hash]*cltypes.BeaconBlockHeader{anchor.Root: {}}
			graph.anchorRoot = anchor.Root
			checkpoint := solid.Checkpoint{Root: anchor.Root}
			graph.currentJustified = checkpoint
			store.finalizedCheckpoint.Store(checkpoint)
			store.unrealizedJustifiedCheckpoint.Store(checkpoint)
			store.latestMessages = newLatestMessagesStore(4)
			store.EnableFastConfirmation(checkpoint)
			parent, child := common.Hash{4}, common.Hash{5}
			for i, root := range []common.Hash{parent, child} {
				block := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.GloasVersion)
				block.Block.Slot = uint64(i + 1)
				block.Block.ParentRoot = anchor.Root
				bid := block.Block.Body.SignedExecutionPayloadBid.Message
				bid.ParentBlockHash, bid.BlockHash = common.Hash{2}, common.Hash{byte(i + 4)}
				if i == 1 {
					block.Block.ParentRoot = parent
					if full {
						bid.ParentBlockHash = common.Hash{4}
					}
				}
				addInvalidAncestorsBlock(graph, root, block)
				store.unrealizedJustifications.Store(root, checkpoint)
			}
			store.MarkPayloadStatus(parent, common.Hash{4}, execution_client.PayloadStatusInvalidated)
			require.True(t, graph.IsBlockInvalid(parent))
			require.False(t, store.cutOffByInvalidation(parent))
			require.Equal(t, full, store.cutOffByInvalidation(child))
			store.headHash, store.headSlot = child, 2
			store.timeMs.Store(3 * store.beaconCfg.SecondsPerSlot * 1000)
			for i := range 4 {
				store.latestMessages.set(i, LatestMessage{Root: child, Slot: 2})
			}
			view, err := store.fastConfirmationView(store.Slot())
			require.NoError(t, err)
			balances := &fcrBalances{balances: []uint64{100, 100, 100, 100}, active: []bool{true, true, true, true}, slashed: make([]bool, 4), total: 400}
			view.balanceIncrement, view.proposerBoost = 1, 0
			view.committees = map[uint64][]uint64{2: {0, 1, 2, 3}}
			view.headBalances = balances
			require.True(t, view.oneConfirmed(balances, parent), "the block's optional invalid payload must not exclude its EMPTY variant")
			require.Equal(t, !full, view.oneConfirmed(balances, child))
			want := child
			if full {
				want = parent
			}
			rule := store.fastConfirmation.rule
			require.Equal(t, want, rule.latestDescendant(view, balances, anchor.Root))
			view.slot = view.slotsPerEpoch
			require.Equal(t, !full, rule.chainSafe(view, balances, child), "epoch-boundary reconfirmation must use the surviving payload path")
		})
	}
}
