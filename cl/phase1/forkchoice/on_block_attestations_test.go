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
	"slices"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/impl/eth2/statechange"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
)

var blockAttestationFixtures = []struct {
	name string
	load func() ([]*cltypes.SignedBeaconBlock, *state.CachingBeaconState, *state.CachingBeaconState)
}{
	{name: "bellatrix", load: tests.GetBellatrixRandom},
	{name: "capella", load: tests.GetCapellaRandom},
	{name: "electra", load: tests.GetElectraRandom},
}

func newBlockAttestationStore(t *testing.T, blocks []*cltypes.SignedBeaconBlock, anchor *state.CachingBeaconState, synced bool) *ForkChoiceStore {
	t.Helper()
	store := newBlockTestStore(t, anchor)
	store.SetSynced(synced)
	lastSlot := blocks[len(blocks)-1].Block.Slot
	store.OnTick((anchor.GenesisTime() + (lastSlot+1)*anchor.BeaconConfig().SecondsPerSlot) * 1000)
	return store
}

func newBlockTestStore(t *testing.T, anchor *state.CachingBeaconState) *ForkChoiceStore {
	t.Helper()
	cfg := anchor.BeaconConfig()
	graph, err := fork_graph.NewForkGraphDisk(anchor, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	store, err := NewForkChoiceStore(
		eth_clock.NewEthereumClock(anchor.GenesisTime(), anchor.GenesisValidatorsRoot(), cfg),
		anchor,
		nil,
		pool.NewOperationsPool(cfg),
		graph,
		beaconevents.NewEventEmitter(),
		synced_data.NewSyncedDataManager(cfg, true),
		nil,
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		validator_params.NewValidatorParams(),
		false,
		nil,
	)
	require.NoError(t, err)
	return store
}

func countLatestMessages(store *ForkChoiceStore) int {
	store.latestMessages.mu.RLock()
	defer store.latestMessages.mu.RUnlock()
	count := 0
	for _, message := range store.latestMessages.latestMessages {
		if message != (LatestMessage{}) {
			count++
		}
	}
	return count
}

// OnBlock alone must leave the store as the on_block step of the spec fork choice tests does: on_block followed by
// on_attestation(is_from_block=True) for every attestation in the body. The reference store discards the
// votes its OnBlock applied and applies them again through OnAttestation with the block as its head.
func TestOnBlockAppliesBodyAttestations(t *testing.T) {
	imports := []struct {
		name   string
		synced bool
		run    func(*ForkChoiceStore, context.Context, *cltypes.SignedBeaconBlock) error
	}{
		{name: "sync", synced: true, run: func(store *ForkChoiceStore, ctx context.Context, block *cltypes.SignedBeaconBlock) error {
			return store.OnBlock(ctx, block, true, true, false)
		}},
		{name: "published", synced: true, run: func(store *ForkChoiceStore, ctx context.Context, block *cltypes.SignedBeaconBlock) error {
			return store.OnBlockWithEquivocationCheck(ctx, block, true, true, false)
		}},
		{name: "initial sync", run: func(store *ForkChoiceStore, ctx context.Context, block *cltypes.SignedBeaconBlock) error {
			return store.OnBlock(ctx, block, false, true, false)
		}},
	}
	for _, fixture := range blockAttestationFixtures {
		for _, imp := range imports {
			t.Run(fixture.name+"/"+imp.name, func(t *testing.T) {
				blocks, anchor, _ := fixture.load()
				got := newBlockAttestationStore(t, blocks, anchor, imp.synced)
				blocks, anchor, _ = fixture.load()
				want := newBlockAttestationStore(t, blocks, anchor, true)
				for _, block := range blocks {
					require.NoError(t, imp.run(got, t.Context(), block))
					applied := slices.Clone(want.latestMessages.latestMessages)
					require.NoError(t, want.OnBlock(t.Context(), block, false, true, false))
					want.latestMessages.latestMessages = applied
					blockRoot, err := block.Block.HashSSZ()
					require.NoError(t, err)
					headState, err := want.GetStateAtBlockRoot(blockRoot)
					require.NoError(t, err)
					require.NoError(t, want.syncedDataManager.OnHeadState(headState))
					block.Block.Body.Attestations.Range(func(_ int, attestation *solid.Attestation, _ int) bool {
						_ = want.OnAttestation(attestation, true, false)
						return true
					})
				}
				require.NotZero(t, countLatestMessages(want))
				require.Equal(t, countLatestMessages(want), countLatestMessages(got))
				require.Equal(t, want.latestMessages.latestMessages, got.latestMessages.latestMessages)
			})
		}
	}
}

func TestOnBlockReadsBodyAttestationCommitteesFromBlockState(t *testing.T) {
	blocks, anchor, _ := tests.GetCapellaRandom()
	store := newBlockAttestationStore(t, blocks, anchor, true)
	for _, block := range blocks {
		require.NoError(t, store.OnBlock(t.Context(), block, false, true, false))
	}
	lastBlock := blocks[len(blocks)-1]
	require.NotZero(t, lastBlock.Block.Body.Attestations.Len())
	lastBlock.Block.Body.Attestations.Range(func(_ int, attestation *solid.Attestation, _ int) bool {
		_, built := store.checkpointStates.Load(attestation.Data.Target)
		require.False(t, built, "checkpoint state built for target %+v", attestation.Data.Target)
		return true
	})
}

func TestOnAttestationLockedUsesBlockStateOnlyWithTargetShuffling(t *testing.T) {
	fixture := newTargetShufflingFixture(t)
	providerErr := errors.New("attesting indices unavailable")
	for _, test := range []struct {
		name            string
		targetRoot      common.Hash
		providerIndices []uint64
		providerErr     error
		wantErr         string
		fromBlock       bool
		keepHeadState   bool
		wantValidator   uint64
		rejectValidator uint64
		wantStateRead   bool
	}{
		{
			name:            "side branch target uses target checkpoint state",
			targetRoot:      fixture.targetRoot,
			fromBlock:       true,
			wantValidator:   fixture.targetValidator,
			rejectValidator: fixture.headValidator,
			wantStateRead:   true,
		},
		{
			name:            "block branch target uses provider indices",
			targetRoot:      fixture.headTargetRoot,
			providerIndices: []uint64{fixture.targetValidator},
			fromBlock:       true,
			wantValidator:   fixture.targetValidator,
			rejectValidator: fixture.headValidator,
		},
		{
			name:        "block branch returns provider error",
			targetRoot:  fixture.headTargetRoot,
			providerErr: providerErr,
			wantErr:     providerErr.Error(),
			fromBlock:   true,
		},
		{
			name:            "gossip ignores block source",
			targetRoot:      fixture.headTargetRoot,
			providerIndices: []uint64{fixture.targetValidator},
			wantErr:         "invalid aggregate signature",
			keepHeadState:   true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			graph := fixture.forkGraph(false)
			graph.states[fixture.targetRoot] = fixture.targetState
			store := newAttestationTestStore(t, fixture.cfg, fixture.headState, fixture.headRoot, fixture.attestationSlot, graph)
			if !test.keepHeadState {
				store.syncedDataManager.UnsetHeadState()
			}
			attestation := &solid.Attestation{
				AggregationBits: aggregationBits(fixture.committeeSize(t, test.targetRoot), int(fixture.cfg.MaxValidatorsPerCommittee), fixture.committeePosition),
				Signature:       common.Bytes96(bls.InfiniteSignature),
				Data: &solid.AttestationData{
					Slot:            fixture.attestationSlot,
					CommitteeIndex:  fixture.committeeIndex,
					BeaconBlockRoot: test.targetRoot,
					Target:          solid.Checkpoint{Epoch: fixture.targetEpoch, Root: test.targetRoot},
				},
			}
			source := &blockAttestationSource{
				state:            fixture.headState,
				root:             fixture.headRoot,
				attestingIndices: preparedAttestingIndices{indices: test.providerIndices, err: test.providerErr},
			}

			store.mu.Lock()
			err := store.onAttestationLocked(attestation, test.fromBlock, source)
			store.mu.Unlock()

			if test.wantErr != "" {
				require.EqualError(t, err, test.wantErr)
				require.Zero(t, countLatestMessages(store))
				return
			}
			require.NoError(t, err)
			_, credited := store.getLatestMessage(test.wantValidator)
			require.True(t, credited, "expected validator was not credited")
			_, credited = store.getLatestMessage(test.rejectValidator)
			require.False(t, credited, "unexpected validator was credited")
			if test.wantStateRead {
				require.Equal(t, []common.Hash{test.targetRoot}, graph.stateRoots())
			} else {
				require.Empty(t, graph.stateRoots())
			}
		})
	}
}

func preparePostStateTestFixture(t *testing.T) (*state.CachingBeaconState, *cltypes.BeaconBlock) {
	t.Helper()
	blocks, anchor, _ := tests.GetBellatrixRandom()
	postState, err := anchor.Copy()
	require.NoError(t, err)
	for _, block := range blocks {
		require.NoError(t, transition.TransitionState(postState, block, nil, true))
		expected, err := postState.Copy()
		require.NoError(t, err)
		require.NoError(t, statechange.ProcessJustificationBitsAndFinality(expected, nil))
		if expected.CurrentJustifiedCheckpoint() != postState.CurrentJustifiedCheckpoint() || expected.FinalizedCheckpoint() != postState.FinalizedCheckpoint() {
			return postState, block.Block
		}
	}
	t.Fatal("Bellatrix fixture has no post-state whose pull-up changes its checkpoints")
	return nil, nil
}

func TestPreparePostStateLeavesPostStateRootUnchanged(t *testing.T) {
	postState, beaconBlock := preparePostStateTestFixture(t)
	before, err := postState.HashSSZ()
	require.NoError(t, err)

	preparePostState(postState, beaconBlock)

	after, err := postState.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestPreparePostStateMatchesInlineComputation(t *testing.T) {
	postState, beaconBlock := preparePostStateTestFixture(t)
	expected, err := postState.Copy()
	require.NoError(t, err)
	require.NoError(t, statechange.ProcessJustificationBitsAndFinality(expected, nil))

	wantIndices := make([][]uint64, 0, beaconBlock.Body.Attestations.Len())
	beaconBlock.Body.Attestations.Range(func(_ int, attestation *solid.Attestation, _ int) bool {
		indices, err := postState.GetAttestingIndicies(attestation, true)
		require.NoError(t, err)
		wantIndices = append(wantIndices, indices)
		return true
	})
	require.NotEmpty(t, wantIndices)

	prepared := preparePostState(postState, beaconBlock)

	require.NoError(t, prepared.pullupErr)
	require.Equal(t, expected.CurrentJustifiedCheckpoint(), prepared.postPullupJustified)
	require.Equal(t, expected.FinalizedCheckpoint(), prepared.postPullupFinalized)
	require.Len(t, prepared.attestingIndices, len(wantIndices))
	for i, want := range wantIndices {
		require.NoError(t, prepared.attestingIndices[i].err)
		require.Equal(t, want, prepared.attestingIndices[i].indices)
	}
}
