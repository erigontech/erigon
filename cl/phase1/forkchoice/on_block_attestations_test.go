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
	store.SetSynced(synced)
	lastSlot := blocks[len(blocks)-1].Block.Slot
	store.OnTick((anchor.GenesisTime() + (lastSlot+1)*cfg.SecondsPerSlot) * 1000)
	return store
}

func countLatestMessages(store *ForkChoiceStore) int {
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
	for _, test := range []struct {
		name            string
		targetRoot      common.Hash
		wantValidator   uint64
		rejectValidator uint64
		wantStateRead   bool
	}{
		{
			name:            "side branch target uses target checkpoint state",
			targetRoot:      fixture.targetRoot,
			wantValidator:   fixture.targetValidator,
			rejectValidator: fixture.headValidator,
			wantStateRead:   true,
		},
		{
			name:            "block branch target uses block state",
			targetRoot:      fixture.headTargetRoot,
			wantValidator:   fixture.headValidator,
			rejectValidator: fixture.targetValidator,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			graph := fixture.forkGraph(false)
			graph.states[fixture.targetRoot] = fixture.targetState
			store := newAttestationTestStore(t, fixture.cfg, fixture.headState, fixture.headRoot, fixture.attestationSlot, graph)
			store.syncedDataManager.UnsetHeadState()
			attestation := &solid.Attestation{
				AggregationBits: aggregationBits(fixture.committeeSize(t, test.targetRoot), int(fixture.cfg.MaxValidatorsPerCommittee), fixture.committeePosition),
				Data: &solid.AttestationData{
					Slot:            fixture.attestationSlot,
					CommitteeIndex:  fixture.committeeIndex,
					BeaconBlockRoot: test.targetRoot,
					Target:          solid.Checkpoint{Epoch: fixture.targetEpoch, Root: test.targetRoot},
				},
			}

			store.mu.Lock()
			err := store.onAttestationLocked(attestation, true, fixture.headState, fixture.headRoot)
			store.mu.Unlock()

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
