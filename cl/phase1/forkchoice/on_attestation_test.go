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
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

type targetShufflingFixture struct {
	cfg                 *clparams.BeaconChainConfig
	headState           *state.CachingBeaconState
	targetState         *state.CachingBeaconState
	targetEpoch         uint64
	attestationSlot     uint64
	committeeIndex      uint64
	committeePosition   int
	headValidator       uint64
	targetValidator     uint64
	dependentSlot       uint64
	headDependentRoot   common.Hash
	targetDependentRoot common.Hash
	commonRoot          common.Hash
	headRoot            common.Hash
	headTargetRoot      common.Hash
	targetRoot          common.Hash
}

type attestationPayloadForkGraph struct {
	payloadVoteForkGraph
}

func (attestationPayloadForkGraph) GetHeader(common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	return &cltypes.BeaconBlockHeader{}, true
}

func TestValidateOnAttestationPayloadPresentVote(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	root := common.Hash{0x01}

	for _, test := range []struct {
		name         string
		index        uint64
		status       execution_client.PayloadStatus
		hasEnvelope  bool
		blockInvalid bool
		unavailable  bool
		wantErr      bool
	}{
		{name: "payload present optimistic", index: 1, status: execution_client.PayloadStatusNotValidated, hasEnvelope: true},
		{name: "payload present validated", index: 1, status: execution_client.PayloadStatusValidated, hasEnvelope: true},
		{name: "payload present invalidated", index: 1, status: execution_client.PayloadStatusInvalidated, hasEnvelope: true, wantErr: true},
		{name: "payload present not imported", index: 1, hasEnvelope: true, wantErr: true},
		{name: "payload present optimistic without envelope", index: 1, status: execution_client.PayloadStatusNotValidated, wantErr: true},
		{name: "payload present optimistic block invalid", index: 1, status: execution_client.PayloadStatusNotValidated, hasEnvelope: true, blockInvalid: true, wantErr: true},
		{name: "payload present optimistic payload unavailable", index: 1, status: execution_client.PayloadStatusNotValidated, hasEnvelope: true, unavailable: true, wantErr: true},
		{name: "payload absent not imported"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newPayloadVoteTestStore(t, root, false, false)
			store.beaconCfg = &cfg
			graph := payloadVoteForkGraph{
				hasEnvelope:        test.hasEnvelope,
				invalidatedHeader:  &common.Hash{},
				unavailablePayload: &common.Hash{},
				acceptedPayloads:   map[common.Hash]bool{},
			}
			store.forkGraph = attestationPayloadForkGraph{graph}
			if test.status != execution_client.PayloadStatusNone {
				store.MarkPayloadStatus(root, common.Hash{0x02}, test.status)
			}
			if test.blockInvalid {
				graph.MarkHeaderAsInvalid(root)
			}
			if test.unavailable {
				graph.MarkPayloadUnavailable(root)
			}
			attestation := &solid.Attestation{Data: &solid.AttestationData{
				Slot:            1,
				CommitteeIndex:  test.index,
				BeaconBlockRoot: root,
				Target:          solid.Checkpoint{Root: root},
			}}

			err := store.ValidateOnAttestation(attestation)
			if test.wantErr {
				require.ErrorContains(t, err, "index 1")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestOnAttestationUsesTargetShuffling(t *testing.T) {
	fixture := newTargetShufflingFixture(t)
	branchGraph := fixture.forkGraph(false)
	branchStore := &ForkChoiceStore{forkGraph: branchGraph}
	require.Equal(t, fixture.headDependentRoot, branchStore.Ancestor(fixture.headRoot, fixture.dependentSlot).Root)
	require.Equal(t, fixture.targetDependentRoot, branchStore.Ancestor(fixture.targetRoot, fixture.dependentSlot).Root)
	require.Equal(t, fixture.commonRoot, branchStore.Ancestor(fixture.headRoot, fixture.dependentSlot-1).Root)
	require.Equal(t, fixture.commonRoot, branchStore.Ancestor(fixture.targetRoot, fixture.dependentSlot-1).Root)

	for _, test := range []struct {
		name                string
		targetRoot          common.Hash
		missingTargetState  bool
		unresolvedAncestors bool
		headEpochOffset     uint64
		wantValidator       uint64
		rejectValidator     uint64
		wantStateRead       bool
		wantError           bool
	}{
		{
			name:            "side branch uses target checkpoint state",
			targetRoot:      fixture.targetRoot,
			wantValidator:   fixture.targetValidator,
			rejectValidator: fixture.headValidator,
			wantStateRead:   true,
		},
		{
			name:               "missing target checkpoint state rejects vote",
			targetRoot:         fixture.targetRoot,
			missingTargetState: true,
			wantError:          true,
		},
		{
			name:                "unresolved dependent roots use target checkpoint state",
			targetRoot:          fixture.targetRoot,
			unresolvedAncestors: true,
			wantValidator:       fixture.targetValidator,
			rejectValidator:     fixture.headValidator,
			wantStateRead:       true,
		},
		{
			name:            "target one epoch behind head keeps head state fast path",
			targetRoot:      fixture.headTargetRoot,
			headEpochOffset: 1,
			wantValidator:   fixture.headValidator,
			rejectValidator: fixture.targetValidator,
		},
		{
			name:            "target two epochs behind head uses target checkpoint state",
			targetRoot:      fixture.headTargetRoot,
			headEpochOffset: 2,
			wantValidator:   fixture.headValidator,
			rejectValidator: fixture.targetValidator,
			wantStateRead:   true,
		},
		{
			name:            "head branch keeps head state fast path",
			targetRoot:      fixture.headTargetRoot,
			wantValidator:   fixture.headValidator,
			rejectValidator: fixture.targetValidator,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			graph := fixture.forkGraph(test.unresolvedAncestors)
			headState := fixture.headState
			if test.targetRoot == fixture.targetRoot && !test.missingTargetState {
				graph.states[test.targetRoot] = fixture.targetState
			}
			if test.headEpochOffset > 0 {
				var err error
				headState, err = fixture.headState.Copy()
				require.NoError(t, err)
				require.NoError(t, transition.DefaultMachine.ProcessSlots(headState, (fixture.targetEpoch+test.headEpochOffset)*fixture.cfg.SlotsPerEpoch))
				graph.headers[fixture.headRoot].Slot = headState.Slot()
				graph.blocks[fixture.headRoot].Block.Slot = headState.Slot()
				graph.states[test.targetRoot] = fixture.headState
			}
			store := newAttestationTestStore(t, fixture.cfg, headState, fixture.headRoot, fixture.attestationSlot, graph)
			if test.headEpochOffset > 0 {
				store.timeMs.Store((store.genesisTime + (headState.Slot()+1)*fixture.cfg.SecondsPerSlot) * 1000)
			}
			attestation := &solid.Attestation{
				AggregationBits: aggregationBits(fixture.committeeSize(t, test.targetRoot), int(fixture.cfg.MaxValidatorsPerCommittee), fixture.committeePosition),
				Data: &solid.AttestationData{
					Slot:            fixture.attestationSlot,
					CommitteeIndex:  fixture.committeeIndex,
					BeaconBlockRoot: test.targetRoot,
					Target: solid.Checkpoint{
						Epoch: fixture.targetEpoch,
						Root:  test.targetRoot,
					},
				},
			}

			err := store.OnAttestation(attestation, true, false)
			if test.wantError {
				require.ErrorContains(t, err, "getCheckpointState: baseState not found in graph")
				require.Equal(t, []common.Hash{test.targetRoot}, graph.stateRoots())
				_, headCredited := store.getLatestMessage(fixture.headValidator)
				_, targetCredited := store.getLatestMessage(fixture.targetValidator)
				require.False(t, headCredited)
				require.False(t, targetCredited)
				return
			}
			require.NoError(t, err)
			message, credited := store.getLatestMessage(test.wantValidator)
			require.True(t, credited, "expected validator was not credited")
			require.Equal(t, test.targetRoot, message.Root)
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

func TestTargetBeaconCommitteeUsesCheckpointState(t *testing.T) {
	fixture := newTargetShufflingFixture(t)
	graph := fixture.forkGraph(false)
	graph.states[fixture.targetRoot] = fixture.targetState
	store := newAttestationTestStore(t, fixture.cfg, fixture.headState, fixture.headRoot, fixture.attestationSlot, graph)
	target := solid.Checkpoint{Epoch: fixture.targetEpoch, Root: fixture.targetRoot}

	committee, committeesPerSlot, err := store.TargetBeaconCommittee(target, fixture.attestationSlot, fixture.committeeIndex, nil)
	require.NoError(t, err)
	require.Equal(t, fixture.targetState.CommitteeCount(fixture.targetEpoch), committeesPerSlot)
	require.Equal(t, fixture.targetValidator, committee[fixture.committeePosition])
	require.NotContains(t, committee, fixture.headValidator)

	committee, count, err := store.TargetBeaconCommittee(target, fixture.attestationSlot, committeesPerSlot, nil)
	require.NoError(t, err)
	require.Nil(t, committee)
	require.Equal(t, committeesPerSlot, count)

	_, _, err = store.TargetBeaconCommittee(solid.Checkpoint{Epoch: fixture.targetEpoch, Root: common.Hash{0xff}}, fixture.attestationSlot, fixture.committeeIndex, nil)
	require.Error(t, err)
}

func TestTargetBeaconCommitteeRejectsTargetOutsideFinalizedChain(t *testing.T) {
	fixture := newTargetShufflingFixture(t)
	graph := fixture.forkGraph(false)
	graph.states[fixture.targetRoot] = fixture.targetState
	store := newAttestationTestStore(t, fixture.cfg, fixture.headState, fixture.headRoot, fixture.attestationSlot, graph)
	store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: fixture.targetEpoch - 1, Root: common.Hash{0xff}})

	_, _, err := store.TargetBeaconCommittee(
		solid.Checkpoint{Epoch: fixture.targetEpoch, Root: fixture.targetRoot},
		fixture.attestationSlot,
		fixture.committeeIndex,
		nil,
	)
	require.Error(t, err)
	require.Empty(t, graph.stateRoots())
	require.Empty(t, store.targetStateBuilds)

	t.Run("target is finalized root", func(t *testing.T) {
		graph := fixture.forkGraph(false)
		graph.states[fixture.targetDependentRoot] = fixture.targetState
		store := newAttestationTestStore(t, fixture.cfg, fixture.headState, fixture.headRoot, fixture.attestationSlot, graph)
		store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: fixture.targetEpoch - 1, Root: fixture.targetDependentRoot})

		_, _, err := store.TargetBeaconCommittee(
			solid.Checkpoint{Epoch: fixture.targetEpoch, Root: fixture.targetDependentRoot},
			fixture.attestationSlot,
			fixture.committeeIndex,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, []common.Hash{fixture.targetDependentRoot}, graph.stateRoots())
	})

	t.Run("different root before finalized epoch start", func(t *testing.T) {
		graph := fixture.forkGraph(false)
		graph.states[fixture.headDependentRoot] = fixture.targetState
		store := newAttestationTestStore(t, fixture.cfg, fixture.headState, fixture.headRoot, fixture.attestationSlot, graph)
		store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: fixture.targetEpoch - 1, Root: fixture.targetDependentRoot})

		_, _, err := store.TargetBeaconCommittee(
			solid.Checkpoint{Epoch: fixture.targetEpoch, Root: fixture.headDependentRoot},
			fixture.attestationSlot,
			fixture.committeeIndex,
			nil,
		)
		require.Error(t, err)
		require.Empty(t, graph.stateRoots())
	})
}

func TestTargetBeaconCommitteeLimitsUncachedBuilds(t *testing.T) {
	fixture := newTargetShufflingFixture(t)
	graph := fixture.forkGraph(false)
	graph.states[fixture.targetRoot] = fixture.targetState
	store := newAttestationTestStore(t, fixture.cfg, fixture.headState, fixture.headRoot, fixture.attestationSlot, graph)
	store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: fixture.targetEpoch - 1, Root: fixture.targetDependentRoot})
	target := solid.Checkpoint{Epoch: fixture.targetEpoch, Root: fixture.targetRoot}

	setBuildsInFlight := func(count int) {
		store.targetStateBuilds = make(map[solid.Checkpoint]*targetStateBuild, count)
		for epoch := range count {
			store.targetStateBuilds[solid.Checkpoint{Epoch: uint64(epoch), Root: common.Hash{0xee}}] = &targetStateBuild{done: make(chan struct{})}
		}
	}

	setBuildsInFlight(maxConcurrentTargetCommitteeBuilds)
	_, _, err := store.TargetBeaconCommittee(target, fixture.attestationSlot, fixture.committeeIndex, nil)
	require.Error(t, err)
	require.Empty(t, graph.stateRoots())
	require.Len(t, store.targetStateBuilds, maxConcurrentTargetCommitteeBuilds)
	headerCalls, blockCalls := graph.forkGraphCalls()
	require.Zero(t, headerCalls)
	require.Zero(t, blockCalls)

	setBuildsInFlight(maxConcurrentTargetCommitteeBuilds - 1)
	_, _, err = store.TargetBeaconCommittee(target, fixture.attestationSlot, fixture.committeeIndex, nil)
	require.NoError(t, err)
	require.Equal(t, []common.Hash{fixture.targetRoot}, graph.stateRoots())
	require.Len(t, store.targetStateBuilds, maxConcurrentTargetCommitteeBuilds-1)

	setBuildsInFlight(maxConcurrentTargetCommitteeBuilds)
	_, _, err = store.TargetBeaconCommittee(target, fixture.attestationSlot, fixture.committeeIndex, nil)
	require.NoError(t, err)
	require.Equal(t, []common.Hash{fixture.targetRoot}, graph.stateRoots())

	errorGraph := fixture.forkGraph(false)
	errorStore := newAttestationTestStore(t, fixture.cfg, fixture.headState, fixture.headRoot, fixture.attestationSlot, errorGraph)
	errorStore.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: fixture.targetEpoch - 1, Root: fixture.targetDependentRoot})
	_, _, err = errorStore.TargetBeaconCommittee(target, fixture.attestationSlot, fixture.committeeIndex, nil)
	require.Error(t, err)
	require.Empty(t, errorStore.targetStateBuilds)
}

func TestTargetBeaconCommitteeClaimsOnlyUncachedBuilds(t *testing.T) {
	fixture := newTargetShufflingFixture(t)
	graph := fixture.forkGraph(false)
	graph.states[fixture.targetRoot] = fixture.targetState
	store := newAttestationTestStore(t, fixture.cfg, fixture.headState, fixture.headRoot, fixture.attestationSlot, graph)
	store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: fixture.targetEpoch - 1, Root: fixture.targetDependentRoot})
	target := solid.Checkpoint{Epoch: fixture.targetEpoch, Root: fixture.targetRoot}
	claims := 0
	claim := func(granted bool) func() bool {
		return func() bool {
			claims++
			return granted
		}
	}

	_, _, err := store.TargetBeaconCommittee(target, fixture.attestationSlot, fixture.committeeIndex, claim(false))
	require.Error(t, err)
	require.Empty(t, graph.stateRoots())

	_, _, err = store.TargetBeaconCommittee(target, fixture.attestationSlot, fixture.committeeIndex, claim(true))
	require.NoError(t, err)
	_, _, err = store.TargetBeaconCommittee(target, fixture.attestationSlot, fixture.committeeIndex, claim(false))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{fixture.targetRoot}, graph.stateRoots())
	require.Equal(t, 2, claims)
}

func TestTargetBeaconCommitteeWaitersShareInFlightBuild(t *testing.T) {
	fixture := newTargetShufflingFixture(t)
	graph := fixture.forkGraph(false)
	graph.states[fixture.targetRoot] = fixture.targetState
	graph.getStateStarted = make(chan common.Hash, 1)
	graph.getStateRelease = make(chan struct{})
	store := newAttestationTestStore(t, fixture.cfg, fixture.headState, fixture.headRoot, fixture.attestationSlot, graph)
	store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: fixture.targetEpoch - 1, Root: fixture.targetDependentRoot})
	target := solid.Checkpoint{Epoch: fixture.targetEpoch, Root: fixture.targetRoot}
	var claims atomic.Int32
	callers := maxConcurrentTargetCommitteeBuilds + 1
	results := make(chan error, callers)
	call := func() {
		_, _, err := store.TargetBeaconCommittee(target, fixture.attestationSlot, fixture.committeeIndex, func() bool {
			claims.Add(1)
			return true
		})
		results <- err
	}

	go call()
	require.Equal(t, fixture.targetRoot, waitForStateLoad(t, graph.getStateStarted))
	for range callers - 1 {
		go call()
	}
	require.Never(t, func() bool { return len(results) > 0 }, 100*time.Millisecond, 10*time.Millisecond)
	close(graph.getStateRelease)
	for range callers {
		require.NoError(t, <-results)
	}
	require.Equal(t, int32(1), claims.Load())
	require.Equal(t, []common.Hash{fixture.targetRoot}, graph.stateRoots())
}

func TestTargetBeaconCommitteeLimitsCheckpointBlockAge(t *testing.T) {
	fixture := newTargetShufflingFixture(t)
	target := solid.Checkpoint{Epoch: fixture.targetEpoch, Root: fixture.targetRoot}
	targetSlot := fixture.targetEpoch * fixture.cfg.SlotsPerEpoch

	for _, test := range []struct {
		name      string
		target    solid.Checkpoint
		slot      uint64
		wantRead  bool
		finalized bool
	}{
		{
			name:      "older than four epochs",
			target:    target,
			slot:      targetSlot + 4*fixture.cfg.SlotsPerEpoch + 1,
			finalized: true,
		},
		{
			name:      "exactly four epochs old",
			target:    target,
			slot:      targetSlot + 4*fixture.cfg.SlotsPerEpoch,
			wantRead:  true,
			finalized: true,
		},
		{
			name:   "unknown target block",
			target: solid.Checkpoint{Epoch: fixture.targetEpoch, Root: common.Hash{0xff}},
			slot:   fixture.attestationSlot,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			graph := fixture.forkGraph(false)
			graph.states[test.target.Root] = fixture.targetState
			store := newAttestationTestStore(t, fixture.cfg, fixture.headState, fixture.headRoot, fixture.attestationSlot, graph)
			if test.finalized {
				store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: fixture.targetEpoch - 1, Root: fixture.targetDependentRoot})
			}

			_, _, err := store.TargetBeaconCommittee(test.target, test.slot, fixture.committeeIndex, nil)
			if test.wantRead {
				require.NoError(t, err)
				require.Equal(t, []common.Hash{test.target.Root}, graph.stateRoots())
				return
			}
			require.Error(t, err)
			require.Empty(t, graph.stateRoots())
		})
	}
}

func TestOnAttestationHeadStateTooOldUsesTargetState(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	baseState := decodeAttestationAnchorState(t, cfg)
	headEpoch := state.Epoch(baseState.BeaconState)

	for _, test := range []struct {
		name                   string
		targetEpochOffset      uint64
		wantStateRead          bool
		wantDifferentShuffling bool
	}{
		{
			name:              "target one epoch after head keeps head state fast path",
			targetEpochOffset: 1,
		},
		{
			name:              "target two epochs after head uses target checkpoint state",
			targetEpochOffset: 2,
			wantStateRead:     true,
		},
		{
			name:                   "target three epochs after head uses target checkpoint state",
			targetEpochOffset:      3,
			wantStateRead:          true,
			wantDifferentShuffling: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			headState, err := baseState.Copy()
			require.NoError(t, err)
			targetEpoch := headEpoch + test.targetEpochOffset
			mixPosition := (targetEpoch + cfg.EpochsPerHistoricalVector - cfg.MinSeedLookahead - 1) % cfg.EpochsPerHistoricalVector
			if test.wantDifferentShuffling {
				require.Equal(t, (headEpoch+1)%cfg.EpochsPerHistoricalVector, mixPosition)
				require.NoError(t, headState.SetRandaoMixAt(int(headEpoch%cfg.EpochsPerHistoricalVector), common.Hash{0xc1}))
				require.NoError(t, headState.SetRandaoMixAt(int(mixPosition), common.Hash{0xd1}))
			}

			checkpointBase, err := headState.Copy()
			require.NoError(t, err)
			processedState, err := headState.Copy()
			require.NoError(t, err)
			require.NoError(t, transition.DefaultMachine.ProcessSlots(processedState, targetEpoch*cfg.SlotsPerEpoch))
			if test.wantDifferentShuffling {
				require.Equal(t, common.Hash{0xc1}, common.Hash(processedState.GetRandaoMix(int(mixPosition))))
			}

			attestationSlot := targetEpoch * cfg.SlotsPerEpoch
			committeeIndex := uint64(0)
			committeePosition := 0
			targetCommittee, err := processedState.GetBeaconCommitee(attestationSlot, committeeIndex)
			require.NoError(t, err)
			headCommittee, err := headState.GetBeaconCommitee(attestationSlot, committeeIndex)
			require.NoError(t, err)
			headValidator := headCommittee[committeePosition]
			targetValidator := targetCommittee[committeePosition]
			if test.wantDifferentShuffling {
				attestationSlot, committeeIndex, committeePosition, headValidator, targetValidator = findDifferentCommitteeMember(t, headState, processedState, targetEpoch)
				require.NotEqual(t, headValidator, targetValidator)
				targetCommittee, err = processedState.GetBeaconCommitee(attestationSlot, committeeIndex)
				require.NoError(t, err)
			} else {
				require.Equal(t, headCommittee, targetCommittee)
			}

			headRoot := common.Hash{0xe1}
			dependentRoot := common.Hash{0xe2}
			dependentSlot := computeShufflingDependentSlot(targetEpoch, cfg.MinSeedLookahead, cfg.SlotsPerEpoch)
			headers := map[common.Hash]*cltypes.BeaconBlockHeader{
				headRoot: {Slot: headState.Slot()},
			}
			blocks := map[common.Hash]*cltypes.SignedBeaconBlock{}
			if dependentSlot < headState.Slot() {
				headers[dependentRoot] = &cltypes.BeaconBlockHeader{Slot: dependentSlot}
				blocks[headRoot] = testSignedBeaconBlock(cfg, headState.Slot(), dependentRoot)
			}
			graph := &getFinalizedExecutionHashForkGraph{
				headers: headers,
				blocks:  blocks,
				states: map[common.Hash]*state.CachingBeaconState{
					headRoot: checkpointBase,
				},
			}
			store := newAttestationTestStore(t, cfg, headState, headRoot, attestationSlot, graph)
			attestation := &solid.Attestation{
				AggregationBits: aggregationBits(len(targetCommittee), int(cfg.MaxValidatorsPerCommittee), committeePosition),
				Data: &solid.AttestationData{
					Slot:            attestationSlot,
					CommitteeIndex:  committeeIndex,
					BeaconBlockRoot: headRoot,
					Target:          solid.Checkpoint{Epoch: targetEpoch, Root: headRoot},
				},
			}

			require.NoError(t, store.OnAttestation(attestation, true, false))
			message, credited := store.getLatestMessage(targetValidator)
			require.True(t, credited, "target-state validator was not credited")
			require.Equal(t, headRoot, message.Root)
			if test.wantDifferentShuffling {
				_, credited = store.getLatestMessage(headValidator)
				require.False(t, credited, "old-head-state validator was credited")
			}
			if test.wantStateRead {
				require.Equal(t, []common.Hash{headRoot}, graph.stateRoots())
			} else {
				require.Empty(t, graph.stateRoots())
			}
		})
	}
}

func TestOnAttestationRandaoMixWrapUsesTargetState(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	baseState := decodeAttestationAnchorState(t, cfg)
	targetEpoch := state.Epoch(baseState.BeaconState) + 3
	headEpoch := targetEpoch + cfg.EpochsPerHistoricalVector - cfg.MinSeedLookahead - 1
	mixPosition := (targetEpoch + cfg.EpochsPerHistoricalVector - cfg.MinSeedLookahead - 1) % cfg.EpochsPerHistoricalVector

	targetState, err := baseState.Copy()
	require.NoError(t, err)
	require.NoError(t, targetState.SetSlot(targetEpoch*cfg.SlotsPerEpoch))
	require.NoError(t, targetState.SetRandaoMixAt(int(mixPosition), common.Hash{0xf1}))
	headState, err := targetState.Copy()
	require.NoError(t, err)
	require.NoError(t, headState.SetSlot(headEpoch*cfg.SlotsPerEpoch))
	require.NoError(t, headState.SetRandaoMixAt(int(mixPosition), common.Hash{0xf2}))

	attestationSlot, committeeIndex, committeePosition, headValidator, targetValidator := findDifferentCommitteeMember(t, headState, targetState, targetEpoch)
	dependentSlot := computeShufflingDependentSlot(targetEpoch, cfg.MinSeedLookahead, cfg.SlotsPerEpoch)
	dependentRoot := common.Hash{0xf3}
	targetRoot := common.Hash{0xf4}
	headRoot := common.Hash{0xf5}
	graph := &getFinalizedExecutionHashForkGraph{
		headers: map[common.Hash]*cltypes.BeaconBlockHeader{
			dependentRoot: {Slot: dependentSlot},
			targetRoot:    {Slot: targetEpoch * cfg.SlotsPerEpoch},
			headRoot:      {Slot: headState.Slot()},
		},
		blocks: map[common.Hash]*cltypes.SignedBeaconBlock{
			targetRoot: testSignedBeaconBlock(cfg, targetEpoch*cfg.SlotsPerEpoch, dependentRoot),
			headRoot:   testSignedBeaconBlock(cfg, headState.Slot(), targetRoot),
		},
		states: map[common.Hash]*state.CachingBeaconState{
			targetRoot: targetState,
		},
	}
	store := newAttestationTestStore(t, cfg, headState, headRoot, attestationSlot, graph)
	store.timeMs.Store((store.genesisTime + (headState.Slot()+1)*cfg.SecondsPerSlot) * 1000)
	targetCommittee, err := targetState.GetBeaconCommitee(attestationSlot, committeeIndex)
	require.NoError(t, err)
	attestation := &solid.Attestation{
		AggregationBits: aggregationBits(len(targetCommittee), int(cfg.MaxValidatorsPerCommittee), committeePosition),
		Data: &solid.AttestationData{
			Slot:            attestationSlot,
			CommitteeIndex:  committeeIndex,
			BeaconBlockRoot: targetRoot,
			Target:          solid.Checkpoint{Epoch: targetEpoch, Root: targetRoot},
		},
	}

	require.NoError(t, store.OnAttestation(attestation, true, false))
	message, credited := store.getLatestMessage(targetValidator)
	require.True(t, credited, "target-state validator was not credited")
	require.Equal(t, targetRoot, message.Root)
	_, credited = store.getLatestMessage(headValidator)
	require.False(t, credited, "wrapped-head-state validator was credited")
	require.Equal(t, []common.Hash{targetRoot}, graph.stateRoots())
}

func TestCheckpointStateAttestingIndiciesCoversAllCommittees(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.InitializeForkSchedule()

	beaconState := state.New(&cfg)
	beaconState.SetVersion(clparams.ElectraVersion)
	validators := make([]solid.Validator, 97)
	for i := range validators {
		var publicKey common.Bytes48
		binary.LittleEndian.PutUint64(publicKey[:8], uint64(i))
		validators[i] = solid.NewValidatorFromParameters(
			publicKey,
			common.Hash{},
			cfg.MaxEffectiveBalance,
			false,
			0,
			0,
			cfg.FarFutureEpoch,
			cfg.FarFutureEpoch,
		)
		require.NoError(t, beaconState.AddValidator(validators[i], cfg.MaxEffectiveBalance))
	}
	require.Equal(t, uint64(3), beaconState.CommitteeCount(0))
	attestationSlot := cfg.SlotsPerEpoch - 1
	firstCommittee, err := beaconState.GetBeaconCommitee(attestationSlot, 0)
	require.NoError(t, err)
	thirdCommittee, err := beaconState.GetBeaconCommitee(attestationSlot, 2)
	require.NoError(t, err)
	require.NotEmpty(t, firstCommittee)
	require.Greater(t, len(thirdCommittee), len(firstCommittee))

	thirdPosition := len(thirdCommittee) - 1
	checkpointState := newCheckpointState(
		&cfg,
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		validators,
		beaconState.RandaoMixes(),
		beaconState.GenesisValidatorsRoot(),
		beaconState.Fork(),
		beaconState.GetTotalActiveBalance(),
		0,
		solid.Checkpoint{},
	)

	bitlistLimit := int(cfg.MaxCommitteesPerSlot * cfg.MaxValidatorsPerCommittee)
	for _, test := range []struct {
		name             string
		committeeIndices []int
		aggregationBits  *solid.BitList
		wantIndices      []uint64
		wantError        string
	}{
		{
			name:             "covers all selected committees",
			committeeIndices: []int{0, 2},
			aggregationBits: aggregationBits(
				len(firstCommittee)+len(thirdCommittee),
				bitlistLimit,
				0,
				len(firstCommittee)+thirdPosition,
			),
			wantIndices: []uint64{firstCommittee[0], thirdCommittee[thirdPosition]},
		},
		{
			name:             "rejects an out-of-range committee bit",
			committeeIndices: []int{3},
			aggregationBits:  aggregationBits(1, bitlistLimit, 0),
			wantError:        "committee index 3 is out of bounds",
		},
		{
			name:             "rejects aggregation bits ending inside the second selected committee",
			committeeIndices: []int{0, 2},
			aggregationBits: aggregationBits(
				len(firstCommittee)+len(thirdCommittee)-1,
				bitlistLimit,
				0,
			),
			wantError: "aggregation bits are too short",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			committeeBits := solid.NewBitVector(int(cfg.MaxCommitteesPerSlot))
			for _, committeeIndex := range test.committeeIndices {
				require.NoError(t, committeeBits.SetBitAt(committeeIndex, true))
			}
			attestation := &solid.Attestation{
				AggregationBits: test.aggregationBits,
				Data:            &solid.AttestationData{Slot: attestationSlot},
				CommitteeBits:   committeeBits,
			}

			var indices []uint64
			var err error
			require.NotPanics(t, func() {
				indices, err = checkpointState.getAttestingIndicies(attestation, test.aggregationBits.Bytes())
			})
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.wantIndices, indices)
		})
	}
}

func newTargetShufflingFixture(t *testing.T) targetShufflingFixture {
	t.Helper()
	cfg := &clparams.MainnetBeaconConfig
	baseState := decodeAttestationAnchorState(t, cfg)
	targetEpoch := state.Epoch(baseState.BeaconState) + 3
	targetSlot := targetEpoch * cfg.SlotsPerEpoch
	headState, err := baseState.Copy()
	require.NoError(t, err)
	targetState, err := baseState.Copy()
	require.NoError(t, err)
	require.NoError(t, headState.SetSlot(targetSlot))
	require.NoError(t, targetState.SetSlot(targetSlot))
	mixPosition := (targetEpoch + cfg.EpochsPerHistoricalVector - cfg.MinSeedLookahead - 1) % cfg.EpochsPerHistoricalVector
	require.NoError(t, headState.SetRandaoMixAt(int(mixPosition), common.Hash{0xa1}))
	require.NoError(t, targetState.SetRandaoMixAt(int(mixPosition), common.Hash{0xb1}))
	attestationSlot, committeeIndex, committeePosition, headValidator, targetValidator := findDifferentCommitteeMember(t, headState, targetState, targetEpoch)
	require.NotEqual(t, headValidator, targetValidator)
	require.NoError(t, headState.SetSlot(attestationSlot+1))
	dependentSlot := computeShufflingDependentSlot(targetEpoch, cfg.MinSeedLookahead, cfg.SlotsPerEpoch)
	require.Positive(t, dependentSlot)
	return targetShufflingFixture{
		cfg:                 cfg,
		headState:           headState,
		targetState:         targetState,
		targetEpoch:         targetEpoch,
		attestationSlot:     attestationSlot,
		committeeIndex:      committeeIndex,
		committeePosition:   committeePosition,
		headValidator:       headValidator,
		targetValidator:     targetValidator,
		dependentSlot:       dependentSlot,
		headDependentRoot:   common.Hash{0xa2},
		targetDependentRoot: common.Hash{0xb2},
		commonRoot:          common.Hash{0xc2},
		headRoot:            common.Hash{0xa4},
		headTargetRoot:      common.Hash{0xa3},
		targetRoot:          common.Hash{0xb3},
	}
}

func (f targetShufflingFixture) committeeSize(t *testing.T, root common.Hash) int {
	t.Helper()
	s := f.headState
	if root == f.targetRoot {
		s = f.targetState
	}
	committee, err := s.GetBeaconCommitee(f.attestationSlot, f.committeeIndex)
	require.NoError(t, err)
	return len(committee)
}

func (f targetShufflingFixture) forkGraph(unresolvedAncestors bool) *getFinalizedExecutionHashForkGraph {
	headParent := f.headDependentRoot
	targetParent := f.targetDependentRoot
	headers := map[common.Hash]*cltypes.BeaconBlockHeader{
		f.headRoot:       {Slot: f.headState.Slot()},
		f.headTargetRoot: {Slot: f.targetEpoch * f.cfg.SlotsPerEpoch},
		f.targetRoot:     {Slot: f.targetEpoch * f.cfg.SlotsPerEpoch},
	}
	if unresolvedAncestors {
		headParent = common.Hash{0xc1}
		targetParent = common.Hash{0xd1}
	} else {
		headers[f.headDependentRoot] = &cltypes.BeaconBlockHeader{Slot: f.dependentSlot}
		headers[f.targetDependentRoot] = &cltypes.BeaconBlockHeader{Slot: f.dependentSlot}
		headers[f.commonRoot] = &cltypes.BeaconBlockHeader{Slot: f.dependentSlot - 1}
	}
	graph := &getFinalizedExecutionHashForkGraph{
		headers: headers,
		blocks: map[common.Hash]*cltypes.SignedBeaconBlock{
			f.headRoot:       testSignedBeaconBlock(f.cfg, f.headState.Slot(), f.headTargetRoot),
			f.headTargetRoot: testSignedBeaconBlock(f.cfg, f.targetEpoch*f.cfg.SlotsPerEpoch, headParent),
			f.targetRoot:     testSignedBeaconBlock(f.cfg, f.targetEpoch*f.cfg.SlotsPerEpoch, targetParent),
		},
		states: make(map[common.Hash]*state.CachingBeaconState),
	}
	if !unresolvedAncestors {
		graph.blocks[f.headDependentRoot] = testSignedBeaconBlock(f.cfg, f.dependentSlot, f.commonRoot)
		graph.blocks[f.targetDependentRoot] = testSignedBeaconBlock(f.cfg, f.dependentSlot, f.commonRoot)
	}
	return graph
}

func newAttestationTestStore(
	t *testing.T,
	cfg *clparams.BeaconChainConfig,
	headState *state.CachingBeaconState,
	headRoot common.Hash,
	attestationSlot uint64,
	graph *getFinalizedExecutionHashForkGraph,
) *ForkChoiceStore {
	t.Helper()
	syncedDataManager := synced_data.NewSyncedDataManager(cfg, true)
	require.NoError(t, syncedDataManager.OnHeadStateWithBlockRoot(headState, headRoot))
	store := &ForkChoiceStore{
		forkGraph:          graph,
		beaconCfg:          cfg,
		genesisTime:        headState.GenesisTime(),
		syncedDataManager:  syncedDataManager,
		publicKeysRegistry: public_keys_registry.NewInMemoryPublicKeysRegistry(),
		latestMessages:     newLatestMessagesStore(headState.ValidatorLength()),
	}
	store.justifiedCheckpoint.Store(solid.Checkpoint{})
	store.gloasWeightTree = newGloasWeightTree(store)
	store.timeMs.Store((store.genesisTime + (attestationSlot+1)*cfg.SecondsPerSlot) * 1000)
	store.SetSynced(true)
	return store
}

func decodeAttestationAnchorState(t *testing.T, cfg *clparams.BeaconChainConfig) *state.CachingBeaconState {
	t.Helper()
	s := state.New(cfg)
	require.NoError(t, utils.DecodeSSZSnappy(s, diffAnchorEnc, int(clparams.AltairVersion)))
	return s
}

func findDifferentCommitteeMember(
	t *testing.T,
	first *state.CachingBeaconState,
	second *state.CachingBeaconState,
	epoch uint64,
) (uint64, uint64, int, uint64, uint64) {
	t.Helper()
	for slot := epoch * first.BeaconConfig().SlotsPerEpoch; slot < (epoch+1)*first.BeaconConfig().SlotsPerEpoch; slot++ {
		for committeeIndex := range first.CommitteeCount(epoch) {
			firstCommittee, err := first.GetBeaconCommitee(slot, committeeIndex)
			require.NoError(t, err)
			secondCommittee, err := second.GetBeaconCommitee(slot, committeeIndex)
			require.NoError(t, err)
			require.Len(t, secondCommittee, len(firstCommittee))
			for position := range firstCommittee {
				if firstCommittee[position] != secondCommittee[position] {
					return slot, committeeIndex, position, firstCommittee[position], secondCommittee[position]
				}
			}
		}
	}
	t.Fatal("fixture shufflings do not differ")
	return 0, 0, 0, 0, 0
}

func aggregationBits(length, limit int, positions ...int) *solid.BitList {
	bits := make([]byte, length/8+1)
	for _, position := range positions {
		bits[position/8] |= 1 << uint(position%8)
	}
	bits[length/8] |= 1 << uint(length%8)
	return solid.BitlistFromBytes(bits, limit)
}

func testSignedBeaconBlock(cfg *clparams.BeaconChainConfig, slot uint64, parentRoot common.Hash) *cltypes.SignedBeaconBlock {
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.AltairVersion)
	block.Block.Slot = slot
	block.Block.ParentRoot = parentRoot
	return block
}
