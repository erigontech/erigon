// Copyright 2024 The Erigon Authors
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

package services

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
)

func TestAggregateAndProofServiceRejectsMalformedNestedInput(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	service := &aggregateAndProofServiceImpl{beaconCfg: &cfg}

	tests := []struct {
		name  string
		input *SignedAggregateAndProofForGossip
	}{
		{"nil wrapper", nil},
		{"nil signed message", &SignedAggregateAndProofForGossip{}},
		{"nil message", &SignedAggregateAndProofForGossip{SignedAggregateAndProof: &cltypes.SignedAggregateAndProof{}}},
		{"nil aggregate", &SignedAggregateAndProofForGossip{SignedAggregateAndProof: &cltypes.SignedAggregateAndProof{Message: &cltypes.AggregateAndProof{}}}},
		{"nil data", &SignedAggregateAndProofForGossip{SignedAggregateAndProof: &cltypes.SignedAggregateAndProof{Message: &cltypes.AggregateAndProof{Aggregate: &solid.Attestation{}}}}},
		{"nil aggregation bits", &SignedAggregateAndProofForGossip{SignedAggregateAndProof: &cltypes.SignedAggregateAndProof{Message: &cltypes.AggregateAndProof{Aggregate: &solid.Attestation{Data: &solid.AttestationData{}}}}}},
		{"nil committee bits", &SignedAggregateAndProofForGossip{SignedAggregateAndProof: &cltypes.SignedAggregateAndProof{Message: &cltypes.AggregateAndProof{Aggregate: &solid.Attestation{Data: &solid.AttestationData{}, AggregationBits: solid.NewBitList(0, 1)}}}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				require.Error(t, service.ProcessMessage(context.Background(), nil, test.input))
			})
		})
	}
}

func getAggregateAndProofAndState() (*SignedAggregateAndProofForGossip, *state.CachingBeaconState) {
	_, _, s := tests.GetBellatrixRandom()
	br, _ := s.BlockRoot()
	checkpoint := s.CurrentJustifiedCheckpoint()
	a := &SignedAggregateAndProofForGossip{
		SignedAggregateAndProof: &cltypes.SignedAggregateAndProof{
			Message: &cltypes.AggregateAndProof{
				AggregatorIndex: 141,
				// Aggregate: solid.NewAttestionFromParameters([]byte{1, 2}, solid.NewAttestionDataFromParameters(
				// 	s.Slot(),
				// 	0,
				// 	br,
				// 	checkpoint,
				// 	checkpoint,
				// ), common.Bytes96{}),
				// SelectionProof: common.Bytes96{},
				Aggregate: &solid.Attestation{
					AggregationBits: solid.BitlistFromBytes([]byte{1, 1}, 2048),
					Data: &solid.AttestationData{
						Slot:            s.Slot(),
						BeaconBlockRoot: br,
						Source:          checkpoint,
						Target:          checkpoint,
					},
				},
			},
		},
	}

	a.SignedAggregateAndProof.Message.Aggregate.Data.Target.Epoch = s.Slot() / 32
	return a, s
}

func setupAggregateAndProofTest(t *testing.T) (AggregateAndProofService, *synced_data.SyncedDataManager, *mock_services.ForkChoiceStorageMock) {
	ctx, cn := context.WithCancel(context.Background())
	cn()
	cfg := &clparams.MainnetBeaconConfig
	syncedDataManager := synced_data.NewSyncedDataManager(cfg, true)
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	p := pool.OperationsPool{}
	p.AttestationsPool = pool.NewOperationPool[common.Bytes96, *solid.Attestation](100, "test")
	verifierCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	batchSignatureVerifier := NewBatchSignatureVerifier(verifierCtx, nil)
	go batchSignatureVerifier.Start()
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, cfg)
	blockService := NewAggregateAndProofService(ctx, syncedDataManager, forkchoiceMock, clock, cfg, p, true, batchSignatureVerifier, validator_params.NewValidatorParams())
	return blockService, syncedDataManager, forkchoiceMock
}

func TestAggregateAndProofServiceUnsynced(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := getAggregateAndProofAndState()

	aggService, _, _ := setupAggregateAndProofTest(t)
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofServiceHighSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState()
	agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot = 9998898

	aggService, sd, _ := setupAggregateAndProofTest(t)
	require.NoError(t, sd.OnHeadState(s))
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofServiceBadEpoch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState()
	agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot = 0

	aggService, sd, _ := setupAggregateAndProofTest(t)
	require.NoError(t, sd.OnHeadState(s))
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofServiceNotAncestor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState()

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	require.NoError(t, sd.OnHeadState(s))
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofServiceNoHeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState()

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	require.NoError(t, sd.OnHeadState(s))
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofInvalidEpoch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState()

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	require.NoError(t, sd.OnHeadState(s))
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}
	agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Epoch = 999999
	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.ErrorContains(t, err, "invalid target epoch in aggregate and proof")
	require.NotErrorIs(t, err, ErrIgnore)
}

func TestAggregateAndProofInvalidCommittee(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState()

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	require.NoError(t, sd.OnHeadState(s))
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}
	agg.SignedAggregateAndProof.Message.AggregatorIndex = 12453224
	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.ErrorContains(t, err, "committee index not in committee")
	require.NotErrorIs(t, err, ErrIgnore)
}

func TestAggregateAndProofRejectsCommitteeRangeBeforeFinalizedAncestor(t *testing.T) {
	agg, s := getAggregateAndProofAndState()

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	require.NoError(t, sd.OnHeadState(s))
	data := agg.SignedAggregateAndProof.Message.Aggregate.Data
	data.CommitteeIndex = s.CommitteeCount(data.Target.Epoch)
	fcu.Headers[data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*clparams.MainnetBeaconConfig.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: common.Hash{0xff}}

	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.EqualError(t, err, "invalid committee index in aggregate and proof")
	require.NotErrorIs(t, err, ErrIgnore)
}

func TestAggregateAndProofAllowsNextEpochWhenForkchoiceHasSeenIt(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState()
	nextEpochSlot := s.Slot() + clparams.MainnetBeaconConfig.SlotsPerEpoch
	nextEpoch := nextEpochSlot / clparams.MainnetBeaconConfig.SlotsPerEpoch
	agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot = nextEpochSlot
	agg.SignedAggregateAndProof.Message.Aggregate.Data.Source.Epoch = nextEpoch - 1
	agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Epoch = nextEpoch

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	require.NoError(t, sd.OnHeadState(s))
	fcu.HighestSeenVal = nextEpochSlot
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*clparams.MainnetBeaconConfig.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fcu.Ancestors[nextEpochSlot] = forkchoice.ForkChoiceNode{Root: agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Root}
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}
	committee, err := s.GetBeaconCommitee(nextEpochSlot, agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex)
	require.NoError(t, err)
	require.NotEmpty(t, committee)
	agg.SignedAggregateAndProof.Message.AggregatorIndex = committee[0]

	err = aggService.ProcessMessage(context.Background(), nil, agg)
	require.NoError(t, err)
}

func TestAggregateAndProofAllowsEpochReachedByClockBeforeItsFirstBlock(t *testing.T) {
	tests := []struct {
		name       string
		epochDelta uint64
	}{
		{name: "next epoch", epochDelta: 1},
		{name: "head epoch + 2 with no later block seen", epochDelta: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			agg, s := getAggregateAndProofAndState()
			slot := s.Slot() + test.epochDelta*clparams.MainnetBeaconConfig.SlotsPerEpoch
			epoch := slot / clparams.MainnetBeaconConfig.SlotsPerEpoch
			agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot = slot
			agg.SignedAggregateAndProof.Message.Aggregate.Data.Source.Epoch = epoch - 1
			agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Epoch = epoch

			aggService, sd, fcu := setupAggregateAndProofTest(t)
			require.NoError(t, sd.OnHeadState(s))
			fcu.HighestSeenVal = s.Slot()
			fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
			fcu.Ancestors[s.FinalizedCheckpoint().Epoch*clparams.MainnetBeaconConfig.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
			fcu.Ancestors[slot] = forkchoice.ForkChoiceNode{Root: agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Root}
			fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}
			committee, err := s.GetBeaconCommitee(slot, agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex)
			require.NoError(t, err)
			require.NotEmpty(t, committee)
			agg.SignedAggregateAndProof.Message.AggregatorIndex = committee[0]

			clock := eth_clock.NewMockEthereumClock(ctrl)
			clock.EXPECT().GetCurrentSlot().Return(slot).AnyTimes()
			aggService.(*aggregateAndProofServiceImpl).ethClock = clock

			require.NoError(t, aggService.ProcessMessage(context.Background(), nil, agg))
		})
	}
}

func TestAggregateAndProofRejectsNextEpochBeforeForkchoiceHasSeenIt(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState()
	nextEpochSlot := s.Slot() + clparams.MainnetBeaconConfig.SlotsPerEpoch
	nextEpoch := nextEpochSlot / clparams.MainnetBeaconConfig.SlotsPerEpoch
	agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot = nextEpochSlot
	agg.SignedAggregateAndProof.Message.Aggregate.Data.Source.Epoch = nextEpoch - 1
	agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Epoch = nextEpoch

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	require.NoError(t, sd.OnHeadState(s))
	fcu.HighestSeenVal = s.Slot()

	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "epoch outside validation range")
}

func TestAggregateAndProofRejectsBeyondNextEpochDespiteForkchoiceHavingSeenIt(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState()
	beyondNextEpochSlot := s.Slot() + 2*clparams.MainnetBeaconConfig.SlotsPerEpoch
	beyondNextEpoch := beyondNextEpochSlot / clparams.MainnetBeaconConfig.SlotsPerEpoch
	agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot = beyondNextEpochSlot
	agg.SignedAggregateAndProof.Message.Aggregate.Data.Source.Epoch = beyondNextEpoch - 1
	agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Epoch = beyondNextEpoch

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	require.NoError(t, sd.OnHeadState(s))
	fcu.HighestSeenVal = beyondNextEpochSlot

	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "epoch outside validation range")
}

func TestAggregateAndProofAncestorMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState()

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	require.NoError(t, sd.OnHeadState(s))
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}
	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.ErrorContains(t, err, "invalid target block")
	require.NotErrorIs(t, err, ErrIgnore)
}

func TestAggregateAndProofAlreadySeenBeforeTargetAncestor(t *testing.T) {
	agg, s := getAggregateAndProofAndState()

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	require.NoError(t, sd.OnHeadState(s))
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*clparams.MainnetBeaconConfig.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}
	message := agg.SignedAggregateAndProof.Message
	aggService.(*aggregateAndProofServiceImpl).seenAggreatorIndexes.Add(seenAggregateIndex{
		epoch: message.Aggregate.Data.Target.Epoch,
		index: message.AggregatorIndex,
	}, common.Hash{0x81})

	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.ErrorIs(t, err, ErrIgnore)
	require.ErrorIs(t, err, ErrAggregatorAlreadyKnown)
}

func TestAggregateAndProofSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState()

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	require.NoError(t, sd.OnHeadState(s))
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fcu.Ancestors[agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot] = forkchoice.ForkChoiceNode{Root: agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Root}
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}
	require.NoError(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

type captureAttestingIndicesForkChoice struct {
	*mock_services.ForkChoiceStorageMock
	attestingIndices []uint64
}

func (f *captureAttestingIndicesForkChoice) ProcessAttestingIndicies(_ *solid.Attestation, indices []uint64) {
	f.attestingIndices = append([]uint64(nil), indices...)
}

func targetCommitteeOutsideHead(t *testing.T, s *state.CachingBeaconState, committee []uint64, count int) []uint64 {
	t.Helper()
	target := make([]uint64, 0, count)
	for index := range s.ValidatorLength() {
		if !slices.Contains(committee, uint64(index)) {
			target = append(target, uint64(index))
			if len(target) == count {
				return target
			}
		}
	}
	t.Fatalf("only found %d validators outside the head committee", len(target))
	return nil
}

func prepareTargetShufflingAggregate(
	t *testing.T,
	s *state.CachingBeaconState,
	fc *mock_services.ForkChoiceStorageMock,
	aggregate *SignedAggregateAndProofForGossip,
	targetCommittee []uint64,
	includeHeader bool,
) {
	t.Helper()
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func([][]byte, [][]byte, [][]byte) (bool, error) { return true, nil }
	data := aggregate.SignedAggregateAndProof.Message.Aggregate.Data
	epoch := data.Slot / clparams.MainnetBeaconConfig.SlotsPerEpoch
	expectedCommitteeIndex := data.CommitteeIndex
	if committeeBits := aggregate.SignedAggregateAndProof.Message.Aggregate.CommitteeBits; committeeBits != nil {
		indices := committeeBits.GetOnIndices()
		require.Len(t, indices, 1)
		expectedCommitteeIndex = uint64(indices[0])
	}
	fc.HeadLacksTargetShuffling = true
	fc.TargetBeaconCommitteeFn = func(target solid.Checkpoint, slot, committeeIndex uint64, _ func() bool) ([]uint64, uint64, error) {
		require.Equal(t, data.Target, target)
		require.Equal(t, data.Slot, slot)
		require.Equal(t, expectedCommitteeIndex, committeeIndex)
		return targetCommittee, s.CommitteeCount(epoch), nil
	}
	fc.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fc.Ancestors[s.FinalizedCheckpoint().Epoch*clparams.MainnetBeaconConfig.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fc.Ancestors[epoch*clparams.MainnetBeaconConfig.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: data.Target.Root}
	if includeHeader {
		fc.Headers[data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{Slot: data.Slot}
	}
}

func TestAggregateAndProofUsesTargetShuffling(t *testing.T) {
	aggregate, s := getAggregateAndProofAndState()
	service, syncedDataManager, fc := setupAggregateAndProofGossipTestWithConfig(t, &clparams.MainnetBeaconConfig)
	require.NoError(t, syncedDataManager.OnHeadState(s))
	data := aggregate.SignedAggregateAndProof.Message.Aggregate.Data
	headCommittee, err := s.GetBeaconCommitee(data.Slot, data.CommitteeIndex)
	require.NoError(t, err)
	require.NotEmpty(t, headCommittee)
	targetCommittee := targetCommitteeOutsideHead(t, s, headCommittee, 2)
	prepareTargetShufflingAggregate(t, s, fc, aggregate, targetCommittee, true)
	var callOrder []string
	blsVerifyMultipleSignatures = func([][]byte, [][]byte, [][]byte) (bool, error) {
		callOrder = append(callOrder, "bls")
		return true, nil
	}
	targetCommitteeFn := fc.TargetBeaconCommitteeFn
	fc.TargetBeaconCommitteeFn = func(target solid.Checkpoint, slot, committeeIndex uint64, claimBuild func() bool) ([]uint64, uint64, error) {
		require.Equal(t, []string{"bls"}, callOrder)
		callOrder = append(callOrder, "target")
		return targetCommitteeFn(target, slot, committeeIndex, claimBuild)
	}
	aggregate.SignedAggregateAndProof.Message.AggregatorIndex = targetCommittee[0]
	aggregate.SignedAggregateAndProof.Message.Aggregate.AggregationBits = solid.BitlistFromBytes([]byte{0b111}, 2048)
	aggregate.ImmediateProcess = true
	for i := range 256 {
		aggregate.SignedAggregateAndProof.Message.SelectionProof[0] = byte(i)
		if state.IsAggregator(&clparams.MainnetBeaconConfig, uint64(len(targetCommittee)), aggregate.SignedAggregateAndProof.Message.SelectionProof) {
			break
		}
	}
	capture := &captureAttestingIndicesForkChoice{ForkChoiceStorageMock: fc}
	service.(*aggregateAndProofServiceImpl).forkchoiceStore = capture

	require.NoError(t, service.ProcessMessage(context.Background(), nil, aggregate))
	require.Equal(t, targetCommittee, capture.attestingIndices)
	require.Equal(t, []string{"bls", "target", "bls"}, callOrder)
}

func TestAggregateAndProofRejectsInvalidSignatureBeforeTargetShuffling(t *testing.T) {
	aggregate, s := getAggregateAndProofAndState()
	service, syncedDataManager, fc := setupAggregateAndProofTest(t)
	require.NoError(t, syncedDataManager.OnHeadState(s))
	data := aggregate.SignedAggregateAndProof.Message.Aggregate.Data
	headCommittee, err := s.GetBeaconCommitee(data.Slot, data.CommitteeIndex)
	require.NoError(t, err)
	targetCommittee := targetCommitteeOutsideHead(t, s, headCommittee, 2)
	prepareTargetShufflingAggregate(t, s, fc, aggregate, targetCommittee, true)
	targetCalls := 0
	targetCommitteeFn := fc.TargetBeaconCommitteeFn
	fc.TargetBeaconCommitteeFn = func(target solid.Checkpoint, slot, committeeIndex uint64, claimBuild func() bool) ([]uint64, uint64, error) {
		targetCalls++
		return targetCommitteeFn(target, slot, committeeIndex, claimBuild)
	}
	blsVerifyMultipleSignatures = func([][]byte, [][]byte, [][]byte) (bool, error) { return false, nil }
	aggregate.SignedAggregateAndProof.Message.AggregatorIndex = targetCommittee[0]
	aggregate.SignedAggregateAndProof.Message.Aggregate.AggregationBits = solid.BitlistFromBytes([]byte{0b111}, 2048)

	err = service.ProcessMessage(context.Background(), nil, aggregate)
	require.ErrorIs(t, err, ErrInvalidBlsSignature)
	require.NotErrorIs(t, err, ErrIgnore)
	require.Zero(t, targetCalls)
}

func TestAggregateAndProofAlreadySeenBeforeTargetShuffling(t *testing.T) {
	aggregate, s := getAggregateAndProofAndState()
	service, syncedDataManager, fc := setupAggregateAndProofTest(t)
	require.NoError(t, syncedDataManager.OnHeadState(s))
	data := aggregate.SignedAggregateAndProof.Message.Aggregate.Data
	headCommittee, err := s.GetBeaconCommitee(data.Slot, data.CommitteeIndex)
	require.NoError(t, err)
	targetCommittee := targetCommitteeOutsideHead(t, s, headCommittee, 2)
	prepareTargetShufflingAggregate(t, s, fc, aggregate, targetCommittee, true)
	targetCalls := 0
	targetCommitteeFn := fc.TargetBeaconCommitteeFn
	fc.TargetBeaconCommitteeFn = func(target solid.Checkpoint, slot, committeeIndex uint64, claimBuild func() bool) ([]uint64, uint64, error) {
		targetCalls++
		return targetCommitteeFn(target, slot, committeeIndex, claimBuild)
	}
	aggregatorIndex := targetCommittee[0]
	aggregate.SignedAggregateAndProof.Message.AggregatorIndex = aggregatorIndex
	aggregate.SignedAggregateAndProof.Message.Aggregate.AggregationBits = solid.BitlistFromBytes([]byte{0b111}, 2048)
	aggregate.ImmediateProcess = true
	aggregateRoot, err := aggregate.SignedAggregateAndProof.HashSSZ()
	require.NoError(t, err)
	service.(*aggregateAndProofServiceImpl).seenAggreatorIndexes.Add(seenAggregateIndex{
		epoch: data.Target.Epoch,
		index: aggregatorIndex,
	}, aggregateRoot)

	err = service.ProcessMessage(context.Background(), nil, aggregate)
	require.ErrorIs(t, err, ErrIgnore)
	require.ErrorIs(t, err, ErrAggregatorAlreadySeen)
	require.Zero(t, targetCalls)
}

func TestAggregateAndProofBuildsUncachedTargetOncePerAggregatorAndEpoch(t *testing.T) {
	aggregate, s := getAggregateAndProofAndState()
	service, syncedDataManager, fc := setupAggregateAndProofTest(t)
	require.NoError(t, syncedDataManager.OnHeadState(s))
	data := aggregate.SignedAggregateAndProof.Message.Aggregate.Data
	headCommittee, err := s.GetBeaconCommitee(data.Slot, data.CommitteeIndex)
	require.NoError(t, err)
	targetCommittee := targetCommitteeOutsideHead(t, s, headCommittee, 2)
	prepareTargetShufflingAggregate(t, s, fc, aggregate, targetCommittee, true)
	builds := 0
	fc.TargetBeaconCommitteeFn = cachingTargetBeaconCommittee(targetCommittee, s.CommitteeCount(data.Target.Epoch), &builds)
	secondBlockRoot, secondTargetRoot := common.Hash{0x83}, common.Hash{0x84}
	fc.Headers[secondBlockRoot] = &cltypes.BeaconBlockHeader{Slot: data.Slot}
	targetSlot := data.Target.Epoch * clparams.MainnetBeaconConfig.SlotsPerEpoch
	fc.AncestorFn = func(root common.Hash, slot uint64) forkchoice.ForkChoiceNode {
		if root == secondBlockRoot && slot == targetSlot {
			return forkchoice.ForkChoiceNode{Root: secondTargetRoot}
		}
		return fc.Ancestors[slot]
	}
	aggregate.SignedAggregateAndProof.Message.AggregatorIndex = targetCommittee[0]
	aggregate.SignedAggregateAndProof.Message.Aggregate.AggregationBits = solid.BitlistFromBytes([]byte{0b1111}, 2048)

	err = service.ProcessMessage(context.Background(), nil, aggregate)
	require.ErrorContains(t, err, "aggregation bits length")
	require.NotErrorIs(t, err, ErrIgnore)

	data.BeaconBlockRoot, data.Target.Root = secondBlockRoot, secondTargetRoot
	err = service.ProcessMessage(context.Background(), nil, aggregate)
	require.ErrorIs(t, err, ErrIgnore)
	require.Equal(t, 1, builds)
}

func TestAggregateAndProofRejectsAggregatorOutsideTargetCommittee(t *testing.T) {
	aggregate, s := getAggregateAndProofAndState()
	service, syncedDataManager, fc := setupAggregateAndProofTest(t)
	require.NoError(t, syncedDataManager.OnHeadState(s))
	data := aggregate.SignedAggregateAndProof.Message.Aggregate.Data
	headCommittee, err := s.GetBeaconCommitee(data.Slot, data.CommitteeIndex)
	require.NoError(t, err)
	require.NotEmpty(t, headCommittee)
	targetCommittee := targetCommitteeOutsideHead(t, s, headCommittee, 2)
	prepareTargetShufflingAggregate(t, s, fc, aggregate, targetCommittee, true)
	aggregate.SignedAggregateAndProof.Message.AggregatorIndex = headCommittee[0]
	aggregate.SignedAggregateAndProof.Message.Aggregate.AggregationBits = solid.BitlistFromBytes([]byte{0b111}, 2048)

	err = service.ProcessMessage(context.Background(), nil, aggregate)
	require.ErrorContains(t, err, "committee index not in committee")
	require.NotErrorIs(t, err, ErrIgnore)
}

func TestAggregateAndProofElectraUsesCommitteeBitsIndexForTargetShuffling(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = cfg.FarFutureEpoch
	cfg.GloasForkEpoch = cfg.FarFutureEpoch
	cfg.TargetCommitteeSize = 1
	aggregate, fixtureState := getAggregateAndProofAndStateForVersion(t, clparams.ElectraVersion)
	encodedState, err := fixtureState.EncodeSSZ(nil)
	require.NoError(t, err)
	s := state.New(&cfg)
	require.NoError(t, s.DecodeSSZ(encodedState, int(clparams.BellatrixVersion)))
	committeeBits := solid.NewBitVector(64)
	require.NoError(t, committeeBits.SetBitAt(1, true))
	aggregate.SignedAggregateAndProof.Message.Aggregate.CommitteeBits = committeeBits
	data := aggregate.SignedAggregateAndProof.Message.Aggregate.Data
	require.Zero(t, data.CommitteeIndex)
	require.Greater(t, s.CommitteeCount(data.Target.Epoch), uint64(1))

	service, syncedDataManager, fc := setupAggregateAndProofTestWithConfig(t, &cfg)
	require.NoError(t, syncedDataManager.OnHeadState(s))
	headCommittee, err := s.GetBeaconCommitee(data.Slot, 1)
	require.NoError(t, err)
	targetCommittee := targetCommitteeOutsideHead(t, s, headCommittee, 2)
	prepareTargetShufflingAggregate(t, s, fc, aggregate, targetCommittee, true)
	aggregate.SignedAggregateAndProof.Message.AggregatorIndex = targetCommittee[0]
	aggregate.SignedAggregateAndProof.Message.Aggregate.AggregationBits = solid.BitlistFromBytes([]byte{0b111}, 2048)

	require.NoError(t, service.ProcessMessage(context.Background(), nil, aggregate))
}

func TestAggregateAndProofBuildsVotedBlockTargetBeforeRejectingMessageTarget(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = cfg.FarFutureEpoch
	cfg.GloasForkEpoch = cfg.FarFutureEpoch
	aggregate, s := getAggregateAndProofAndStateForVersion(t, clparams.ElectraVersion)
	service, syncedDataManager, fc := setupAggregateAndProofTestWithConfig(t, &cfg)
	require.NoError(t, syncedDataManager.OnHeadState(s))
	data := aggregate.SignedAggregateAndProof.Message.Aggregate.Data
	committeeIndex := uint64(aggregate.SignedAggregateAndProof.Message.Aggregate.CommitteeBits.GetOnIndices()[0])
	headCommittee, err := s.GetBeaconCommitee(data.Slot, committeeIndex)
	require.NoError(t, err)
	targetCommittee := targetCommitteeOutsideHead(t, s, headCommittee, 2)
	prepareTargetShufflingAggregate(t, s, fc, aggregate, targetCommittee, true)
	derivedRoot := common.Hash{0x82}
	fc.AncestorFn = func(root common.Hash, slot uint64) forkchoice.ForkChoiceNode {
		if root == data.BeaconBlockRoot && slot == data.Target.Epoch*cfg.SlotsPerEpoch {
			return forkchoice.ForkChoiceNode{Root: derivedRoot}
		}
		return fc.Ancestors[slot]
	}
	var gotTarget solid.Checkpoint
	fc.TargetBeaconCommitteeFn = func(target solid.Checkpoint, slot, gotCommitteeIndex uint64, _ func() bool) ([]uint64, uint64, error) {
		gotTarget = target
		require.Equal(t, data.Slot, slot)
		require.Equal(t, committeeIndex, gotCommitteeIndex)
		return targetCommittee, s.CommitteeCount(data.Target.Epoch), nil
	}
	aggregate.SignedAggregateAndProof.Message.AggregatorIndex = targetCommittee[0]
	aggregate.SignedAggregateAndProof.Message.Aggregate.AggregationBits = solid.BitlistFromBytes([]byte{0b111}, 2048)

	err = service.ProcessMessage(context.Background(), nil, aggregate)
	require.Equal(t, solid.Checkpoint{Epoch: data.Target.Epoch, Root: derivedRoot}, gotTarget)
	require.ErrorContains(t, err, "invalid target block")
	require.NotErrorIs(t, err, ErrIgnore)
}

func TestAggregateAndProofUnseenBlockIsIgnoredBeforeCommitteeRange(t *testing.T) {
	aggregate, s := getAggregateAndProofAndState()
	service, syncedDataManager, fc := setupAggregateAndProofTest(t)
	require.NoError(t, syncedDataManager.OnHeadState(s))
	data := aggregate.SignedAggregateAndProof.Message.Aggregate.Data
	data.CommitteeIndex = s.CommitteeCount(data.Target.Epoch)
	prepareTargetShufflingAggregate(t, s, fc, aggregate, nil, false)

	err := service.ProcessMessage(context.Background(), nil, aggregate)
	require.ErrorIs(t, err, ErrIgnore)
}

func TestAggregateAndProofIgnoresUnknownVotedBlockTargetAncestor(t *testing.T) {
	aggregate, s := getAggregateAndProofAndState()
	service, syncedDataManager, fc := setupAggregateAndProofTest(t)
	require.NoError(t, syncedDataManager.OnHeadState(s))
	data := aggregate.SignedAggregateAndProof.Message.Aggregate.Data
	headCommittee, err := s.GetBeaconCommitee(data.Slot, data.CommitteeIndex)
	require.NoError(t, err)
	targetCommittee := targetCommitteeOutsideHead(t, s, headCommittee, 2)
	prepareTargetShufflingAggregate(t, s, fc, aggregate, targetCommittee, true)
	blsCalls := 0
	blsVerifyMultipleSignatures = func([][]byte, [][]byte, [][]byte) (bool, error) {
		blsCalls++
		return true, nil
	}
	targetCalls := 0
	fc.TargetBeaconCommitteeFn = func(target solid.Checkpoint, slot, committeeIndex uint64, _ func() bool) ([]uint64, uint64, error) {
		targetCalls++
		require.Equal(t, data.Target, target)
		require.Equal(t, data.Slot, slot)
		require.Equal(t, data.CommitteeIndex, committeeIndex)
		return nil, 0, errors.New("target state unavailable")
	}
	delete(fc.Ancestors, data.Target.Epoch*clparams.MainnetBeaconConfig.SlotsPerEpoch)
	aggregate.SignedAggregateAndProof.Message.AggregatorIndex = targetCommittee[0]
	aggregate.SignedAggregateAndProof.Message.Aggregate.AggregationBits = solid.BitlistFromBytes([]byte{0b111}, 2048)

	err = service.ProcessMessage(context.Background(), nil, aggregate)
	require.ErrorIs(t, err, ErrIgnore)
	require.Equal(t, []int{0, 0}, []int{blsCalls, targetCalls})
}

func TestAggregateAndProofIgnoresUnavailableTargetCheckpointState(t *testing.T) {
	aggregate, s := getAggregateAndProofAndState()
	service, syncedDataManager, fc := setupAggregateAndProofTest(t)
	require.NoError(t, syncedDataManager.OnHeadState(s))
	data := aggregate.SignedAggregateAndProof.Message.Aggregate.Data
	headCommittee, err := s.GetBeaconCommitee(data.Slot, data.CommitteeIndex)
	require.NoError(t, err)
	targetCommittee := targetCommitteeOutsideHead(t, s, headCommittee, 2)
	prepareTargetShufflingAggregate(t, s, fc, aggregate, targetCommittee, true)
	blsCalls := 0
	blsVerifyMultipleSignatures = func([][]byte, [][]byte, [][]byte) (bool, error) {
		blsCalls++
		return true, nil
	}
	targetCalls := 0
	fc.TargetBeaconCommitteeFn = func(target solid.Checkpoint, slot, committeeIndex uint64, _ func() bool) ([]uint64, uint64, error) {
		targetCalls++
		require.Equal(t, data.Target, target)
		require.Equal(t, data.Slot, slot)
		require.Equal(t, data.CommitteeIndex, committeeIndex)
		return nil, 0, errors.New("target state unavailable")
	}
	aggregate.SignedAggregateAndProof.Message.AggregatorIndex = targetCommittee[0]
	aggregate.SignedAggregateAndProof.Message.Aggregate.AggregationBits = solid.BitlistFromBytes([]byte{0b111}, 2048)

	err = service.ProcessMessage(context.Background(), nil, aggregate)
	require.ErrorIs(t, err, ErrIgnore)
	require.Equal(t, []int{1, 1}, []int{blsCalls, targetCalls})
}

func TestAggregateAndProofGossipDoesNotAcceptBeforeBLSVerification(t *testing.T) {
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		return false, nil
	}

	agg, s := getAggregateAndProofAndState()
	setValidAggregateSelectionProof(t, &clparams.MainnetBeaconConfig, agg, s)

	service, syncedDataManager, forkchoiceMock := setupAggregateAndProofGossipTestWithConfig(t, &clparams.MainnetBeaconConfig)
	require.NoError(t, syncedDataManager.OnHeadState(s))
	forkchoiceMock.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	forkchoiceMock.Ancestors[s.FinalizedCheckpoint().Epoch*clparams.MainnetBeaconConfig.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	forkchoiceMock.Ancestors[agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot] = forkchoice.ForkChoiceNode{Root: agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Root}
	forkchoiceMock.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}

	err := service.ProcessMessage(context.Background(), nil, agg)
	require.ErrorIs(t, err, ErrInvalidBlsSignature)
}

func TestAggregateAndProofImmediateRetryReturnsAlreadySeenOnlyForSameAggregate(t *testing.T) {
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		return true, nil
	}

	cfg := clparams.MainnetBeaconConfig
	agg, s := getAggregateAndProofAndState()
	epoch := agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot / cfg.SlotsPerEpoch
	service, _ := setupValidAggregateGossipMessage(t, &cfg, agg, s, cfg.GetCurrentStateVersion(epoch))
	digest, err := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg).ComputeForkDigest(epoch)
	require.NoError(t, err)
	agg.ImmediateProcess = true
	agg.TopicForkDigest = &digest

	require.NoError(t, service.ProcessMessage(context.Background(), nil, agg))
	err = service.ProcessMessage(context.Background(), nil, agg)
	require.ErrorIs(t, err, ErrIgnore)
	require.ErrorIs(t, err, ErrAggregatorAlreadySeen)
	require.Contains(t, err.Error(), "ignore")

	differentSignedAggregate := *agg.SignedAggregateAndProof
	differentSignedAggregate.Signature[0] ^= 1
	differentAggregate := *agg
	differentAggregate.SignedAggregateAndProof = &differentSignedAggregate
	err = service.ProcessMessage(context.Background(), nil, &differentAggregate)
	require.ErrorIs(t, err, ErrIgnore)
	require.ErrorIs(t, err, ErrAggregatorAlreadyKnown)
	require.NotErrorIs(t, err, ErrAggregatorAlreadySeen)
}

func setValidAggregateSelectionProof(t *testing.T, cfg *clparams.BeaconChainConfig, agg *SignedAggregateAndProofForGossip, s *state.CachingBeaconState) {
	t.Helper()
	committee, err := s.GetBeaconCommitee(
		agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot,
		agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex,
	)
	require.NoError(t, err)
	for i := range 256 {
		agg.SignedAggregateAndProof.Message.SelectionProof[0] = byte(i)
		if state.IsAggregator(cfg, uint64(len(committee)), agg.SignedAggregateAndProof.Message.SelectionProof) {
			return
		}
	}
	t.Fatal("no valid selection proof found")
}

func TestSyncMapRangeDeadlock(t *testing.T) {
	var m sync.Map
	m.Store(1, 1)
	m.Store(2, 2)
	m.Store(3, 3)

	m.Range(func(key, value any) bool {
		m.Store(4, 5)
		return true
	})
}

// getAggregateAndProofAndStateForVersion creates test data for a specific version.
// For Electra+, it sets up CommitteeBits properly.
func getAggregateAndProofAndStateForVersion(t *testing.T, version clparams.StateVersion) (*SignedAggregateAndProofForGossip, *state.CachingBeaconState) {
	_, _, s := tests.GetBellatrixRandom()
	br, _ := s.BlockRoot()
	checkpoint := s.CurrentJustifiedCheckpoint()

	a := &SignedAggregateAndProofForGossip{
		SignedAggregateAndProof: &cltypes.SignedAggregateAndProof{
			Message: &cltypes.AggregateAndProof{
				AggregatorIndex: 141,
				Aggregate: &solid.Attestation{
					AggregationBits: solid.BitlistFromBytes([]byte{1, 1}, 2048),
					Data: &solid.AttestationData{
						Slot:            s.Slot(),
						BeaconBlockRoot: br,
						Source:          checkpoint,
						Target:          checkpoint,
					},
				},
			},
		},
	}

	// For Electra+, set up CommitteeBits with exactly one bit set
	if version >= clparams.ElectraVersion {
		committeeBits := solid.NewBitVector(64)
		require.NoError(t, committeeBits.SetBitAt(0, true)) // Set committee index 0
		a.SignedAggregateAndProof.Message.Aggregate.CommitteeBits = committeeBits
	}

	a.SignedAggregateAndProof.Message.Aggregate.Data.Target.Epoch = s.Slot() / 32
	return a, s
}

// setupAggregateAndProofTestWithConfig creates a test setup with custom config
func setupAggregateAndProofTestWithConfig(t *testing.T, cfg *clparams.BeaconChainConfig) (AggregateAndProofService, *synced_data.SyncedDataManager, *mock_services.ForkChoiceStorageMock) {
	ctx, cn := context.WithCancel(context.Background())
	cn()
	syncedDataManager := synced_data.NewSyncedDataManager(cfg, true)
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	p := pool.OperationsPool{}
	p.AttestationsPool = pool.NewOperationPool[common.Bytes96, *solid.Attestation](100, "test")
	verifierCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	batchSignatureVerifier := NewBatchSignatureVerifier(verifierCtx, nil)
	go batchSignatureVerifier.Start()
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, cfg)
	blockService := NewAggregateAndProofService(ctx, syncedDataManager, forkchoiceMock, clock, cfg, p, true, batchSignatureVerifier, validator_params.NewValidatorParams())
	return blockService, syncedDataManager, forkchoiceMock
}

func setupAggregateAndProofGossipTestWithConfig(t *testing.T, cfg *clparams.BeaconChainConfig) (AggregateAndProofService, *synced_data.SyncedDataManager, *mock_services.ForkChoiceStorageMock) {
	service, syncedDataManager, forkchoiceMock := setupAggregateAndProofTestWithConfig(t, cfg)
	serviceImpl := service.(*aggregateAndProofServiceImpl)
	serviceImpl.test = false
	serviceImpl.validatorParams.SetFeeRecipient(0, common.Address{})
	return service, syncedDataManager, forkchoiceMock
}

func setupValidAggregateGossipMessage(
	t *testing.T,
	cfg *clparams.BeaconChainConfig,
	agg *SignedAggregateAndProofForGossip,
	s *state.CachingBeaconState,
	encodingVersion clparams.StateVersion,
) (AggregateAndProofService, []byte) {
	t.Helper()
	setValidAggregateSelectionProof(t, cfg, agg, s)
	agg.SignedAggregateAndProof.SetVersion(encodingVersion)
	encoded, err := agg.SignedAggregateAndProof.EncodeSSZ(nil)
	require.NoError(t, err)

	service, syncedDataManager, forkchoiceMock := setupAggregateAndProofGossipTestWithConfig(t, cfg)
	require.NoError(t, syncedDataManager.OnHeadState(s))
	forkchoiceMock.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	forkchoiceMock.Ancestors[s.FinalizedCheckpoint().Epoch*cfg.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	forkchoiceMock.Ancestors[agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot] = forkchoice.ForkChoiceNode{Root: agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Root}
	forkchoiceMock.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}
	return service, encoded
}

func TestAggregateAndProofGossipNewForkAggregateOnOldForkTopicNotAccepted(t *testing.T) {
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		return true, nil
	}

	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.GloasVersion)
	aggregateEpoch := agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot / cfg.SlotsPerEpoch
	require.Positive(t, aggregateEpoch)
	cfg.GloasForkEpoch = aggregateEpoch
	service, encoded := setupValidAggregateGossipMessage(t, &cfg, agg, s, clparams.GloasVersion)

	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	oldDigest, err := clock.ComputeForkDigest(aggregateEpoch - 1)
	require.NoError(t, err)
	message, err := service.DecodeGossipMessage("peer", encoded, clparams.FuluVersion)
	require.NoError(t, err)
	message.SetTopicForkDigest(oldDigest)

	err = service.ProcessMessage(context.Background(), nil, message)
	require.ErrorIs(t, err, ErrIgnore)
}

func TestAggregateAndProofGossipRejectsDifferentBPOForkDigest(t *testing.T) {
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		return true, nil
	}

	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = cfg.FarFutureEpoch

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.FuluVersion)
	aggregateEpoch := agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot / cfg.SlotsPerEpoch
	require.Positive(t, aggregateEpoch)
	cfg.BlobSchedule = []clparams.BlobParameters{
		{Epoch: aggregateEpoch - 1, MaxBlobsPerBlock: 15},
		{Epoch: aggregateEpoch, MaxBlobsPerBlock: 21},
	}
	service, encoded := setupValidAggregateGossipMessage(t, &cfg, agg, s, clparams.FuluVersion)

	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	oldDigest, err := clock.ComputeForkDigest(aggregateEpoch - 1)
	require.NoError(t, err)
	messageDigest, err := clock.ComputeForkDigest(aggregateEpoch)
	require.NoError(t, err)
	require.NotEqual(t, oldDigest, messageDigest)

	oldTopicMessage, err := service.DecodeGossipMessage("peer", encoded, clparams.FuluVersion)
	require.NoError(t, err)
	oldTopicMessage.SetTopicForkDigest(oldDigest)
	require.ErrorIs(t, service.ProcessMessage(context.Background(), nil, oldTopicMessage), ErrIgnore)

	matchingTopicMessage, err := service.DecodeGossipMessage("peer", encoded, clparams.FuluVersion)
	require.NoError(t, err)
	matchingTopicMessage.SetTopicForkDigest(messageDigest)
	require.NoError(t, service.ProcessMessage(context.Background(), nil, matchingTopicMessage))
}

func TestAggregateAndProofGossipPreForkAggregateOnPostForkTopicNotAccepted(t *testing.T) {
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		return true, nil
	}

	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.FuluVersion)
	aggregateEpoch := agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot / cfg.SlotsPerEpoch
	cfg.GloasForkEpoch = aggregateEpoch + 1
	setValidAggregateSelectionProof(t, &cfg, agg, s)
	agg.SignedAggregateAndProof.SetVersion(clparams.FuluVersion)
	encoded, err := agg.SignedAggregateAndProof.EncodeSSZ(nil)
	require.NoError(t, err)

	service, syncedDataManager, forkchoiceMock := setupAggregateAndProofGossipTestWithConfig(t, &cfg)
	require.NoError(t, syncedDataManager.OnHeadState(s))
	forkchoiceMock.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	forkchoiceMock.Ancestors[s.FinalizedCheckpoint().Epoch*cfg.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	forkchoiceMock.Ancestors[agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot] = forkchoice.ForkChoiceNode{Root: agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Root}
	forkchoiceMock.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}

	gloasMessage, err := service.DecodeGossipMessage("peer", encoded, clparams.GloasVersion)
	require.NoError(t, err)
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	gloasDigest, err := clock.ComputeForkDigest(aggregateEpoch + 1)
	require.NoError(t, err)
	gloasMessage.SetTopicForkDigest(gloasDigest)
	err = service.ProcessMessage(context.Background(), nil, gloasMessage)
	require.ErrorIs(t, err, ErrIgnore)

	fuluMessage, err := service.DecodeGossipMessage("peer", encoded, clparams.FuluVersion)
	require.NoError(t, err)
	fuluDigest, err := clock.ComputeForkDigest(aggregateEpoch)
	require.NoError(t, err)
	fuluMessage.SetTopicForkDigest(fuluDigest)
	require.NoError(t, service.ProcessMessage(context.Background(), nil, fuluMessage))
}

// TestAggregateAndProofGloasRejectIndexGte2 tests that GLOAS rejects aggregate.data.index >= 2
func TestAggregateAndProofGloasRejectIndexGte2(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a config with all forks at epoch 0 so any slot is GLOAS
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.GloasVersion)

	// Set CommitteeIndex to 2 (invalid for GLOAS, must be < 2)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex = 2

	aggService, sd, _ := setupAggregateAndProofTestWithConfig(t, &cfg)
	require.NoError(t, sd.OnHeadState(s))

	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be < 2")
}

// TestAggregateAndProofGloasRejectIndexNot0WhenSlotsMatch tests that GLOAS rejects
// aggregate.data.index != 0 when block.slot == aggregate.data.slot
// Per spec: [REJECT] aggregate.data.index == 0 if block.slot == aggregate.data.slot
// This means: when same slot, index MUST be 0
func TestAggregateAndProofGloasRejectIndexNot0WhenSlotsMatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a config with all forks at epoch 0
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.GloasVersion)

	// Set CommitteeIndex to 1 (will be rejected if slots match, must be 0)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex = 1

	aggService, sd, fcu := setupAggregateAndProofTestWithConfig(t, &cfg)
	require.NoError(t, sd.OnHeadState(s))
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}

	// Set the block header with same slot as aggregate.data.slot
	blockSlot := agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{
		Slot: blockSlot, // Same slot as aggregate
	}

	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be 0 when block.slot == aggregate.data.slot")
}

// TestAggregateAndProofGloasAllowIndex0WhenSlotsDiffer tests that GLOAS allows
// aggregate.data.index == 0 when block.slot != aggregate.data.slot
func TestAggregateAndProofGloasAllowIndex0WhenSlotsDiffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a config with all forks at epoch 0
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.GloasVersion)

	// Set CommitteeIndex to 0 (allowed when slots differ - signals payload not present)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex = 0

	aggService, sd, fcu := setupAggregateAndProofTestWithConfig(t, &cfg)
	require.NoError(t, sd.OnHeadState(s))
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fcu.Ancestors[agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot] = forkchoice.ForkChoiceNode{Root: agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Root}

	// Set the block header with DIFFERENT slot than aggregate.data.slot
	blockSlot := agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot - 1
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{
		Slot: blockSlot, // Different slot from aggregate
	}

	// Should pass (index=0 is allowed when slots differ)
	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.NoError(t, err)
}

// TestAggregateAndProofGloasAllowIndex1WhenSlotsDiffer tests that GLOAS allows
// aggregate.data.index == 1 when block.slot != aggregate.data.slot AND envelope exists
func TestAggregateAndProofGloasAllowIndex1WhenSlotsDiffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a config with all forks at epoch 0
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.GloasVersion)

	// Set CommitteeIndex to 1 (allowed when slots differ - signals payload present)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex = 1

	aggService, sd, fcu := setupAggregateAndProofTestWithConfig(t, &cfg)
	require.NoError(t, sd.OnHeadState(s))
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fcu.Ancestors[agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot] = forkchoice.ForkChoiceNode{Root: agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Root}

	// Set the block header with DIFFERENT slot than aggregate.data.slot
	blockSlot := agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot - 1
	blockRoot := agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{
		Slot: blockSlot, // Different slot from aggregate
	}

	// Set envelope as seen/validated (required for index=1)
	fcu.SetEnvelope(blockRoot, &cltypes.SignedExecutionPayloadEnvelope{})
	fcu.VerifiedPayloads = map[common.Hash]bool{blockRoot: true}

	// Should pass (index=1 is allowed when slots differ and envelope exists)
	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.NoError(t, err)
}

// TestAggregateAndProofGloasAllowIndex0WhenSlotsMatch tests that GLOAS allows
// aggregate.data.index == 0 when block.slot == aggregate.data.slot
// Per spec: [REJECT] aggregate.data.index == 0 if block.slot == aggregate.data.slot
// This means: when same slot, index MUST be 0 (index=0 is allowed/required)
func TestAggregateAndProofGloasAllowIndex0WhenSlotsMatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a config with all forks at epoch 0
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.GloasVersion)

	// Set aggregate.data.CommitteeIndex to 0 (required when slots match)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex = 0

	aggService, sd, fcu := setupAggregateAndProofTestWithConfig(t, &cfg)
	require.NoError(t, sd.OnHeadState(s))
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fcu.Ancestors[agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot] = forkchoice.ForkChoiceNode{Root: agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Root}

	// Set the block header with SAME slot as aggregate.data.slot
	blockSlot := agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{
		Slot: blockSlot,
	}

	// Should pass (index=0 is required when slots match)
	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.NoError(t, err)
}

// TestAggregateAndProofGloasIgnoreIndex1NoEnvelope tests that GLOAS ignores
// aggregate.data.index == 1 when the execution payload has not been verified.
func TestAggregateAndProofGloasIgnoreIndex1NoEnvelope(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a config with all forks at epoch 0
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.GloasVersion)

	// Set CommitteeIndex to 1 (payload present attestation)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex = 1

	aggService, sd, fcu := setupAggregateAndProofTestWithConfig(t, &cfg)
	require.NoError(t, sd.OnHeadState(s))
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}

	// Set the block header with DIFFERENT slot than aggregate.data.slot
	blockSlot := agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot - 1
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{
		Slot: blockSlot,
	}

	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrIgnore)
	require.Contains(t, err.Error(), "execution payload not verified")
}

func TestAggregateAndProofGloasIgnoreIndex1EnvelopeNotVerified(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.GloasVersion)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex = 1
	blockRoot := agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot

	aggService, sd, fcu := setupAggregateAndProofTestWithConfig(t, &cfg)
	require.NoError(t, sd.OnHeadState(s))
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{
		Slot: agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot - 1,
	}
	fcu.SetEnvelope(blockRoot, &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			BeaconBlockRoot: blockRoot,
			Payload:         &cltypes.Eth1Block{BlockHash: common.HexToHash("0x1234")},
		},
	})
	fcu.VerifiedPayloads = map[common.Hash]bool{blockRoot: false}
	fcu.ExecutionPayloadStatusMap[common.HexToHash("0x1234")] = execution_client.PayloadStatusInvalidated

	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrIgnore)
	require.Contains(t, err.Error(), "execution payload not verified")
}

func TestAggregateAndProofGloasRejectIndex1InvalidPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.GloasVersion)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex = 1
	blockRoot := agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot
	execHash := common.HexToHash("0x1234")

	aggService, sd, fcu := setupAggregateAndProofTestWithConfig(t, &cfg)
	require.NoError(t, sd.OnHeadState(s))
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{
		Slot: agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot - 1,
	}
	fcu.SetEnvelope(blockRoot, &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			BeaconBlockRoot: blockRoot,
			Payload:         &cltypes.Eth1Block{BlockHash: execHash},
		},
	})
	fcu.VerifiedPayloads = map[common.Hash]bool{blockRoot: false}
	fcu.PayloadStatusByRootMap[blockRoot] = execution_client.PayloadStatusInvalidated

	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrIgnore)
	require.Contains(t, err.Error(), "execution payload is invalid")
}

// TestAggregateAndProofGloasIndex0NoEnvelopeOk tests that GLOAS does NOT require
// an envelope when index=0 (beacon block attestation, not payload attestation).
func TestAggregateAndProofGloasIndex0NoEnvelopeOk(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a config with all forks at epoch 0
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.GloasVersion)

	// Set CommitteeIndex to 0 (beacon block attestation — no envelope required)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex = 0

	aggService, sd, fcu := setupAggregateAndProofTestWithConfig(t, &cfg)
	require.NoError(t, sd.OnHeadState(s))
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fcu.Ancestors[agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot] = forkchoice.ForkChoiceNode{Root: agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Root}

	// Set the block header with DIFFERENT slot than aggregate.data.slot
	blockSlot := agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot - 1
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{
		Slot: blockSlot,
	}

	// No envelope set — should still pass for index=0
	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.NoError(t, err)
}

// TestAggregateAndProofElectraRejectIndexNot0 tests that Electra/Fulu still rejects index != 0
func TestAggregateAndProofElectraRejectIndexNot0(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a config with Electra fork but GLOAS far in the future
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 999999 // Far in the future, so we stay in Fulu

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.ElectraVersion)

	// Set CommitteeIndex to 1 (invalid for Electra/Fulu, must be 0)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex = 1

	aggService, sd, _ := setupAggregateAndProofTestWithConfig(t, &cfg)
	require.NoError(t, sd.OnHeadState(s))

	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid committee_index in aggregate and proof")
}

func aggregationBitsWithLength(length int, trailingSet bool, limit int) *solid.BitList {
	encoded := make([]byte, length/8+1)
	encoded[0] = 1
	if trailingSet {
		encoded[(length-1)/8] |= 1 << uint((length-1)%8)
	}
	encoded[length/8] |= 1 << uint(length%8)
	return solid.BitlistFromBytes(encoded, limit)
}

func TestAggregateAndProofRejectsAggregationBitsLengthMismatch(t *testing.T) {
	_, _, fixtureState := tests.GetElectraRandom()
	encodedState, err := fixtureState.EncodeSSZ(nil)
	require.NoError(t, err)

	forks := []struct {
		name    string
		version clparams.StateVersion
	}{
		{name: "Electra", version: clparams.ElectraVersion},
		{name: "Fulu", version: clparams.FuluVersion},
		{name: "Gloas", version: clparams.GloasVersion},
	}
	testCases := []struct {
		name               string
		lengthDelta        int
		trailingSet        bool
		wantError          bool
		wantLengthMismatch bool
	}{
		{name: "n-1 bits", lengthDelta: -1, wantError: true},
		{name: "n bits"},
		{name: "n+1 bits trailing unset", lengthDelta: 1, wantError: true, wantLengthMismatch: true},
		{name: "n+1 bits trailing set", lengthDelta: 1, trailingSet: true, wantError: true, wantLengthMismatch: true},
	}

	for _, fork := range forks {
		t.Run(fork.name, func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			cfg.AltairForkEpoch = 0
			cfg.BellatrixForkEpoch = 0
			cfg.CapellaForkEpoch = 0
			cfg.DenebForkEpoch = 0
			cfg.ElectraForkEpoch = 0
			cfg.FuluForkEpoch = cfg.FarFutureEpoch
			cfg.GloasForkEpoch = cfg.FarFutureEpoch
			if fork.version >= clparams.FuluVersion {
				cfg.FuluForkEpoch = 0
			}
			if fork.version >= clparams.GloasVersion {
				cfg.GloasForkEpoch = 0
			}

			headState := state.New(&cfg)
			require.NoError(t, headState.DecodeSSZ(encodedState, int(clparams.ElectraVersion)))
			epoch := headState.Slot() / cfg.SlotsPerEpoch
			require.Equal(t, fork.version, cfg.GetCurrentStateVersion(epoch))

			committee, err := headState.GetBeaconCommitee(headState.Slot(), 0)
			require.NoError(t, err)
			require.Len(t, committee, 13)

			blockRoot, err := headState.BlockRoot()
			require.NoError(t, err)
			source := headState.CurrentJustifiedCheckpoint()
			target := source
			target.Epoch = epoch

			service, syncedData, forkchoiceStore := setupAggregateAndProofTestWithConfig(t, &cfg)
			require.NoError(t, syncedData.OnHeadState(headState))
			forkchoiceStore.FinalizedCheckpointVal = headState.FinalizedCheckpoint()
			forkchoiceStore.Ancestors[headState.FinalizedCheckpoint().Epoch*cfg.SlotsPerEpoch] = forkchoice.ForkChoiceNode{
				Root: headState.FinalizedCheckpoint().Root,
			}
			forkchoiceStore.Ancestors[epoch*cfg.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: target.Root}
			forkchoiceStore.Headers[blockRoot] = &cltypes.BeaconBlockHeader{}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					committeeBits := solid.NewBitVector(int(cfg.MaxCommitteesPerSlot))
					require.NoError(t, committeeBits.SetBitAt(0, true))
					aggregate := &SignedAggregateAndProofForGossip{
						SignedAggregateAndProof: &cltypes.SignedAggregateAndProof{
							Message: &cltypes.AggregateAndProof{
								AggregatorIndex: committee[0],
								Aggregate: &solid.Attestation{
									AggregationBits: aggregationBitsWithLength(
										len(committee)+testCase.lengthDelta,
										testCase.trailingSet,
										int(cfg.MaxCommitteesPerSlot*cfg.MaxValidatorsPerCommittee),
									),
									Data: &solid.AttestationData{
										Slot:            headState.Slot(),
										CommitteeIndex:  0,
										BeaconBlockRoot: blockRoot,
										Source:          source,
										Target:          target,
									},
									CommitteeBits: committeeBits,
								},
							},
						},
					}

					err := service.ProcessMessage(t.Context(), nil, aggregate)
					if !testCase.wantError {
						require.NoError(t, err)
						return
					}
					require.Error(t, err)
					require.NotErrorIs(t, err, ErrIgnore)
					if testCase.wantLengthMismatch {
						require.Contains(t, err.Error(), "aggregation bits length")
					}
				})
			}
		})
	}
}

func TestAggregateAndProofRejectsAggregationBitsLengthMismatchBeforeElectra(t *testing.T) {
	testCases := []struct {
		name        string
		lengthDelta int
		trailingSet bool
	}{
		{name: "n-1 bits", lengthDelta: -1},
		{name: "n+1 bits trailing unset", lengthDelta: 1},
		{name: "n+1 bits trailing set", lengthDelta: 1, trailingSet: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			aggregate, headState := getAggregateAndProofAndState()
			epoch := headState.Slot() / clparams.MainnetBeaconConfig.SlotsPerEpoch
			require.Equal(t, clparams.Phase0Version, clparams.MainnetBeaconConfig.GetCurrentStateVersion(epoch))
			committee, err := headState.GetBeaconCommitee(
				headState.Slot(),
				aggregate.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex,
			)
			require.NoError(t, err)
			require.Len(t, committee, 8)
			aggregate.SignedAggregateAndProof.Message.Aggregate.AggregationBits = aggregationBitsWithLength(
				len(committee)+testCase.lengthDelta,
				testCase.trailingSet,
				2048,
			)

			service, syncedData, forkchoiceStore := setupAggregateAndProofTest(t)
			require.NoError(t, syncedData.OnHeadState(headState))
			forkchoiceStore.FinalizedCheckpointVal = headState.FinalizedCheckpoint()
			forkchoiceStore.Ancestors[headState.FinalizedCheckpoint().Epoch*clparams.MainnetBeaconConfig.SlotsPerEpoch] = forkchoice.ForkChoiceNode{
				Root: headState.FinalizedCheckpoint().Root,
			}
			forkchoiceStore.Ancestors[aggregate.SignedAggregateAndProof.Message.Aggregate.Data.Slot] = forkchoice.ForkChoiceNode{
				Root: aggregate.SignedAggregateAndProof.Message.Aggregate.Data.Target.Root,
			}
			forkchoiceStore.Headers[aggregate.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}

			err = service.ProcessMessage(t.Context(), nil, aggregate)
			require.Error(t, err)
			require.NotErrorIs(t, err, ErrIgnore)
			require.Contains(t, err.Error(), "aggregation bits length")
		})
	}
}

func TestAggregateAndProofForLateBlockIsProcessedOnceBlockArrives(t *testing.T) {
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(_, _, _ [][]byte) (bool, error) { return true, nil }

	cfg := &clparams.MainnetBeaconConfig
	agg, s := getAggregateAndProofAndState()
	setValidAggregateSelectionProof(t, cfg, agg, s)
	data := agg.SignedAggregateAndProof.Message.Aggregate.Data

	syncedDataManager := synced_data.NewSyncedDataManager(cfg, true)
	require.NoError(t, syncedDataManager.OnHeadState(s))
	forkChoiceMock := mock_services.NewForkChoiceStorageMock(t)
	forkChoiceMock.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	forkChoiceMock.Ancestors[s.FinalizedCheckpoint().Epoch*cfg.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	forkChoiceMock.Ancestors[data.Slot] = forkchoice.ForkChoiceNode{Root: data.Target.Root}
	forkChoiceMock.Headers[data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}
	forkChoice := &lateBlockForkChoice{ForkChoiceStorageMock: forkChoiceMock, root: data.BeaconBlockRoot}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	verifier := NewBatchSignatureVerifier(ctx, nil)
	verifier.Start()
	opPool := pool.OperationsPool{AttestationsPool: pool.NewOperationPool[common.Bytes96, *solid.Attestation](100, "test")}
	validatorParams := validator_params.NewValidatorParams()
	validatorParams.SetFeeRecipient(0, common.Address{})
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, cfg)
	service := NewAggregateAndProofService(ctx, syncedDataManager, forkChoice, clock, cfg, opPool, false, verifier, validatorParams)

	require.ErrorIs(t, service.ProcessMessage(context.Background(), nil, agg), ErrIgnore)

	forkChoice.arrived.Store(true)
	require.Eventually(t, func() bool { return forkChoice.attestingSets.Load() == 1 }, 5*time.Second, 10*time.Millisecond,
		"aggregate was not processed by fork choice after its block arrived")
}

func TestAggregateAndProofIsVerifiedWhenTheNodeHasValidators(t *testing.T) {
	for _, validatorSource := range []string{"none", "fee recipient", "attached"} {
		t.Run(validatorSource, func(t *testing.T) {
			saveSignatureGlobals(t)
			blsVerifyMultipleSignatures = func(_, _, _ [][]byte) (bool, error) { return true, nil }

			cfg := &clparams.MainnetBeaconConfig
			agg, s := getAggregateAndProofAndState()
			setValidAggregateSelectionProof(t, cfg, agg, s)
			data := agg.SignedAggregateAndProof.Message.Aggregate.Data
			s.SetVersion(clparams.FuluVersion)

			// The local validator proposes neither in this epoch nor in the next one.
			proposers, err := s.GetBeaconProposerIndices(state.Epoch(s))
			require.NoError(t, err)
			lookahead := s.GetProposerLookahead()
			for i := range lookahead.Length() {
				proposers = append(proposers, lookahead.Get(i))
			}
			localValidator := uint64(0)
			for slices.Contains(proposers, localValidator) {
				localValidator++
			}

			syncedDataManager := synced_data.NewSyncedDataManager(cfg, true)
			require.NoError(t, syncedDataManager.OnHeadState(s))
			forkChoiceMock := mock_services.NewForkChoiceStorageMock(t)
			forkChoiceMock.FinalizedCheckpointVal = s.FinalizedCheckpoint()
			forkChoiceMock.Ancestors[s.FinalizedCheckpoint().Epoch*cfg.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
			forkChoiceMock.Ancestors[data.Slot] = forkchoice.ForkChoiceNode{Root: data.Target.Root}
			forkChoiceMock.Headers[data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			verifier := NewBatchSignatureVerifier(ctx, nil)
			verifier.Start()
			opPool := pool.OperationsPool{AttestationsPool: pool.NewOperationPool[common.Bytes96, *solid.Attestation](100, "test")}
			validatorParams := validator_params.NewValidatorParams()
			switch validatorSource {
			case "fee recipient":
				validatorParams.SetFeeRecipient(localValidator, common.Address{})
			case "attached":
				validatorParams.AddAttachedValidator(localValidator)
			}
			clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, cfg)
			service := NewAggregateAndProofService(ctx, syncedDataManager, forkChoiceMock, clock, cfg, opPool, false, verifier, validatorParams)

			err = service.ProcessMessage(context.Background(), nil, agg)
			stored := func() bool {
				_, ok := opPool.AttestationsPool.Get(agg.SignedAggregateAndProof.Message.Aggregate.Signature)
				return ok
			}
			if validatorSource == "none" {
				require.ErrorIs(t, err, ErrIgnore)
				require.False(t, stored())
				return
			}
			require.NoError(t, err)
			require.Eventually(t, stored, 5*time.Second, 10*time.Millisecond, "a verified aggregate must reach the block attestation pool")
		})
	}
}
