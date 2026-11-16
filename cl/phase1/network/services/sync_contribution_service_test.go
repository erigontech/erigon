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
	"testing"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	syncpoolmock "github.com/erigontech/erigon/cl/validator/sync_contribution_pool/mock_services"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func setupSyncContributionServiceTest(ctrl *gomock.Controller) (SyncContributionService, *synced_data.SyncedDataManager, *eth_clock.MockEthereumClock) {
	cfg := &clparams.MainnetBeaconConfig
	syncedDataManager := synced_data.NewSyncedDataManager(cfg, true)
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	syncContributionPool := syncpoolmock.NewMockSyncContributionPool(ctrl)
	batchSignatureVerifier := NewBatchSignatureVerifier(context.TODO(), nil)
	go batchSignatureVerifier.Start()
	s := NewSyncContributionService(syncedDataManager, cfg, syncContributionPool, ethClock, beaconevents.NewEventEmitter(), batchSignatureVerifier, validator_params.NewValidatorParams(), true)
	syncContributionPool.EXPECT().AddSyncContribution(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return s, syncedDataManager, ethClock
}

func getObjectsForSyncContributionServiceTest() (*state.CachingBeaconState, *SignedContributionAndProofForGossip) {
	_, _, state := tests.GetBellatrixRandom()
	br, _ := state.BlockRoot()
	aggBits := make([]byte, 16)
	aggBits[0] = 1
	msg := &SignedContributionAndProofForGossip{
		SignedContributionAndProof: &cltypes.SignedContributionAndProof{
			Message: &cltypes.ContributionAndProof{
				AggregatorIndex: 0,
				Contribution: &cltypes.Contribution{
					Slot:              state.Slot(),
					BeaconBlockRoot:   br,
					SubcommitteeIndex: 0,
					AggregationBits:   aggBits,
				},
			},
		},
		ImmediateVerification: true,
	}

	return state, msg
}

func TestSyncContributionServiceUnsynced(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, _, _ := setupSyncContributionServiceTest(ctrl)
	_, msg := getObjectsForSyncContributionServiceTest()
	err := s.ProcessMessage(context.TODO(), nil, msg)
	require.Error(t, err)
}

func TestSyncContributionServiceBadTiming(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, sd, clock := setupSyncContributionServiceTest(ctrl)
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(false).AnyTimes()
	state, msg := getObjectsForSyncContributionServiceTest()
	require.NoError(t, sd.OnHeadState(state))
	err := s.ProcessMessage(context.TODO(), nil, msg)
	require.Error(t, err)
}

func TestSyncContributionServiceBadSubcommitteeIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, sd, clock := setupSyncContributionServiceTest(ctrl)
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	state, msg := getObjectsForSyncContributionServiceTest()
	require.NoError(t, sd.OnHeadState(state))
	msg.SignedContributionAndProof.Message.Contribution.SubcommitteeIndex = 1000
	err := s.ProcessMessage(context.TODO(), nil, msg)
	require.Error(t, err)
}

func TestSyncContributionServiceBadAggregationBits(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, sd, clock := setupSyncContributionServiceTest(ctrl)
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	state, msg := getObjectsForSyncContributionServiceTest()
	require.NoError(t, sd.OnHeadState(state))
	msg.SignedContributionAndProof.Message.Contribution.AggregationBits = make([]byte, 16)
	err := s.ProcessMessage(context.TODO(), nil, msg)
	require.Error(t, err)
}

func TestSyncContributionServiceBadAggregator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, sd, clock := setupSyncContributionServiceTest(ctrl)
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	state, msg := getObjectsForSyncContributionServiceTest()
	require.NoError(t, state.SetCurrentSyncCommittee(&solid.SyncCommittee{}))
	require.NoError(t, state.SetNextSyncCommittee(&solid.SyncCommittee{}))
	require.NoError(t, sd.OnHeadState(state))
	err := s.ProcessMessage(context.TODO(), nil, msg)
	require.Error(t, err)
}

func TestSyncContributionServiceSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFuncs := &mockFuncs{ctrl: ctrl}
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = mockFuncs.BlsVerifyMultipleSignatures
	s, sd, clock := setupSyncContributionServiceTest(ctrl)
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	ctrl.RecordCall(mockFuncs, "BlsVerifyMultipleSignatures", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	state, msg := getObjectsForSyncContributionServiceTest()
	require.NoError(t, sd.OnHeadState(state))
	err := s.ProcessMessage(context.TODO(), nil, msg)
	require.NoError(t, err)
	err = s.ProcessMessage(context.TODO(), nil, msg)
	require.NoError(t, err) // Silent ignore: returns nil when seen before
}

func TestSyncSubcommitteePubkeysUseTheCommitteeOfTheContributionSlot(t *testing.T) {
	_, _, head := tests.GetBellatrixRandom()
	cfg := head.BeaconConfig()
	nextPeriod := cfg.RoundSlotToSyncCommitteePeriod(head.Slot()) + cfg.SlotsPerEpoch*cfg.EpochsPerSyncCommitteePeriod
	current, next := head.CurrentSyncCommittee().Copy(), head.NextSyncCommittee().Copy()
	currentKeys, nextKeys := current.GetCommittee(), next.GetCommittee()
	currentKeys[0], nextKeys[0] = common.Bytes48{0xaa}, common.Bytes48{0xbb}
	current.SetCommittee(currentKeys)
	next.SetCommittee(nextKeys)
	require.NoError(t, head.SetCurrentSyncCommittee(current))
	require.NoError(t, head.SetNextSyncCommittee(next))
	s := &syncContributionService{beaconCfg: cfg}

	// The head has not imported the last block of the period yet; the next committee signs for that slot.
	require.NoError(t, head.SetSlot(nextPeriod-2))
	pubkeys, err := s.getSyncSubcommitteePubkeys(head, nextPeriod-1, 0)
	require.NoError(t, err)
	require.Equal(t, common.Bytes48{0xbb}, pubkeys[0])

	// The head is at the last slot of the period; the current committee signed for the slot before.
	require.NoError(t, head.SetSlot(nextPeriod-1))
	pubkeys, err = s.getSyncSubcommitteePubkeys(head, nextPeriod-2, 0)
	require.NoError(t, err)
	require.Equal(t, common.Bytes48{0xaa}, pubkeys[0])
}

// The aggregator check does not depend on the state, so it runs before the committee lookup: a
// non-aggregator is rejected even when the head state does not hold the committee that signs for
// the contribution slot, and an aggregator is ignored.
func TestSyncContributionServiceCommitteeNotInHeadState(t *testing.T) {
	for _, tt := range []struct {
		name           string
		selectionProof byte // the first byte of a selection proof that selects (1) or does not select (0) an aggregator
		wantIgnore     bool
	}{
		{"non-aggregator", 0, false},
		{"aggregator", 1, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			cfg := &clparams.MainnetBeaconConfig
			syncedData := synced_data.NewSyncedDataManager(cfg, true)
			clock := eth_clock.NewMockEthereumClock(ctrl)
			clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
			verifier := NewBatchSignatureVerifier(t.Context(), nil)
			s := NewSyncContributionService(syncedData, cfg, syncpoolmock.NewMockSyncContributionPool(ctrl), clock, beaconevents.NewEventEmitter(), verifier, validator_params.NewValidatorParams(), false)
			head, msg := getObjectsForSyncContributionServiceTest()
			require.NoError(t, syncedData.OnHeadState(head))
			contribution := msg.SignedContributionAndProof.Message
			contribution.Contribution.Slot = head.Slot() + 2*cfg.SlotsPerEpoch*cfg.EpochsPerSyncCommitteePeriod
			contribution.SelectionProof[0] = tt.selectionProof

			err := s.ProcessMessage(t.Context(), nil, msg)
			if tt.wantIgnore {
				require.ErrorIs(t, err, ErrIgnore)
				require.ErrorIs(t, err, state.ErrSyncCommitteeNotInState)
				return
			}
			require.Error(t, err)
			require.NotErrorIs(t, err, ErrIgnore)
		})
	}
}

func TestSyncContributionServiceSeenSetIsBounded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, _, clock := setupSyncContributionServiceTest(ctrl)
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	service := s.(*syncContributionService)
	contribution := func(slot uint64) *cltypes.ContributionAndProof {
		return &cltypes.ContributionAndProof{Contribution: &cltypes.Contribution{Slot: slot}}
	}

	slots := 100 * service.beaconCfg.SyncCommitteeSize
	for slot := range slots {
		service.markContributionAsSeen(contribution(slot))
	}
	require.False(t, service.wasContributionSeen(contribution(0)))
	require.True(t, service.wasContributionSeen(contribution(slots-1)))
}

func TestSyncContributionServiceSeenSetHoldsThreeSlots(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, _, clock := setupSyncContributionServiceTest(ctrl)
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	service := s.(*syncContributionService)
	contribution := func(slot, aggregator uint64) *cltypes.ContributionAndProof {
		return &cltypes.ContributionAndProof{AggregatorIndex: aggregator, Contribution: &cltypes.Contribution{Slot: slot}}
	}

	for slot := range uint64(3) {
		for aggregator := range service.beaconCfg.SyncCommitteeSize {
			service.markContributionAsSeen(contribution(slot, aggregator))
		}
	}
	require.True(t, service.wasContributionSeen(contribution(0, 0)))
}

func TestSyncContributionServiceLateMarkDoesNotEvict(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, _, clock := setupSyncContributionServiceTest(ctrl)
	const currentSlot = 10
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).DoAndReturn(func(slot uint64) bool {
		return slot >= currentSlot
	}).AnyTimes()
	service := s.(*syncContributionService)
	contribution := func(slot, aggregator uint64) *cltypes.ContributionAndProof {
		return &cltypes.ContributionAndProof{AggregatorIndex: aggregator, Contribution: &cltypes.Contribution{Slot: slot}}
	}

	size := service.beaconCfg.SyncCommitteeSize * seenContributionSlots
	for aggregator := range size {
		service.markContributionAsSeen(contribution(currentSlot, aggregator))
	}
	service.markContributionAsSeen(contribution(currentSlot-3, 0))
	require.True(t, service.wasContributionSeen(contribution(currentSlot, 0)))
	require.False(t, service.wasContributionSeen(contribution(currentSlot-3, 0)))
}
