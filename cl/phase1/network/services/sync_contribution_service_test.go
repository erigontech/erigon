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
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	syncpoolmock "github.com/erigontech/erigon/cl/validator/sync_contribution_pool/mock_services"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
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

func TestVerifySyncContributionAggregatedSignatureUsesContributionSlotDomain(t *testing.T) {
	headState, msg := getObjectsForSyncContributionServiceTest()
	cfg := headState.BeaconConfig()
	forkEpoch := state.Epoch(headState) + 1
	headSlot := forkEpoch * cfg.SlotsPerEpoch
	require.NoError(t, headState.SetSlot(headSlot))

	previousVersion := common.Bytes4{0x01}
	currentVersion := common.Bytes4{0x02}
	headState.SetFork(&cltypes.Fork{
		PreviousVersion: previousVersion,
		CurrentVersion:  currentVersion,
		Epoch:           forkEpoch,
	})
	publicKey, err := headState.ValidatorPublicKey(0)
	require.NoError(t, err)
	contribution := msg.SignedContributionAndProof.Message.Contribution

	for _, tc := range []struct {
		name        string
		slot        uint64
		forkVersion common.Bytes4
	}{
		{name: "previous epoch", slot: headSlot - 1, forkVersion: previousVersion},
		{name: "same epoch", slot: headSlot, forkVersion: currentVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			contribution.Slot = tc.slot
			_, signingRoot, _, err := verifySyncContributionProofAggregatedSignature(headState, contribution, []common.Bytes48{publicKey})
			require.NoError(t, err)

			domain, err := fork.ComputeDomain(cfg.DomainSyncCommittee[:], tc.forkVersion, headState.GenesisValidatorsRoot())
			require.NoError(t, err)
			expected := crypto.Sha256(contribution.BeaconBlockRoot[:], domain)
			require.Equal(t, expected[:], signingRoot)
		})
	}
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
