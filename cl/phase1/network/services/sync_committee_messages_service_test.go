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
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	syncpoolmock "github.com/erigontech/erigon/cl/validator/sync_contribution_pool/mock_services"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func setupSyncCommitteesServiceTest(t *testing.T, ctrl *gomock.Controller) (SyncCommitteeMessagesService, *synced_data.SyncedDataManager, *eth_clock.MockEthereumClock) {
	cfg := &clparams.MainnetBeaconConfig
	syncedDataManager := synced_data.NewSyncedDataManager(cfg, true)
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	syncContributionPool := syncpoolmock.NewMockSyncContributionPool(ctrl)
	batchSignatureVerifier := NewBatchSignatureVerifier(context.TODO(), nil)
	go batchSignatureVerifier.Start()
	s := NewSyncCommitteeMessagesService(cfg, ethClock, syncedDataManager, syncContributionPool, batchSignatureVerifier, true)
	syncContributionPool.EXPECT().AddSyncCommitteeMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return s, syncedDataManager, ethClock
}

func getObjectsForSyncCommitteesServiceTest(t *testing.T, ctrl *gomock.Controller) (*state.CachingBeaconState, *SyncCommitteeMessageForGossip) {
	_, _, state := tests.GetBellatrixRandom()
	br, _ := state.BlockRoot()
	msg := &SyncCommitteeMessageForGossip{
		SyncCommitteeMessage: &cltypes.SyncCommitteeMessage{
			Slot:            state.Slot(),
			BeaconBlockRoot: br,
			ValidatorIndex:  0,
		},
		ImmediateVerification: true,
	}
	return state, msg
}

func TestSyncCommitteesServiceUnsynced(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, _, _ := setupSyncCommitteesServiceTest(t, ctrl)
	require.Error(t, s.ProcessMessage(context.TODO(), nil, nil))
}

func TestSyncCommitteesBadTiming(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	state, msg := getObjectsForSyncCommitteesServiceTest(t, ctrl)

	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	synced.OnHeadState(state)
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(msg.SyncCommitteeMessage.Slot).Return(false).AnyTimes()
	require.Error(t, s.ProcessMessage(context.Background(), nil, msg))
}

func TestSyncCommitteesBadSubnet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	state, msg := getObjectsForSyncCommitteesServiceTest(t, ctrl)
	sn := uint64(1000)

	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	synced.OnHeadState(state)
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(msg.SyncCommitteeMessage.Slot).Return(true).AnyTimes()
	require.Error(t, s.ProcessMessage(context.Background(), &sn, msg))
}

func TestSyncCommitteesSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFuncs := &mockFuncs{ctrl: ctrl}
	blsVerifyMultipleSignatures = mockFuncs.BlsVerifyMultipleSignatures

	state, msg := getObjectsForSyncCommitteesServiceTest(t, ctrl)
	ctrl.RecordCall(mockFuncs, "BlsVerifyMultipleSignatures", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	synced.OnHeadState(state)
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(msg.SyncCommitteeMessage.Slot).Return(true).AnyTimes()
	require.NoError(t, s.ProcessMessage(context.Background(), new(uint64), msg))
	require.Error(t, s.ProcessMessage(context.Background(), new(uint64), msg)) // Ignore if done twice
}
