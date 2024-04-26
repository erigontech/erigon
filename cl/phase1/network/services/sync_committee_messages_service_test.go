package services

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon/cl/antiquary/tests"
	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
	syncpoolmock "github.com/ledgerwatch/erigon/cl/validator/sync_contribution_pool/mock_services"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func setupSyncCommitteesServiceTest(t *testing.T, ctrl *gomock.Controller) (SyncCommitteeMessagesService, *synced_data.SyncedDataManager, *eth_clock.MockEthereumClock) {
	cfg := &clparams.MainnetBeaconConfig
	syncedDataManager := synced_data.NewSyncedDataManager(true, cfg)
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	syncContributionPool := syncpoolmock.NewMockSyncContributionPool(ctrl)
	s := NewSyncCommitteeMessagesService(cfg, ethClock, syncedDataManager, syncContributionPool, true)
	syncContributionPool.EXPECT().AddSyncCommitteeMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return s, syncedDataManager, ethClock
}

func getObjectsForSyncCommitteesServiceTest(t *testing.T, ctrl *gomock.Controller) (*state.CachingBeaconState, *cltypes.SyncCommitteeMessage) {
	_, _, state := tests.GetBellatrixRandom()
	br, _ := state.BlockRoot()
	msg := &cltypes.SyncCommitteeMessage{
		Slot:            state.Slot(),
		BeaconBlockRoot: br,
		ValidatorIndex:  0,
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
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(msg.Slot).Return(false).AnyTimes()
	require.Error(t, s.ProcessMessage(context.Background(), nil, msg))
}

func TestSyncCommitteesBadSubnet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	state, msg := getObjectsForSyncCommitteesServiceTest(t, ctrl)
	sn := uint64(1000)

	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	synced.OnHeadState(state)
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(msg.Slot).Return(true).AnyTimes()
	require.Error(t, s.ProcessMessage(context.Background(), &sn, msg))
}

func TestSyncCommitteesSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	state, msg := getObjectsForSyncCommitteesServiceTest(t, ctrl)

	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	synced.OnHeadState(state)
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(msg.Slot).Return(true).AnyTimes()
	require.NoError(t, s.ProcessMessage(context.Background(), new(uint64), msg))
	require.Error(t, s.ProcessMessage(context.Background(), new(uint64), msg)) // Ignore if done twice
}
