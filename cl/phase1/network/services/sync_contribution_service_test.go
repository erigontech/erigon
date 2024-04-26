package services

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon/cl/antiquary/tests"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconevents"
	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
	syncpoolmock "github.com/ledgerwatch/erigon/cl/validator/sync_contribution_pool/mock_services"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func setupSyncContributionServiceTest(t *testing.T, ctrl *gomock.Controller) (SyncContributionService, *synced_data.SyncedDataManager, *eth_clock.MockEthereumClock) {
	cfg := &clparams.MainnetBeaconConfig
	syncedDataManager := synced_data.NewSyncedDataManager(true, cfg)
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	syncContributionPool := syncpoolmock.NewMockSyncContributionPool(ctrl)
	s := NewSyncContributionService(syncedDataManager, cfg, syncContributionPool, ethClock, beaconevents.NewEmitters(), true)
	syncContributionPool.EXPECT().AddSyncContribution(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return s, syncedDataManager, ethClock
}

func getObjectsForSyncContributionServiceTest(t *testing.T, ctrl *gomock.Controller) (*state.CachingBeaconState, *cltypes.SignedContributionAndProof) {
	_, _, state := tests.GetBellatrixRandom()
	br, _ := state.BlockRoot()
	aggBits := make([]byte, 16)
	aggBits[0] = 1
	msg := &cltypes.SignedContributionAndProof{
		Message: &cltypes.ContributionAndProof{
			AggregatorIndex: 0,
			Contribution: &cltypes.Contribution{
				Slot:              state.Slot(),
				BeaconBlockRoot:   br,
				SubcommitteeIndex: 0,
				AggregationBits:   aggBits,
			},
		},
	}

	return state, msg
}

func TestSyncContributionServiceUnsynced(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, _, _ := setupSyncContributionServiceTest(t, ctrl)
	_, msg := getObjectsForSyncContributionServiceTest(t, ctrl)
	err := s.ProcessMessage(context.TODO(), nil, msg)
	require.Error(t, err)
}

func TestSyncContributionServiceBadTiming(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, sd, clock := setupSyncContributionServiceTest(t, ctrl)
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(false).AnyTimes()
	state, msg := getObjectsForSyncContributionServiceTest(t, ctrl)
	sd.OnHeadState(state)
	err := s.ProcessMessage(context.TODO(), nil, msg)
	require.Error(t, err)
}

func TestSyncContributionServiceBadSubcommitteeIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, sd, clock := setupSyncContributionServiceTest(t, ctrl)
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	state, msg := getObjectsForSyncContributionServiceTest(t, ctrl)
	sd.OnHeadState(state)
	msg.Message.Contribution.SubcommitteeIndex = 1000
	err := s.ProcessMessage(context.TODO(), nil, msg)
	require.Error(t, err)
}

func TestSyncContributionServiceBadAggregationBits(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, sd, clock := setupSyncContributionServiceTest(t, ctrl)
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	state, msg := getObjectsForSyncContributionServiceTest(t, ctrl)
	sd.OnHeadState(state)
	msg.Message.Contribution.AggregationBits = make([]byte, 16)
	err := s.ProcessMessage(context.TODO(), nil, msg)
	require.Error(t, err)
}

func TestSyncContributionServiceBadAggregator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, sd, clock := setupSyncContributionServiceTest(t, ctrl)
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	state, msg := getObjectsForSyncContributionServiceTest(t, ctrl)
	state.SetCurrentSyncCommittee(&solid.SyncCommittee{})
	state.SetNextSyncCommittee(&solid.SyncCommittee{})
	sd.OnHeadState(state)
	err := s.ProcessMessage(context.TODO(), nil, msg)
	require.Error(t, err)
}

func TestSyncContributionServiceSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, sd, clock := setupSyncContributionServiceTest(t, ctrl)
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	state, msg := getObjectsForSyncContributionServiceTest(t, ctrl)
	sd.OnHeadState(state)
	err := s.ProcessMessage(context.TODO(), nil, msg)
	require.NoError(t, err)
	err = s.ProcessMessage(context.TODO(), nil, msg)
	require.Error(t, err)
}
