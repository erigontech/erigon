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

package committee_subscription

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	gossip_mock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
)

const (
	testHeadSlot      = 6400 // first slot of epoch 200
	testLocalProposer = 7
	testOtherProposer = 9
)

// newProposalWindowTest returns a manager whose head state is at testHeadSlot, with testLocalProposer attached and
// proposing at localProposalSlots; every other slot of the lookahead has testOtherProposer.
func newProposalWindowTest(t *testing.T, currentSlot uint64, localProposalSlots ...uint64) (*CommitteeSubscribeMgmt, *gossip_mock.MockGossip, *eth_clock.MockEthereumClock) {
	cfg := clparams.MainnetBeaconConfig
	netCfg := clparams.NetworkConfig{AttestationSubnetCount: 64}
	ctrl := gomock.NewController(t)

	headState := state.New(&cfg)
	headState.SetVersion(clparams.FuluVersion)
	require.NoError(t, headState.SetSlot(testHeadSlot))
	lookahead := solid.NewUint64VectorSSZ(int((cfg.MinSeedLookahead + 1) * cfg.SlotsPerEpoch))
	for i := range lookahead.Length() {
		lookahead.Set(i, testOtherProposer)
	}
	for _, slot := range localProposalSlots {
		lookahead.Set(int(slot-testHeadSlot), testLocalProposer)
	}
	headState.SetProposerLookahead(lookahead)
	syncedData := synced_data.NewSyncedDataManager(&cfg, true)
	require.NoError(t, syncedData.OnHeadStateWithBlockRoot(headState, common.Hash{1}))

	validatorParams := validator_params.NewValidatorParams()
	validatorParams.AddAttachedValidator(testLocalProposer)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(currentSlot).AnyTimes()
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	return &CommitteeSubscribeMgmt{
		beaconConfig:    &cfg,
		netConfig:       &netCfg,
		syncedData:      syncedData,
		gossipManager:   gossipManager,
		ethClock:        clock,
		validatorParams: validatorParams,
	}, gossipManager, clock
}

func TestProposalWindowSubscribesEveryAttestationSubnetUntilTheProposalSlotEnds(t *testing.T) {
	proposalSlot := uint64(testHeadSlot + 20)
	c, gossipManager, clock := newProposalWindowTest(t, proposalSlot-proposalWindowSlots, proposalSlot)
	expiry := time.Unix(1_700_000_000, 0)
	clock.EXPECT().GetSlotTime(proposalSlot + 1).Return(expiry)
	for subnet := range uint64(64) {
		gossipManager.EXPECT().SubscribeWithExpiry(gossip.TopicNameBeaconAttestation(subnet), expiry).Times(1)
	}

	c.subscribeForUpcomingProposal()
}

func TestProposalWindowDoesNotSubscribeBeforeTheWindow(t *testing.T) {
	proposalSlot := uint64(testHeadSlot + 20)
	c, _, _ := newProposalWindowTest(t, proposalSlot-proposalWindowSlots-1, proposalSlot)

	c.subscribeForUpcomingProposal() // the gossip mock fails the test on any subscription
}

func TestProposalWindowDoesNotSubscribeForAnotherValidatorsProposal(t *testing.T) {
	c, _, _ := newProposalWindowTest(t, testHeadSlot)

	c.subscribeForUpcomingProposal()
}

func TestProposalWindowDoesNotSubscribeWithoutAHeadState(t *testing.T) {
	proposalSlot := uint64(testHeadSlot + 20)
	c, _, _ := newProposalWindowTest(t, proposalSlot-1, proposalSlot)
	c.syncedData = synced_data.NewSyncedDataManager(c.beaconConfig, true)

	c.subscribeForUpcomingProposal()
}

// One subscription covers every local proposal of the window, so a later proposal does not end it early.
func TestLastLocalProposalReturnsTheLastLocalProposalOfTheWindow(t *testing.T) {
	isLocal := func(index uint64) bool { return index == testLocalProposer }
	proposers := map[uint64]uint64{105: testLocalProposer, 110: testLocalProposer, 117: testLocalProposer}
	proposerAt := func(slot uint64) (uint64, bool) {
		if slot == 112 {
			return 0, false // beyond the lookahead
		}
		if p, ok := proposers[slot]; ok {
			return p, true
		}
		return testOtherProposer, true
	}

	slot, ok := lastLocalProposal(proposerAt, isLocal, 100)
	require.True(t, ok)
	require.Equal(t, uint64(110), slot)

	_, ok = lastLocalProposal(proposerAt, isLocal, 110)
	require.True(t, ok, "117 is within the window of slot 110")
	_, ok = lastLocalProposal(proposerAt, isLocal, 117)
	require.False(t, ok, "the proposal slot itself is too late to subscribe for")
}
