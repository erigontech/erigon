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

package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	sync_mock_services "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	gossip_mock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	"github.com/erigontech/erigon/cl/validator/committee_subscription/mock_services"
	"github.com/erigontech/erigon/common/log/v3"
)

func postBeaconCommitteeSubscription(t *testing.T, handler *ApiHandler) int {
	t.Helper()

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	body, err := json.Marshal([]*cltypes.BeaconCommitteeSubscription{{
		ValidatorIndex:   1,
		CommitteeIndex:   0,
		CommitteesAtSlot: 1,
		Slot:             1,
		IsAggregator:     false,
	}})
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		server.URL+"/eth/v1/validator/beacon_committee_subscriptions", bytes.NewBuffer(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}

// The beacon-APIs spec declares 503 CurrentlySyncing for this endpoint, so a node without a head
// state must not report the condition as an internal error.
func TestBeaconCommitteeSubscriptionIsUnavailableWhileSyncing(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)

	committeeSub := mock_services.NewMockCommitteeSubscribe(ctrl)
	committeeSub.EXPECT().AddAttestationSubscription(gomock.Any(), gomock.Any()).
		Return(synced_data.ErrNotSynced).AnyTimes()
	handler.committeeSub = committeeSub

	require.Equal(t, http.StatusServiceUnavailable, postBeaconCommitteeSubscription(t, handler))
}

func TestBeaconCommitteeSubscriptionRecordsAttachedValidatorsBeforeSubscribing(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, _, handler, _, _, _, validatorParams := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)

	committeeSub := mock_services.NewMockCommitteeSubscribe(ctrl)
	committeeSub.EXPECT().AddAttestationSubscription(gomock.Any(), gomock.Any()).Return(synced_data.ErrNotSynced)
	handler.committeeSub = committeeSub
	body, err := json.Marshal([]*cltypes.BeaconCommitteeSubscription{{ValidatorIndex: 7}, {ValidatorIndex: 8}})
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost,
		"/eth/v1/validator/beacon_committee_subscriptions", bytes.NewReader(body))

	handler.mux.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.ElementsMatch(t, []uint64{7, 8}, validatorParams.GetValidators())
	_, found := validatorParams.GetFeeRecipient(7)
	require.False(t, found)
}

func TestBeaconCommitteeSubscriptionReportsOtherFailuresAsInternalError(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)

	committeeSub := mock_services.NewMockCommitteeSubscribe(ctrl)
	committeeSub.EXPECT().AddAttestationSubscription(gomock.Any(), gomock.Any()).
		Return(errors.New("subnet computation blew up")).AnyTimes()
	handler.committeeSub = committeeSub

	require.Equal(t, http.StatusInternalServerError, postBeaconCommitteeSubscription(t, handler))
}

func TestBeaconCommitteeSubscriptionSucceedsWhenSynced(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)

	committeeSub := mock_services.NewMockCommitteeSubscribe(ctrl)
	committeeSub.EXPECT().AddAttestationSubscription(gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()
	handler.committeeSub = committeeSub

	require.Equal(t, http.StatusOK, postBeaconCommitteeSubscription(t, handler))
}

// The subnets come from the request, so a syncing node subscribes too, and the endpoint has no 503 response.
func TestSyncCommitteeSubscriptionSucceedsWhileSyncing(t *testing.T) {
	_, _, _, _, _, handler, _, syncedDataMgr, _, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	syncedDataMgr.(*sync_mock_services.MockSyncedData).EXPECT().Syncing().Return(true).AnyTimes()
	ctrl := gomock.NewController(t)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	gossipManager.EXPECT().SubscribeWithExpiry(gossip.TopicNameSyncCommittee(0), gomock.Any()).Return(nil).Times(1)
	handler.gossipManager = gossipManager

	// Far enough ahead that the subscription has not expired, without overflowing the slot clock.
	require.Equal(t, http.StatusOK, postSyncCommitteeSubscriptions(t, handler, []ValidatorSyncCommitteeSubscriptionsRequest{{
		ValidatorIndex:        1,
		SyncCommitteeIndicies: []string{"0"},
		UntilEpoch:            1_000_000_000,
	}}))
}

func TestSyncCommitteeSubscriptionRecordsAttachedValidatorWhenExpired(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, validatorParams := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)

	require.Equal(t, http.StatusOK, postSyncCommitteeSubscriptions(t, handler, []ValidatorSyncCommitteeSubscriptionsRequest{{
		ValidatorIndex:        7,
		SyncCommitteeIndicies: []string{"0"},
	}}))
	require.ElementsMatch(t, []uint64{7}, validatorParams.GetValidators())
	_, found := validatorParams.GetFeeRecipient(7)
	require.False(t, found)
}

func postSyncCommitteeSubscriptions(t *testing.T, handler *ApiHandler, subs []ValidatorSyncCommitteeSubscriptionsRequest) int {
	t.Helper()
	body, err := json.Marshal(subs)
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/sync_committee_subscriptions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	handler.mux.ServeHTTP(recorder, req)
	return recorder.Code
}

// A validator client subscribes before the period in which its validator joins the committee, so the subnets come
// from the requested committee positions, not from the head state's committee.
func TestSyncCommitteeSubscriptionUsesRequestedIndices(t *testing.T) {
	_, _, _, s, _, handler, _, sd, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	require.NoError(t, sd.OnHeadState(s))
	subcommitteeSize := handler.beaconChainCfg.SyncCommitteeSize / handler.beaconChainCfg.SyncCommitteeSubnetCount
	const untilEpoch = 1_000_000
	expiry := handler.ethClock.GetSlotTime(untilEpoch * handler.beaconChainCfg.SlotsPerEpoch)

	// The head committee puts validator 1 on subnets 0 and 2; request positions on the other two.
	ctrl := gomock.NewController(t)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	for _, subnet := range []int{1, 3} {
		gossipManager.EXPECT().SubscribeWithExpiry(gossip.TopicNameSyncCommittee(subnet), expiry).Return(nil).Times(1)
	}
	handler.gossipManager = gossipManager

	indices := []string{strconv.FormatUint(subcommitteeSize+1, 10), strconv.FormatUint(3*subcommitteeSize, 10), strconv.FormatUint(4*subcommitteeSize-1, 10)}
	require.Equal(t, http.StatusOK, postSyncCommitteeSubscriptions(t, handler, []ValidatorSyncCommitteeSubscriptionsRequest{{
		ValidatorIndex:        1,
		SyncCommitteeIndicies: indices,
		UntilEpoch:            untilEpoch,
	}}))
}

// sync_committee_indices is required; an empty list is a valid request with nothing to subscribe.
func TestSyncCommitteeSubscriptionRequiresIndices(t *testing.T) {
	_, _, _, s, _, handler, _, sd, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	require.NoError(t, sd.OnHeadState(s))
	ctrl := gomock.NewController(t)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	gossipManager.EXPECT().SubscribeWithExpiry(gomock.Any(), gomock.Any()).Times(0)
	handler.gossipManager = gossipManager

	require.Equal(t, http.StatusBadRequest, postSyncCommitteeSubscriptions(t, handler, []ValidatorSyncCommitteeSubscriptionsRequest{
		{ValidatorIndex: 1, UntilEpoch: 1_000_000},
	}))
	require.Equal(t, http.StatusOK, postSyncCommitteeSubscriptions(t, handler, []ValidatorSyncCommitteeSubscriptionsRequest{
		{ValidatorIndex: 1, SyncCommitteeIndicies: []string{}, UntilEpoch: 1_000_000},
	}))
}

func TestSyncCommitteeSubscriptionRejectsInvalidIndices(t *testing.T) {
	_, _, _, s, _, handler, _, sd, _, validatorParams := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	require.NoError(t, sd.OnHeadState(s))
	for _, index := range []string{strconv.FormatUint(handler.beaconChainCfg.SyncCommitteeSize, 10), "x", "-1"} {
		t.Run(index, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			gossipManager := gossip_mock.NewMockGossip(ctrl)
			gossipManager.EXPECT().SubscribeWithExpiry(gomock.Any(), gomock.Any()).Times(0)
			handler.gossipManager = gossipManager

			require.Equal(t, http.StatusBadRequest, postSyncCommitteeSubscriptions(t, handler, []ValidatorSyncCommitteeSubscriptionsRequest{
				{ValidatorIndex: 1, SyncCommitteeIndicies: []string{"0"}, UntilEpoch: 1_000_000},
				{ValidatorIndex: 2, SyncCommitteeIndicies: []string{index}, UntilEpoch: 1_000_000},
			}))
			require.Empty(t, validatorParams.GetValidators())
		})
	}
}
