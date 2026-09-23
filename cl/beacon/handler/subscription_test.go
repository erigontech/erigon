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
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	sync_mock_services "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
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

func TestSyncCommitteeSubscriptionIsUnavailableWhileSyncing(t *testing.T) {
	_, _, _, _, _, handler, _, syncedDataMgr, _, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	syncedDataMgr.(*sync_mock_services.MockSyncedData).EXPECT().ViewHeadState(gomock.Any()).
		Return(synced_data.ErrNotSynced).AnyTimes()

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	// Far enough ahead that the subscription has not expired, without overflowing the slot clock.
	body, err := json.Marshal([]ValidatorSyncCommitteeSubscriptionsRequest{{
		ValidatorIndex:        1,
		SyncCommitteeIndicies: []string{"0"},
		UntilEpoch:            1_000_000_000,
	}})
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		server.URL+"/eth/v1/validator/sync_committee_subscriptions", bytes.NewBuffer(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}
