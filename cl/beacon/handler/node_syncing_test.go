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
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	sync_mock_services "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	forkchoicemock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

func TestNodeSyncingSlotSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name         string
		headSlot     uint64
		nextHeadSlot uint64
		wantDistance string
	}{
		{"advancing head", 900, 950, "100"},
		{"head at current slot", 1000, 1000, "0"},
		{"head ahead of clock", 1001, 1001, "0"},
		{"no head", 0, 0, "1000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			clock := eth_clock.NewMockEthereumClock(ctrl)
			clock.EXPECT().GetCurrentSlot().Return(uint64(1000)).Times(1)
			syncedData := sync_mock_services.NewMockSyncedData(ctrl)
			headSlot := tc.headSlot
			syncedData.EXPECT().HeadSlot().DoAndReturn(func() uint64 {
				slot := headSlot
				headSlot = tc.nextHeadSlot
				return slot
			}).AnyTimes()
			syncedData.EXPECT().StateHead().Return(common.Hash{}, tc.headSlot, tc.headSlot != 0)
			cfg := clparams.MainnetBeaconConfig
			handler := &ApiHandler{
				ethClock:        clock,
				syncedData:      syncedData,
				beaconChainCfg:  &cfg,
				forkchoiceStore: &forkchoicemock.ForkChoiceStorageMock{},
			}

			resp, err := handler.GetEthV1NodeSyncing(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/node/syncing", http.NoBody))
			require.NoError(t, err)
			data := resp.Data.(map[string]any)
			require.Equal(t, strconv.FormatUint(tc.headSlot, 10), data["head_slot"])
			require.Equal(t, tc.wantDistance, data["sync_distance"])
			require.Equal(t, tc.headSlot == 0, data["is_syncing"])
		})
	}
}
