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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestGetAttesterDutiesEpochBounds(t *testing.T) {
	for _, tc := range []struct {
		clockLead     uint64
		nextEpochCode int
	}{
		{0, http.StatusOK},
		{1, http.StatusOK},
		{2, http.StatusServiceUnavailable},
	} {
		t.Run("clockLead="+strconv.FormatUint(tc.clockLead, 10), func(t *testing.T) {
			_, blocks, _, _, headState, h, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
			headRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
			require.NoError(t, err)
			fcu.HeadVal = headRoot
			fcu.HeadSlotVal = headState.Slot()
			headEpoch := headState.Slot() / h.beaconChainCfg.SlotsPerEpoch
			setDutiesClockEpoch(h, headEpoch+tc.clockLead)
			currentEpoch := h.ethClock.GetCurrentEpoch()
			require.Equal(t, headEpoch+tc.clockLead, currentEpoch)
			require.NotEqual(t, headState.GetRandaoMixes(headEpoch), headState.GetRandaoMixes(headEpoch+1))

			for _, request := range []struct {
				epoch uint64
				code  int
			}{
				{currentEpoch, http.StatusOK},
				{currentEpoch + 1, tc.nextEpochCode},
				{currentEpoch + 2, http.StatusBadRequest},
			} {
				t.Run(strconv.FormatUint(request.epoch, 10), func(t *testing.T) {
					req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/duties/attester/"+strconv.FormatUint(request.epoch, 10), strings.NewReader(`["0","1","2"]`))
					req.Header.Set("Content-Type", "application/json")
					recorder := httptest.NewRecorder()
					h.mux.ServeHTTP(recorder, req)
					require.Equal(t, request.code, recorder.Code, recorder.Body.String())
				})
			}
		})
	}
}

func TestGetAttesterDutiesHeadSeedBounds(t *testing.T) {
	_, blocks, _, _, headState, h, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	headRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadVal = headRoot
	fcu.HeadSlotVal = headState.Slot()
	headEpoch := headState.Slot() / h.beaconChainCfg.SlotsPerEpoch

	for _, tc := range []struct {
		name      string
		clockLead uint64
		epochLead uint64
		body      string
		code      int
	}{
		{"empty_beyond_seed", 2, 3, `[]`, http.StatusServiceUnavailable},
		{"empty_at_seed_boundary", 2, 2, `[]`, http.StatusOK},
		{"beyond_old_head_cap", 32, 33, `["0"]`, http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setDutiesClockEpoch(h, headEpoch+tc.clockLead)
			path := "/eth/v1/validator/duties/attester/" + strconv.FormatUint(headEpoch+tc.epochLead, 10)
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			h.mux.ServeHTTP(recorder, req)
			require.Equal(t, tc.code, recorder.Code, recorder.Body.String())
			if tc.code == http.StatusOK {
				var response struct {
					Data          json.RawMessage `json:"data"`
					DependentRoot common.Hash     `json:"dependent_root"`
				}
				require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
				require.JSONEq(t, `[]`, string(response.Data))
				require.Equal(t, common.Hash(headRoot), response.DependentRoot)
			}
		})
	}
}

func setDutiesClockEpoch(h *ApiHandler, epoch uint64) {
	cfg := h.beaconChainCfg
	h.ethClock = eth_clock.NewEthereumClockWithNow(0, common.Hash{}, cfg, func() time.Time {
		return time.Unix(int64(epoch*cfg.SlotsPerEpoch*cfg.SecondsPerSlot), 0)
	})
}
