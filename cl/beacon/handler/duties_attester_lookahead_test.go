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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

func TestGetAttesterDutiesHeadSeedBounds(t *testing.T) {
	_, blocks, _, _, headState, h, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	headRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadVal = headRoot
	fcu.HeadSlotVal = headState.Slot()
	headEpoch := headState.Slot() / h.beaconChainCfg.SlotsPerEpoch
	require.NotEqual(t, headState.GetRandaoMixes(headEpoch), headState.GetRandaoMixes(headEpoch+1))

	const validators = `["0","1","2"]`

	for _, tc := range []struct {
		name      string
		clockLead uint64
		epochLead uint64
		body      string
		code      int
	}{
		{"clock_0_request_0", 0, 0, validators, http.StatusOK},
		{"clock_0_request_1", 0, 1, validators, http.StatusOK},
		{"clock_0_request_2", 0, 2, validators, http.StatusBadRequest},
		{"clock_1_request_1", 1, 1, validators, http.StatusOK},
		{"clock_1_request_2", 1, 2, validators, http.StatusOK},
		{"clock_1_request_3", 1, 3, validators, http.StatusBadRequest},
		{"clock_2_request_2", 2, 2, validators, http.StatusOK},
		{"clock_2_request_3", 2, 3, validators, http.StatusServiceUnavailable},
		{"clock_2_request_4", 2, 4, validators, http.StatusBadRequest},
		{"beyond_old_head_cap", 32, 33, `["0"]`, http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clockEpoch := headEpoch + tc.clockLead
			setDutiesClockEpoch(h, clockEpoch)
			require.Equal(t, clockEpoch, h.ethClock.GetCurrentEpoch())
			path := "/eth/v1/validator/duties/attester/" + strconv.FormatUint(headEpoch+tc.epochLead, 10)
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			h.mux.ServeHTTP(recorder, req)
			require.Equal(t, tc.code, recorder.Code, recorder.Body.String())
		})
	}
}

func TestGetAttesterDutiesUsesCurrentEpochAtCheckpointFloor(t *testing.T) {
	db, _, _, _, anchor, h, _, manager, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return state_accessors.SetStateProcessingProgress(tx, 0)
	}))

	const epoch = 2
	cfg := h.beaconChainCfg
	epochSlot := uint64(epoch) * cfg.SlotsPerEpoch
	floor := epochSlot + 1
	fcu.LowestAvailableSlotVal = &floor
	require.NoError(t, anchor.SetSlot(floor))
	dependentRoot := common.Hash{0x42}
	require.NoError(t, anchor.SetBlockRootAt(int(cfg.SlotsPerEpoch-1), dependentRoot))
	require.NoError(t, manager.OnHeadStateWithBlockRoot(anchor, common.Hash{0x43}))
	setDutiesClockEpoch(h, epoch)

	path := "/eth/v1/validator/duties/attester/" + strconv.FormatUint(epoch, 10)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(`["0"]`))
	recorder := httptest.NewRecorder()
	h.mux.ServeHTTP(recorder, req)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var response struct {
		Data          []attesterDutyResponse `json:"data"`
		DependentRoot common.Hash            `json:"dependent_root"`
	}
	require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
	require.Equal(t, dependentRoot, response.DependentRoot)
	require.Len(t, response.Data, 1)
	require.Zero(t, response.Data[0].ValidatorIndex)
	require.GreaterOrEqual(t, response.Data[0].Slot, epochSlot)
	require.Less(t, response.Data[0].Slot, epochSlot+cfg.SlotsPerEpoch)
}

func setDutiesClockEpoch(h *ApiHandler, epoch uint64) {
	cfg := h.beaconChainCfg
	h.ethClock = eth_clock.NewEthereumClockWithNow(0, common.Hash{}, cfg, func() time.Time {
		return time.Unix(int64(epoch*cfg.SlotsPerEpoch*cfg.SecondsPerSlot), 0)
	})
}
