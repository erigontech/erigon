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
			cfg := h.beaconChainCfg
			headEpoch := headState.Slot() / cfg.SlotsPerEpoch
			clockSlot := (headEpoch + tc.clockLead) * cfg.SlotsPerEpoch
			h.ethClock = eth_clock.NewEthereumClockWithNow(0, common.Hash{}, cfg, func() time.Time {
				return time.Unix(int64(clockSlot*cfg.SecondsPerSlot), 0)
			})
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
