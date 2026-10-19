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

func TestGetAttesterDutiesRejectsEpochBeyondNext(t *testing.T) {
	// clockLead is how many epochs the wall clock is ahead of the head: the bound
	// must follow the clock, not the head.
	for _, clockLead := range []uint64{0, 1} {
		t.Run("clockLead="+strconv.FormatUint(clockLead, 10), func(t *testing.T) {
			_, blocks, _, _, _, h, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), true)
			var err error
			fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
			require.NoError(t, err)
			headSlot := blocks[len(blocks)-1].Block.Slot
			fcu.HeadSlotVal = headSlot
			cfg := h.beaconChainCfg
			genesisTime := uint64(time.Now().Unix()) - (headSlot/cfg.SlotsPerEpoch+clockLead)*cfg.SlotsPerEpoch*cfg.SecondsPerSlot
			h.ethClock = eth_clock.NewEthereumClock(genesisTime, common.Hash{}, cfg)
			currentEpoch := h.ethClock.GetCurrentEpoch()
			require.Equal(t, headSlot/cfg.SlotsPerEpoch+clockLead, currentEpoch)

			server := httptest.NewServer(h.mux)
			defer server.Close()
			for _, tc := range []struct {
				epoch uint64
				code  int
			}{
				{currentEpoch, http.StatusOK},
				{currentEpoch + 1, http.StatusOK},
				{currentEpoch + 2, http.StatusBadRequest},
			} {
				t.Run(strconv.FormatUint(tc.epoch, 10), func(t *testing.T) {
					resp, err := server.Client().Post(server.URL+"/eth/v1/validator/duties/attester/"+strconv.FormatUint(tc.epoch, 10), "application/json", strings.NewReader(`["0","1","2"]`))
					require.NoError(t, err)
					defer resp.Body.Close()
					require.Equal(t, tc.code, resp.StatusCode)
				})
			}
		})
	}
}
