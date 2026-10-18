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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestGetLightClientUpdatesRange(t *testing.T) {
	_, _, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), false)

	for period := uint64(0); period < 300; period++ {
		update := cltypes.NewLightClientUpdate(postState.Version(), postState.BeaconConfig())
		update.SignatureSlot = period
		fcu.LCUpdates[period] = update
	}

	cases := []struct {
		start, count uint64
		want         []uint64
	}{
		{start: 1, count: 1, want: []uint64{1}},
		{start: 1, count: 3, want: []uint64{1, 2, 3}},
		{start: 5, count: 0, want: []uint64{}},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("start=%d,count=%d", c.start, c.count), func(t *testing.T) {
			got := fetchLightClientUpdatePeriods(t, handler, c.start, c.count)
			require.Equal(t, c.want, got)
		})
	}

	got := fetchLightClientUpdatePeriods(t, handler, 0, 1000)
	require.Len(t, got, 128)
}

func fetchLightClientUpdatePeriods(t *testing.T, handler *ApiHandler, start, count uint64) []uint64 {
	t.Helper()
	server := httptest.NewServer(handler.mux)
	defer server.Close()

	resp, err := http.Get(fmt.Sprintf("%s/eth/v1/beacon/light_client/updates?start_period=%d&count=%d", server.URL, start, count))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body []struct {
		Data struct {
			SignatureSlot uint64 `json:"signature_slot,string"`
		} `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	periods := make([]uint64, 0, len(body))
	for _, u := range body {
		periods = append(periods, u.Data.SignatureSlot)
	}
	return periods
}
