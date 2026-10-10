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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

func TestGetSyncDutiesHistoricalFirstEpochOfPeriod(t *testing.T) {
	db, _, _, _, postState, h, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	cfg := h.beaconChainCfg
	periodSlots := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod

	committeeOf := func(validatorIndex int) *solid.SyncCommittee {
		pk, err := postState.ValidatorPublicKey(validatorIndex)
		require.NoError(t, err)
		members := make([]common.Bytes48, cfg.SyncCommitteeSize)
		for i := range members {
			members[i] = pk
		}
		return solid.NewSyncCommitteeFromParameters(members, pk)
	}
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.Put(kv.CurrentSyncCommittee, base_encoding.Encode64ToBytes4(0), committeeOf(1).Bytes()))
	require.NoError(t, tx.Put(kv.CurrentSyncCommittee, base_encoding.Encode64ToBytes4(periodSlots), committeeOf(2).Bytes()))
	require.NoError(t, tx.Commit())

	server := httptest.NewServer(h.mux)
	defer server.Close()
	epoch := cfg.EpochsPerSyncCommitteePeriod
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/eth/v1/validator/duties/sync/"+strconv.FormatUint(epoch, 10), strings.NewReader(`["1","2"]`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out struct {
		Data []struct {
			ValidatorIndex string `json:"validator_index"`
		} `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Data, 1)
	require.Equal(t, "2", out.Data[0].ValidatorIndex)
}
