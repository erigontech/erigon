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
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

func TestGetSyncDutiesHistoricalPeriod(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	periodSlots := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	for _, tc := range []struct {
		name            string
		altairForkEpoch uint64
		epoch           uint64
		code            int
		validator       string
	}{
		{name: "first epoch of a period", altairForkEpoch: 1, epoch: cfg.EpochsPerSyncCommitteePeriod, code: http.StatusOK, validator: "2"},
		{name: "epoch 0 with altair at genesis", altairForkEpoch: 0, epoch: 0, code: http.StatusOK, validator: "1"},
		{name: "epoch whose period start key aliases a stored period", altairForkEpoch: 1, epoch: (1<<32 + periodSlots) / cfg.SlotsPerEpoch, code: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _, _, _, postState, h, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
			h.beaconChainCfg.AltairForkEpoch = tc.altairForkEpoch

			committeeOf := func(validatorIndex int) *solid.SyncCommittee {
				pk, err := postState.ValidatorPublicKey(validatorIndex)
				require.NoError(t, err)
				return solid.NewSyncCommitteeFromParameters(slices.Repeat([]common.Bytes48{pk}, int(cfg.SyncCommitteeSize)), pk)
			}
			tx, err := db.BeginRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.Put(kv.CurrentSyncCommittee, base_encoding.Encode64ToBytes4(0), committeeOf(1).Bytes()))
			require.NoError(t, tx.Put(kv.CurrentSyncCommittee, base_encoding.Encode64ToBytes4(periodSlots), committeeOf(2).Bytes()))
			require.NoError(t, tx.Commit())

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/duties/sync/"+strconv.FormatUint(tc.epoch, 10), strings.NewReader(`["1","2"]`))
			rec := httptest.NewRecorder()
			h.mux.ServeHTTP(rec, req)
			require.Equal(t, tc.code, rec.Code, rec.Body.String())
			if tc.code != http.StatusOK {
				return
			}

			var out struct {
				Data []struct {
					ValidatorIndex string `json:"validator_index"`
				} `json:"data"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			require.Len(t, out.Data, 1)
			require.Equal(t, tc.validator, out.Data[0].ValidatorIndex)
		})
	}
}

func TestGetSyncDutiesNextPeriodBoundary(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	period := cfg.EpochsPerSyncCommitteePeriod
	for _, tc := range []struct {
		name  string
		epoch uint64
		code  int
	}{
		{"last epoch of the next period", 3*period - 1, http.StatusNotFound},
		{"first epoch past the next period", 3 * period, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, _, _, h, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
			clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
			clock.EXPECT().GetCurrentEpoch().Return(period + 44).AnyTimes()
			h.ethClock = clock

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/duties/sync/"+strconv.FormatUint(tc.epoch, 10), strings.NewReader(`["1"]`))
			rec := httptest.NewRecorder()
			h.mux.ServeHTTP(rec, req)
			require.Equal(t, tc.code, rec.Code, rec.Body.String())
		})
	}
}
