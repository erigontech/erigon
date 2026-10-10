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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
)

func TestGetNodeHealthWhileSyncing(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	a := &ApiHandler{syncedData: synced_data.NewSyncedDataManager(&cfg, true)}
	require.True(t, a.syncedData.Syncing())

	for _, tc := range []struct {
		query string
		code  int
	}{
		{"", http.StatusPartialContent},
		{"?syncing_status=503", http.StatusServiceUnavailable},
		{"?syncing_status=42", http.StatusBadRequest},
		{"?syncing_status=600", http.StatusBadRequest},
	} {
		t.Run(tc.query, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/node/health"+tc.query, nil)
			require.NotPanics(t, func() { a.GetEthV1NodeHealth(rec, req) })
			require.Equal(t, tc.code, rec.Code)
		})
	}
}
