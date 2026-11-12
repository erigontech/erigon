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

package beaconhttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

func requestWithURLParam(t *testing.T, name, value string) *http.Request {
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add(name, value)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

// A decimal id beyond uint64 is an invalid id, not a root.
func TestIdsRejectADecimalAboveUint64(t *testing.T) {
	const tooLarge = "18446744073709551616"
	_, err := StateIdFromRequest(requestWithURLParam(t, "state_id", tooLarge))
	requireBadRequest(t, err)
	_, err = BlockIdFromRequest(requestWithURLParam(t, "block_id", tooLarge))
	requireBadRequest(t, err)

	id, err := StateIdFromRequest(requestWithURLParam(t, "state_id", "18446744073709551615"))
	require.NoError(t, err)
	require.Equal(t, uint64(18446744073709551615), *id.GetSlot())
}

// An invalid or out-of-range {epoch} is a bad request, as an invalid block or state id is.
func TestEpochFromRequestRejectsInvalidEpochs(t *testing.T) {
	for _, epoch := range []string{"not-an-epoch", "-1", "18446744073709551616"} {
		_, err := EpochFromRequest(requestWithURLParam(t, "epoch", epoch))
		requireBadRequest(t, err)
	}
	epoch, err := EpochFromRequest(requestWithURLParam(t, "epoch", "18446744073709551615"))
	require.NoError(t, err)
	require.Equal(t, uint64(18446744073709551615), epoch)
}

func requireBadRequest(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, http.StatusBadRequest, WrapEndpointError(err).Code)
}
