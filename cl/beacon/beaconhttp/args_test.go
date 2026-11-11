// Copyright 2024 The Erigon Authors
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
	require.Error(t, err)
	_, err = BlockIdFromRequest(requestWithURLParam(t, "block_id", tooLarge))
	require.Error(t, err)

	id, err := StateIdFromRequest(requestWithURLParam(t, "state_id", "18446744073709551615"))
	require.NoError(t, err)
	require.Equal(t, uint64(18446744073709551615), *id.GetSlot())
}
