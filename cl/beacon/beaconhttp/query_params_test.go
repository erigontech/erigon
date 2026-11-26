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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUint64FromQueryParamsRejectsInvalidNumbers(t *testing.T) {
	for _, value := range []string{"abc", "-1", "0x10", "18446744073709551616"} {
		_, err := Uint64FromQueryParams(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/?epoch="+value, nil), "epoch")
		require.Error(t, err, value)
		require.Equal(t, http.StatusBadRequest, WrapEndpointError(err).Code, value)
	}

	got, err := Uint64FromQueryParams(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/?epoch=18446744073709551615", nil), "epoch")
	require.NoError(t, err)
	require.Equal(t, uint64(18446744073709551615), *got)

	got, err = Uint64FromQueryParams(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil), "epoch")
	require.NoError(t, err)
	require.Nil(t, got)
}
