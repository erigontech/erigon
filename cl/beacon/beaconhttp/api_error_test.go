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
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
)

func TestWrapEndpointErrorStatusCodes(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code int
	}{
		{"not synced", synced_data.ErrNotSynced, http.StatusServiceUnavailable},
		{"not synced, wrapped", fmt.Errorf("attester duties: %w", synced_data.ErrNotSynced), http.StatusServiceUnavailable},
		{"state not found", fork_graph.ErrStateNotFound, http.StatusNotFound},
		{"anything else", errors.New("boom"), http.StatusInternalServerError},
		{"explicit code is preserved", NewEndpointError(http.StatusBadRequest, errors.New("bad")), http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.code, WrapEndpointError(tc.err).Code)
		})
	}
}
