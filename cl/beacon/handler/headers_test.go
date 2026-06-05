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

package handler

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestGetHeadersIncludesFinalized(t *testing.T) {
	_, blocks, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	blockRoot, err := blocks[0].Block.HashSSZ()
	require.NoError(t, err)
	parentRoot := blocks[0].Block.ParentRoot
	missingParentRoot := common.Hash{0xbe, 0xef}

	for _, tc := range []struct {
		name          string
		parentRoot    common.Hash
		finalizedSlot uint64
		finalized     bool
		expectedRoots []common.Hash
	}{
		{
			name:          "not finalized",
			parentRoot:    parentRoot,
			finalizedSlot: 0,
			finalized:     false,
			expectedRoots: []common.Hash{common.Hash(blockRoot)},
		},
		{
			name:          "finalized",
			parentRoot:    parentRoot,
			finalizedSlot: math.MaxUint64,
			finalized:     true,
			expectedRoots: []common.Hash{common.Hash(blockRoot)},
		},
		{
			name:          "no matching headers",
			parentRoot:    missingParentRoot,
			finalizedSlot: math.MaxUint64,
			finalized:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcu.FinalizedSlotVal = tc.finalizedSlot
			server := httptest.NewServer(handler.mux)
			defer server.Close()

			resp, err := http.Get(server.URL + "/eth/v1/beacon/headers?parent_root=0x" + common.Bytes2Hex(tc.parentRoot[:]))
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var body struct {
				Finalized           *bool `json:"finalized"`
				ExecutionOptimistic *bool `json:"execution_optimistic"`
				Data                []struct {
					Root      common.Hash `json:"root"`
					Canonical bool        `json:"canonical"`
				} `json:"data"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
			require.NotNil(t, body.ExecutionOptimistic)
			require.NotNil(t, body.Finalized)
			require.Equal(t, tc.finalized, *body.Finalized)
			require.Len(t, body.Data, len(tc.expectedRoots))
			for i, expectedRoot := range tc.expectedRoots {
				require.Equal(t, expectedRoot, body.Data[i].Root)
				require.True(t, body.Data[i].Canonical)
			}
		})
	}
}
