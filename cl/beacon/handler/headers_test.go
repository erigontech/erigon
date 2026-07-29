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
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	antiquarytests "github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestGetHeadersIncludesFinalized(t *testing.T) {
	db, blocks, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	blockRoot, err := blocks[0].Block.HashSSZ()
	require.NoError(t, err)
	canonicalOnlyBlockRoot, err := blocks[1].Block.HashSSZ()
	require.NoError(t, err)
	parentRoot := blocks[0].Block.ParentRoot
	canonicalOnlyParentRoot := blocks[1].Block.ParentRoot
	missingParentRoot := common.Hash{0xbe, 0xef}
	nonCanonicalBlock := cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, clparams.Phase0Version)
	nonCanonicalBlock.Block.Slot = blocks[len(blocks)-1].Block.Slot + 1
	nonCanonicalBlock.Block.ParentRoot = parentRoot
	nonCanonicalBlock.Block.StateRoot = common.Hash{0xca, 0xfe}
	nonCanonicalRoot, err := nonCanonicalBlock.Block.HashSSZ()
	require.NoError(t, err)
	blockReader := handler.blockReader.(*antiquarytests.MockBlockReader)
	blockReader.U[nonCanonicalBlock.Block.Slot] = nonCanonicalBlock
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, beacon_indicies.WriteBeaconBlockAndIndicies(context.Background(), tx, nonCanonicalBlock, false))
	require.NoError(t, tx.Commit())

	type expectedHeader struct {
		root      common.Hash
		canonical bool
	}

	for _, tc := range []struct {
		name          string
		parentRoot    common.Hash
		finalizedSlot uint64
		finalized     bool
		expected      []expectedHeader
	}{
		{
			name:          "not finalized",
			parentRoot:    parentRoot,
			finalizedSlot: 0,
			finalized:     false,
			expected: []expectedHeader{
				{root: common.Hash(blockRoot), canonical: true},
				{root: common.Hash(nonCanonicalRoot), canonical: false},
			},
		},
		{
			name:          "finalized",
			parentRoot:    canonicalOnlyParentRoot,
			finalizedSlot: math.MaxUint64,
			finalized:     true,
			expected: []expectedHeader{
				{root: common.Hash(canonicalOnlyBlockRoot), canonical: true},
			},
		},
		{
			name:          "non canonical child keeps envelope not finalized",
			parentRoot:    parentRoot,
			finalizedSlot: math.MaxUint64,
			finalized:     false,
			expected: []expectedHeader{
				{root: common.Hash(blockRoot), canonical: true},
				{root: common.Hash(nonCanonicalRoot), canonical: false},
			},
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
			require.Len(t, body.Data, len(tc.expected))
			for i, expected := range tc.expected {
				require.Equal(t, expected.root, body.Data[i].Root)
				require.Equal(t, expected.canonical, body.Data[i].Canonical)
			}
		})
	}
}
