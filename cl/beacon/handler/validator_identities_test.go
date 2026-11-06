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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
)

type overwriteAfterViewSyncedData struct {
	synced_data.SyncedData
	t    *testing.T
	next *state.CachingBeaconState
}

func (s overwriteAfterViewSyncedData) ViewHeadStateWithIdentity(fn synced_data.ViewHeadStateWithIdentityFn) error {
	return s.SyncedData.ViewHeadStateWithIdentity(func(headState *state.CachingBeaconState, root common.Hash, slot uint64) error {
		defer func() { assert.NoError(s.t, s.next.CopyInto(headState)) }()
		return fn(headState, root, slot)
	})
}

func TestValidatorIdentitiesAtHeadAreReadInsideTheView(t *testing.T) {
	_, _, _, _, postState, handler, _, syncedData, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	next := state.New(postState.BeaconConfig())
	v := solid.NewValidator()
	v.SetPublicKey([48]byte{0xee})
	require.NoError(t, next.AddValidator(v, 32_000_000_000))
	handler.syncedData = overwriteAfterViewSyncedData{SyncedData: syncedData, t: t, next: next}

	server := httptest.NewServer(handler.mux)
	defer server.Close()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/eth/v1/beacon/states/head/validator_identities", nil)
	require.NoError(t, err)
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		Data []struct {
			Pubkey hexutil.Bytes `json:"pubkey"`
		} `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Len(t, body.Data, postState.ValidatorLength())
	want := postState.ValidatorSet().Get(0).PublicKey()
	require.Equal(t, want[:], []byte(body.Data[0].Pubkey))
}

func TestPostStateValidatorIdentities(t *testing.T) {
	_, blocks, _, _, postState, h, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), true)
	var err error
	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	server := httptest.NewServer(h.mux)
	defer server.Close()

	pk, err := postState.ValidatorPublicKey(3)
	require.NoError(t, err)
	for _, tc := range []struct {
		body    string
		indices []string
	}{
		{`["1","` + pk.Hex() + `","99999999"]`, []string{"1", "3"}},
		{``, nil},
		{`[]`, nil},
	} {
		t.Run(tc.body, func(t *testing.T) {
			resp, err := server.Client().Post(server.URL+"/eth/v1/beacon/states/head/validator_identities", "application/json", strings.NewReader(tc.body))
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			var out struct {
				Data []struct {
					Index string `json:"index"`
				} `json:"data"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
			if tc.indices == nil {
				require.Len(t, out.Data, postState.ValidatorLength())
				return
			}
			got := make([]string, 0, len(out.Data))
			for _, d := range out.Data {
				got = append(got, d.Index)
			}
			require.Equal(t, tc.indices, got)
		})
	}
}
