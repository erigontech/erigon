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

type overwriteBeforeViewSyncedData struct {
	synced_data.SyncedData
	t    *testing.T
	next *state.CachingBeaconState
}

func (s overwriteBeforeViewSyncedData) ViewHeadStateWithIdentity(fn synced_data.ViewHeadStateWithIdentityFn) error {
	return s.SyncedData.ViewHeadStateWithIdentity(func(headState *state.CachingBeaconState, root common.Hash, slot uint64) error {
		assert.NoError(s.t, s.next.CopyInto(headState))
		return fn(headState, root, slot)
	})
}

func postValidatorIdentities(t *testing.T, server *httptest.Server, stateID, body string) (int, []string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/eth/v1/beacon/states/"+stateID+"/validator_identities", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, nil
	}
	var out struct {
		Data []struct {
			Index string `json:"index"`
		} `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	got := make([]string, 0, len(out.Data))
	for _, d := range out.Data {
		got = append(got, d.Index)
	}
	return resp.StatusCode, got
}

func TestPostStateValidatorIdentities(t *testing.T) {
	_, blocks, _, _, postState, h, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), true)
	var err error
	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot
	// Make the slot state id below resolve through the historical reader.
	fcu.FinalizedSlotVal = fcu.HeadSlotVal + 1

	server := httptest.NewServer(h.mux)
	defer server.Close()

	pk, err := postState.ValidatorPublicKey(3)
	require.NoError(t, err)
	slotID := strconv.FormatUint(blocks[len(blocks)-1].Block.Slot, 10)
	all := make([]string, 0, postState.ValidatorLength())
	for i := 0; i < postState.ValidatorLength(); i++ {
		all = append(all, strconv.Itoa(i))
	}
	for _, stateID := range []string{"head", slotID} {
		for _, tc := range []struct {
			body    string
			indices []string
		}{
			{`["1","` + pk.Hex() + `","99999999"]`, []string{"1", "3"}},
			{``, all},
			{`[]`, all},
		} {
			t.Run(stateID+"/"+tc.body, func(t *testing.T) {
				code, got := postValidatorIdentities(t, server, stateID, tc.body)
				require.Equal(t, http.StatusOK, code)
				require.Equal(t, tc.indices, got)
			})
		}
	}
	for _, body := range []string{`["abc"]`, `[1]`, `{}`} {
		t.Run("bad/"+body, func(t *testing.T) {
			code, _ := postValidatorIdentities(t, server, "head", body)
			require.Equal(t, http.StatusBadRequest, code)
		})
	}
}

// Public keys must be matched against the state being served, not the head state used to
// resolve ids: here the head is replaced before the view callback runs.
func TestPostStateValidatorIdentitiesMatchPubkeysInServedState(t *testing.T) {
	_, _, _, _, postState, handler, _, syncedData, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	next := state.New(postState.BeaconConfig())
	v := solid.NewValidator()
	newKey := [48]byte{0xee}
	v.SetPublicKey(newKey)
	require.NoError(t, next.AddValidator(v, 32_000_000_000))
	handler.syncedData = overwriteBeforeViewSyncedData{SyncedData: syncedData, t: t, next: next}

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	oldKey, err := postState.ValidatorPublicKey(0)
	require.NoError(t, err)
	// A key that is only in the old head must not match whatever sits at its old index.
	code, got := postValidatorIdentities(t, server, "head", `["`+oldKey.Hex()+`"]`)
	require.Equal(t, http.StatusOK, code)
	require.Empty(t, got)
}
