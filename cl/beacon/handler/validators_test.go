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
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// Per the beacon-APIs spec, Uint64 fields in ValidatorIdentityResponse must be
// serialized as JSON strings. Regression test for erigon#20562.
func TestValidatorIdentityResponseJSONUint64AsString(t *testing.T) {
	t.Parallel()
	v := &validatorIdentityResponse{Index: 1, ActivationEpoch: 1}
	got, err := json.Marshal(v)
	require.NoError(t, err)
	require.Contains(t, string(got), `"index":"1"`)
	require.Contains(t, string(got), `"activation_epoch":"1"`)
}

func TestHeadValidatorsSlowClientDoesNotBlockHeadStatePublish(t *testing.T) {
	t.Parallel()
	_, _, _, _, postState, handler, _, syncedData, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	manager, ok := syncedData.(*synced_data.SyncedDataManager)
	require.True(t, ok)

	w := newStalledStreamWriter()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/states/head/validators", http.NoBody)
	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		handler.mux.ServeHTTP(w, request)
	}()
	defer func() {
		close(w.release)
		<-handlerDone
	}()
	waitDone(t, w.started, "validators handler never wrote its response")

	published := make(chan error, 1)
	go func() { published <- manager.OnHeadState(postState) }()
	select {
	case err := <-published:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("a stalled head validators response blocked the head-state writer")
	}
}

func TestHeadValidatorsResponseMatchesHeadState(t *testing.T) {
	t.Parallel()
	_, _, _, _, postState, handler, _, syncedData, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	fcu.IsRootOptimisticVal = true
	head, err := postState.Copy()
	require.NoError(t, err)
	epoch := state.Epoch(head)
	require.NoError(t, head.SetExitEpochForValidatorAtIndex(0, epoch+1))
	require.NoError(t, head.SetActivationEpochForValidatorAtIndex(1, epoch))
	require.NoError(t, syncedData.(*synced_data.SyncedDataManager).OnHeadState(head))

	for _, tc := range []struct {
		query    string
		ids      []uint64
		statuses []validatorStatus
	}{
		{query: ""},
		{query: "?id=0,1,2&status=active", ids: []uint64{0, 1, 2}, statuses: []validatorStatus{validatorActive}},
		{query: "?id=2,0,2,1,999999&status=active", ids: []uint64{2, 0, 2, 1, 999999}, statuses: []validatorStatus{validatorActive}},
		{query: "?id=1,0&status=active_exiting", ids: []uint64{1, 0}, statuses: []validatorStatus{validatorActiveExiting}},
	} {
		expected := httptest.NewRecorder()
		require.NoError(t, syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
			registryOrder := func(yield func(validatorEntry) bool) {
				s.Validators().Range(func(i int, v solid.Validator, _ int) bool {
					if len(tc.ids) > 0 && !slices.Contains(tc.ids, uint64(i)) {
						return true
					}
					return yield(validatorEntry{i, v, s.Balances().Get(i)})
				})
			}
			responseValidatorEntries(expected, registryOrder, tc.statuses, state.Epoch(s), false, true)
			return nil
		}))

		actual := httptest.NewRecorder()
		handler.mux.ServeHTTP(actual, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/states/head/validators"+tc.query, http.NoBody))
		require.Equal(t, http.StatusOK, actual.Code)
		require.Equal(t, expected.Body.String(), actual.Body.String(), tc.query)
	}
}

func TestHeadValidatorsAreCopiedInsideTheView(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		query string
		ids   []uint64
	}{
		{query: ""},
		{query: "?id=0", ids: []uint64{0}},
	} {
		_, _, _, _, postState, handler, _, syncedData, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
		expected := httptest.NewRecorder()
		require.NoError(t, syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
			responseValidators(expected, tc.ids, nil, state.Epoch(s), s.Balances(), s.Validators(), false, false)
			return nil
		}))

		next, err := postState.Copy()
		require.NoError(t, err)
		require.NoError(t, next.SetValidatorBalance(0, next.Balances().Get(0)+1))
		require.NoError(t, next.SetExitEpochForValidatorAtIndex(0, state.Epoch(next)+1))
		handler.syncedData = overwriteAfterViewSyncedData{SyncedData: syncedData, t: t, next: next}

		actual := httptest.NewRecorder()
		handler.mux.ServeHTTP(actual, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/states/head/validators"+tc.query, http.NoBody))
		require.Equal(t, http.StatusOK, actual.Code)
		require.Equal(t, expected.Body.String(), actual.Body.String(), tc.query)
	}
}

type allocationInViewSyncedData struct {
	synced_data.SyncedData
	allocated *uint64
}

func (s allocationInViewSyncedData) ViewHeadStateWithIdentity(fn synced_data.ViewHeadStateWithIdentityFn) error {
	return s.SyncedData.ViewHeadStateWithIdentity(func(headState *state.CachingBeaconState, root common.Hash, slot uint64) error {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		defer func() {
			runtime.ReadMemStats(&after)
			*s.allocated = after.TotalAlloc - before.TotalAlloc
		}()
		return fn(headState, root, slot)
	})
}

func TestHeadValidatorsFilteredByIdCopyOnlyTheRequestedEntries(t *testing.T) {
	_, _, _, _, postState, handler, _, syncedData, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	head, err := postState.Copy()
	require.NoError(t, err)
	for head.ValidatorLength() < 100_000 {
		require.NoError(t, head.AddValidator(solid.NewValidator(), 32_000_000_000))
	}
	require.NoError(t, syncedData.(*synced_data.SyncedDataManager).OnHeadState(head))
	var allocated uint64
	handler.syncedData = allocationInViewSyncedData{SyncedData: syncedData, allocated: &allocated}

	var ids []string
	for i := range 30_000 {
		ids = append(ids, strconv.Itoa(i%3), strconv.Itoa(head.ValidatorLength()+i))
	}
	response := httptest.NewRecorder()
	handler.mux.ServeHTTP(response, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/states/head/validators?id="+strings.Join(ids, ","), http.NoBody))
	require.Equal(t, http.StatusOK, response.Code)
	require.Less(t, allocated, uint64(8*len(ids)), "bytes allocated under the head-state lock, against one copy of the requested ids")
}

func TestCopyValidatorEntriesByIdAllocatesOneBuffer(t *testing.T) {
	_, _, _, _, postState, _, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ids := make([]uint64, postState.ValidatorLength())
	for i := range ids {
		ids[i] = uint64(i)
	}
	allocs := testing.AllocsPerRun(5, func() {
		_, err := copyValidatorEntries(postState, ids)
		require.NoError(t, err)
	})
	require.Less(t, allocs, float64(10), "mallocs for %d ids", len(ids))
}
