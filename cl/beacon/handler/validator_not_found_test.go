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
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestGetStateValidatorUnknownIndexIsNotFound(t *testing.T) {
	_, blocks, _, _, postState, h, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), true)
	var err error
	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot
	fcu.FinalizedSlotVal = fcu.HeadSlotVal + 1

	server := httptest.NewServer(h.mux)
	defer server.Close()
	get := func(t *testing.T, stateID string, idx int) int {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/eth/v1/beacon/states/"+stateID+"/validators/"+strconv.Itoa(idx), nil)
		require.NoError(t, err)
		resp, err := server.Client().Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp.StatusCode
	}
	n := postState.ValidatorLength()
	for _, stateID := range []string{"head", strconv.FormatUint(fcu.HeadSlotVal, 10)} {
		t.Run(stateID, func(t *testing.T) {
			require.Equal(t, http.StatusOK, get(t, stateID, n-1))
			require.Equal(t, http.StatusNotFound, get(t, stateID, n))
			require.Equal(t, http.StatusNotFound, get(t, stateID, n+10))
		})
	}
}

func TestGetLighthouseValidatorInclusionUnknownIndexReturnsNullData(t *testing.T) {
	_, _, _, _, postState, h, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	fcu.FinalizedSlotVal = ^uint64(0)

	server := httptest.NewServer(h.mux)
	defer server.Close()
	get := func(t *testing.T, epoch uint64, validatorID string) (int, string) {
		url := server.URL + "/lighthouse/validator_inclusion/" + strconv.FormatUint(epoch, 10) + "/" + validatorID
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		require.NoError(t, err)
		resp, err := server.Client().Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, string(body)
	}

	validatorCount := postState.ValidatorLength()
	status, _ := get(t, 4, strconv.Itoa(validatorCount-1))
	require.Equal(t, http.StatusOK, status)

	for _, validatorID := range []string{
		strconv.Itoa(validatorCount),
		strconv.Itoa(validatorCount + 10),
		"0x" + strings.Repeat("ff", 48),
		strconv.FormatUint(^uint64(0), 10),
	} {
		status, body := get(t, 4, validatorID)
		require.Equal(t, http.StatusOK, status, "validator id: %s", validatorID)
		require.JSONEq(t, `{"data":null}`, body, "validator id: %s", validatorID)
	}
}

func TestComputeLighthouseValidatorInclusionRejectsShortData(t *testing.T) {
	_, _, _, _, postState, h, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	validatorCount := postState.ValidatorLength()
	earlierValidatorSet := solid.NewValidatorSet(validatorCount - 1)
	for idx := 0; idx < validatorCount-1; idx++ {
		earlierValidatorSet.Append(postState.Validators().Get(idx))
	}
	shortParticipation := solid.NewParticipationBitList(validatorCount-1, validatorCount-1)
	fullParticipation := solid.NewParticipationBitList(validatorCount, validatorCount)

	for _, tc := range []struct {
		name     string
		set      *solid.ValidatorSet
		current  *solid.ParticipationBitList
		previous *solid.ParticipationBitList
		wantErr  bool
	}{
		{name: "earlier registry", set: earlierValidatorSet, current: fullParticipation, previous: fullParticipation},
		{name: "current participation", set: postState.Validators(), current: shortParticipation, previous: fullParticipation, wantErr: true},
		{name: "previous participation", set: postState.Validators(), current: fullParticipation, previous: shortParticipation, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inclusion, err := h.computeLighthouseValidatorInclusion(validatorCount-1, 0, 1, tc.set, tc.current, tc.previous)
			require.Nil(t, inclusion)
			if tc.wantErr {
				require.EqualError(t, err, "Code 500: participation data inconsistent with validator set")
				return
			}
			require.NoError(t, err)
		})
	}
}
