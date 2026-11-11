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

func TestGetLighthouseValidatorInclusionUnknownIndexIsNotFound(t *testing.T) {
	_, _, _, _, postState, h, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	fcu.FinalizedSlotVal = ^uint64(0)

	server := httptest.NewServer(h.mux)
	defer server.Close()
	get := func(epoch uint64, idx int) (int, string, error) {
		url := server.URL + "/lighthouse/validator_inclusion/" + strconv.FormatUint(epoch, 10) + "/" + strconv.Itoa(idx)
		resp, err := server.Client().Get(url)
		if err != nil {
			return 0, "", err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body), err
	}

	validatorCount := postState.ValidatorLength()
	status, body, err := get(4, 1)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	want, err := testData.ReadFile("test_data/harness_inclusion_local_1.json")
	require.NoError(t, err)
	require.JSONEq(t, string(want), body)

	status, _, err = get(4, validatorCount-1)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)

	for _, idx := range []int{validatorCount, validatorCount + 10} {
		status, body, err = get(4, idx)
		if err != nil {
			status = 0
			body = err.Error()
		}
		require.Equal(t, http.StatusNotFound, status, "response: %s", body)
		require.True(t, strings.Contains(body, "validator not found"), "response: %s", body)
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
	compute := func(set *solid.ValidatorSet, current, previous *solid.ParticipationBitList) (err error) {
		defer func() {
			if recover() != nil {
				err = nil
			}
		}()
		_, err = h.computeLighthouseValidatorInclusion(validatorCount-1, 0, 1, set, current, previous)
		return err
	}

	for _, tc := range []struct {
		name     string
		set      *solid.ValidatorSet
		current  *solid.ParticipationBitList
		previous *solid.ParticipationBitList
	}{
		{name: "earlier registry", set: earlierValidatorSet, current: shortParticipation, previous: shortParticipation},
		{name: "current participation", set: postState.Validators(), current: shortParticipation, previous: fullParticipation},
		{name: "previous participation", set: postState.Validators(), current: fullParticipation, previous: shortParticipation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, compute(tc.set, tc.current, tc.previous), "validator not found")
		})
	}
}
