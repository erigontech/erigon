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
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
)

func TestPostEthV1ValidatorPreparation(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, vp := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), false)
	server := httptest.NewServer(handler.mux)
	defer server.Close()
	req := []ValidatorPreparationPayload{
		{
			ValidatorIndex: 1,
			FeeRecipient:   common.Address{1},
		},
		{
			ValidatorIndex: 2,
			FeeRecipient:   common.Address{2},
		},
	}

	reqByte, err := json.Marshal(req)
	require.NoError(t, err)

	resp, err := http.Post(server.URL+"/eth/v1/validator/prepare_beacon_proposer", "application/json", bytes.NewBuffer(reqByte))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	a1, _ := vp.GetFeeRecipient(1)
	a2, _ := vp.GetFeeRecipient(2)

	require.Equal(t, common.Address{1}, a1)
	require.Equal(t, common.Address{2}, a2)
}
