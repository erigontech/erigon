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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func TestGetSpecIncludesSepoliaGasLimitSchedule(t *testing.T) {
	network, config := clparams.GetConfigsByNetwork(chainspec.SepoliaChainID)
	handler := &ApiHandler{beaconChainCfg: config, netConfig: network}
	response, err := handler.getSpec(nil, nil)
	require.NoError(t, err)
	encoded, err := json.Marshal(response)
	require.NoError(t, err)
	var spec struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	require.NoError(t, json.Unmarshal(encoded, &spec))

	require.Contains(t, spec.Data, "GAS_LIMIT_SCHEDULE")
	require.JSONEq(t, `[{"EPOCH":"353024","GAS_LIMIT":"200000000"}]`, string(spec.Data["GAS_LIMIT_SCHEDULE"]))
}
