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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func TestGetSpecIncludesGasLimitSchedule(t *testing.T) {
	for _, test := range []struct {
		name         string
		network      clparams.NetworkType
		customConfig string
		schedule     string
	}{
		{name: "mainnet", network: chainspec.MainnetChainID, schedule: `[]`},
		{name: "sepolia", network: chainspec.SepoliaChainID, schedule: `[{"EPOCH":"353024","GAS_LIMIT":"200000000"}]`},
		{name: "custom omitted", customConfig: "GLOAS_FORK_EPOCH: 10\n", schedule: `[]`},
		{name: "custom empty value", customConfig: "GLOAS_FORK_EPOCH: 10\nGAS_LIMIT_SCHEDULE:\n", schedule: `[]`},
		{name: "custom null", customConfig: "GLOAS_FORK_EPOCH: 10\nGAS_LIMIT_SCHEDULE: null\n", schedule: `[]`},
		{name: "custom empty array", customConfig: "GLOAS_FORK_EPOCH: 10\nGAS_LIMIT_SCHEDULE: []\n", schedule: `[]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var network *clparams.NetworkConfig
			var config *clparams.BeaconChainConfig
			if test.customConfig == "" {
				network, config = clparams.GetConfigsByNetwork(test.network)
			} else {
				configPath := filepath.Join(t.TempDir(), "config.yaml")
				require.NoError(t, os.WriteFile(configPath, []byte(test.customConfig), 0o644))
				beaconCfg, networkCfg, err := clparams.CustomConfig(configPath)
				require.NoError(t, err)
				network, config = &networkCfg, &beaconCfg
			}
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
			require.JSONEq(t, test.schedule, string(spec.Data["GAS_LIMIT_SCHEDULE"]))
		})
	}
}
