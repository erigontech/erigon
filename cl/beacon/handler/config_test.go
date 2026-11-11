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
	"slices"
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

func TestGetSpecIncludesMissingKeys(t *testing.T) {
	network, config := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	handler := &ApiHandler{beaconChainCfg: config, netConfig: network}
	response, err := handler.getSpec(nil, nil)
	require.NoError(t, err)
	encoded, err := json.Marshal(response)
	require.NoError(t, err)
	var spec struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	require.NoError(t, json.Unmarshal(encoded, &spec))

	expected := map[string]string{
		"SLOT_DURATION_MS":                      `"12000"`,
		"PROPOSER_REORG_CUTOFF_BPS":             `"1667"`,
		"ATTESTATION_DUE_BPS":                   `"3333"`,
		"AGGREGATE_DUE_BPS":                     `"6667"`,
		"SYNC_MESSAGE_DUE_BPS":                  `"3333"`,
		"CONTRIBUTION_DUE_BPS":                  `"6667"`,
		"ATTESTATION_DUE_BPS_GLOAS":             `"2500"`,
		"AGGREGATE_DUE_BPS_GLOAS":               `"5000"`,
		"SYNC_MESSAGE_DUE_BPS_GLOAS":            `"2500"`,
		"CONTRIBUTION_DUE_BPS_GLOAS":            `"5000"`,
		"PAYLOAD_ATTESTATION_DUE_BPS":           `"7500"`,
		"REORG_HEAD_WEIGHT_THRESHOLD":           `"20"`,
		"REORG_PARENT_WEIGHT_THRESHOLD":         `"160"`,
		"REORG_MAX_EPOCHS_SINCE_FINALIZATION":   `"2"`,
		"CONFIRMATION_BYZANTINE_THRESHOLD":      `"25"`,
		"ATTESTATION_SUBNET_EXTRA_BITS":         `"0"`,
		"EPOCHS_PER_SUBNET_SUBSCRIPTION":        `"256"`,
		"MAXIMUM_GOSSIP_CLOCK_DISPARITY":        `"500"`,
		"MAX_PAYLOAD_SIZE":                      `"10485760"`,
		"CELLS_PER_EXT_BLOB":                    `"128"`,
		"FIELD_ELEMENTS_PER_CELL":               `"64"`,
		"FIELD_ELEMENTS_PER_EXT_BLOB":           `"8192"`,
		"KZG_COMMITMENT_INCLUSION_PROOF_DEPTH":  `"17"`,
		"KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH": `"4"`,
		"PENDING_DEPOSITS_LIMIT":                `"134217728"`,
		"MAX_ATTESTER_SLASHING_SIZE":            `"2097616"`,
		"MAX_SIGNED_AGGREGATE_AND_PROOF_SIZE":   `"16829"`,
		"MAX_SIGNED_EXECUTION_PAYLOAD_BID_SIZE": `"196932"`,
		"UPDATE_TIMEOUT":                        `"8192"`,
		"DOMAIN_BLS_TO_EXECUTION_CHANGE":        `"0x0a000000"`,
		"DOMAIN_BEACON_BUILDER":                 `"0x0b000000"`,
		"DOMAIN_BUILDER_DEPOSIT":                `"0x0e000000"`,
		"DOMAIN_PROPOSER_PREFERENCES":           `"0x0d000000"`,
		"DOMAIN_PTC_ATTESTER":                   `"0x0c000000"`,
	}
	keys := make([]string, 0, len(expected))
	for key := range expected {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		require.Contains(t, spec.Data, key, key)
		require.JSONEq(t, expected[key], string(spec.Data[key]), key)
	}
}
