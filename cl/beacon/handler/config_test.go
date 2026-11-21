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
	"github.com/erigontech/erigon/cl/gossip"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func specData(t *testing.T, cfg *clparams.BeaconChainConfig, net *clparams.NetworkConfig) map[string]json.RawMessage {
	t.Helper()
	response, err := (&ApiHandler{beaconChainCfg: cfg, netConfig: net}).getSpec(nil, nil)
	require.NoError(t, err)
	encoded, err := json.Marshal(response)
	require.NoError(t, err)
	var spec struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	require.NoError(t, json.Unmarshal(encoded, &spec))
	return spec.Data
}

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
			spec := specData(t, config, network)

			require.Contains(t, spec, "GAS_LIMIT_SCHEDULE")
			require.JSONEq(t, test.schedule, string(spec["GAS_LIMIT_SCHEDULE"]))
		})
	}
}

func TestGetSpecIncludesMissingKeys(t *testing.T) {
	network, config := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	spec := specData(t, config, network)

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
		require.Contains(t, spec, key, key)
		require.JSONEq(t, expected[key], string(spec[key]), key)
	}
}

func TestGetSpecUsesSpecConfigKeyNames(t *testing.T) {
	for _, name := range []string{"mainnet", "sepolia"} {
		t.Run(name, func(t *testing.T) {
			network, config, _, err := clparams.GetConfigsByNetworkName(name)
			require.NoError(t, err)
			spec := specData(t, config, network)
			for _, test := range []struct{ key, oldKey, want string }{
				{"MAX_PAYLOAD_SIZE", "GOSSIP_MAX_SIZE", `"10485760"`},
				{"MAXIMUM_GOSSIP_CLOCK_DISPARITY", "MAXIMUM_GOSSIP_CLOCK_DISPARITY_MILLIS", `"500"`},
				{"PENDING_DEPOSITS_LIMIT", "PENDING_DEPOSIT_LIMIT", `"134217728"`},
			} {
				t.Run(test.key, func(t *testing.T) {
					if _, ok := spec[test.oldKey]; ok {
						t.Errorf("unexpected response key %s", test.oldKey)
					}
					got, ok := spec[test.key]
					if !ok {
						t.Fatalf("%s: expected %s, got <missing>", test.key, test.want)
					}
					require.JSONEq(t, test.want, string(got), test.key)
				})
			}
		})
	}
}

func TestGetSpecCustomTimingMatchesRuntime(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte("ATTESTATION_DUE_BPS: 2500\nPAYLOAD_ATTESTATION_DUE_BPS: 6000\nREORG_HEAD_WEIGHT_THRESHOLD: 10\nREORG_PARENT_WEIGHT_THRESHOLD: 80\nREORG_MAX_EPOCHS_SINCE_FINALIZATION: 1\nPROPOSER_REORG_CUTOFF_BPS: 900\nCONFIRMATION_BYZANTINE_THRESHOLD: 12\nEPOCHS_PER_SUBNET_SUBSCRIPTION: 128\nATTESTATION_SUBNET_EXTRA_BITS: 3\n"), 0o644))
	config, network, err := clparams.CustomConfig(configPath)
	require.NoError(t, err)
	spec := specData(t, &config, &network)

	require.JSONEq(t, `"2500"`, string(spec["ATTESTATION_DUE_BPS"]))
	require.Equal(t, uint64(3000), config.AttestationDueMs(false))
	require.JSONEq(t, `"6000"`, string(spec["PAYLOAD_ATTESTATION_DUE_BPS"]))
	require.Equal(t, uint64(7200), config.PayloadAttestationDueMs())
	require.JSONEq(t, `"10"`, string(spec["REORG_HEAD_WEIGHT_THRESHOLD"]))
	require.JSONEq(t, `"160"`, string(spec["REORG_PARENT_WEIGHT_THRESHOLD"]))
	require.JSONEq(t, `"2"`, string(spec["REORG_MAX_EPOCHS_SINCE_FINALIZATION"]))
	require.JSONEq(t, `"1667"`, string(spec["PROPOSER_REORG_CUTOFF_BPS"]))
	require.JSONEq(t, `"25"`, string(spec["CONFIRMATION_BYZANTINE_THRESHOLD"]))
	require.JSONEq(t, `"256"`, string(spec["EPOCHS_PER_SUBNET_SUBSCRIPTION"]))
	require.JSONEq(t, `"0"`, string(spec["ATTESTATION_SUBNET_EXTRA_BITS"]))
}

func TestGetSpecUsesResolvedPresetSizes(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte("PRESET_BASE: minimal\n"), 0o644))
	config, network, err := clparams.CustomConfig(configPath)
	require.NoError(t, err)
	spec := specData(t, &config, &network)

	require.Equal(t, uint64(4), config.MaxCommitteesPerSlot)
	require.JSONEq(t, `"131536"`, string(spec["MAX_ATTESTER_SLASHING_SIZE"]))
	require.JSONEq(t, `"1462"`, string(spec["MAX_SIGNED_AGGREGATE_AND_PROOF_SIZE"]))
	require.JSONEq(t, `"196932"`, string(spec["MAX_SIGNED_EXECUTION_PAYLOAD_BID_SIZE"]))
	require.Equal(t, uint64(131536), gossip.MaxUncompressedSize(gossip.TopicNameAttesterSlashing, &config, &network))
	require.Equal(t, uint64(1462), gossip.MaxUncompressedSize(gossip.TopicNameBeaconAggregateAndProof, &config, &network))
}

func TestGetSpecUsesUncappedPresetSizes(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte("MAX_PAYLOAD_SIZE: 1024\n"), 0o644))
	config, network, err := clparams.CustomConfig(configPath)
	require.NoError(t, err)
	spec := specData(t, &config, &network)

	require.JSONEq(t, `"2097616"`, string(spec["MAX_ATTESTER_SLASHING_SIZE"]))
	require.JSONEq(t, `"16829"`, string(spec["MAX_SIGNED_AGGREGATE_AND_PROOF_SIZE"]))
	require.Equal(t, uint64(1024), gossip.MaxUncompressedSize(gossip.TopicNameAttesterSlashing, &config, &network))
	require.Equal(t, uint64(1024), gossip.MaxUncompressedSize(gossip.TopicNameBeaconAggregateAndProof, &config, &network))
}

func TestGetSpecDerivesUpdateTimeout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("PRESET_BASE: minimal\n"), 0o644))
	config, network, err := clparams.CustomConfig(path)
	require.NoError(t, err)
	spec := specData(t, &config, &network)
	require.JSONEq(t, `"64"`, string(spec["UPDATE_TIMEOUT"]))
}

func TestNamedNetworkRuntimeConfigDefaults(t *testing.T) {
	_, mainnetConfig, _, err := clparams.GetConfigsByNetworkName("mainnet")
	require.NoError(t, err)
	require.Equal(t, clparams.AttestationDueBps, mainnetConfig.AttestationDueBps)
	require.Equal(t, clparams.AggregateDueBps, mainnetConfig.AggregateDueBps)
	require.Equal(t, clparams.SyncMessageDueBps, mainnetConfig.SyncMessageDueBps)
	require.Equal(t, clparams.ContributionDueBps, mainnetConfig.ContributionDueBps)
	require.Equal(t, clparams.AttestationDueBpsGloas, mainnetConfig.AttestationDueBpsGloas)
	require.Equal(t, clparams.AggregateDueBpsGloas, mainnetConfig.AggregateDueBpsGloas)
	require.Equal(t, clparams.SyncMessageDueBpsGloas, mainnetConfig.SyncMessageDueBpsGloas)
	require.Equal(t, clparams.ContributionDueBpsGloas, mainnetConfig.ContributionDueBpsGloas)
	require.Equal(t, clparams.PayloadAttestationDueBps, mainnetConfig.PayloadAttestationDueBps)
	require.Equal(t, clparams.ReorgHeadWeightThreshold, mainnetConfig.ReorgHeadWeightThreshold)
	for _, name := range []string{"mainnet", "sepolia", "hoodi", "gnosis", "chiado", "bloatnet"} {
		t.Run(name, func(t *testing.T) {
			_, config, _, err := clparams.GetConfigsByNetworkName(name)
			require.NoError(t, err)
			require.Equal(t, mainnetConfig.AttestationDueBps, config.AttestationDueBps)
			require.Equal(t, mainnetConfig.AggregateDueBps, config.AggregateDueBps)
			require.Equal(t, mainnetConfig.SyncMessageDueBps, config.SyncMessageDueBps)
			require.Equal(t, mainnetConfig.ContributionDueBps, config.ContributionDueBps)
			require.Equal(t, mainnetConfig.AttestationDueBpsGloas, config.AttestationDueBpsGloas)
			require.Equal(t, mainnetConfig.AggregateDueBpsGloas, config.AggregateDueBpsGloas)
			require.Equal(t, mainnetConfig.SyncMessageDueBpsGloas, config.SyncMessageDueBpsGloas)
			require.Equal(t, mainnetConfig.ContributionDueBpsGloas, config.ContributionDueBpsGloas)
			require.Equal(t, mainnetConfig.PayloadAttestationDueBps, config.PayloadAttestationDueBps)
			require.Equal(t, mainnetConfig.ReorgHeadWeightThreshold, config.ReorgHeadWeightThreshold)
			require.Equal(t, config.SecondsPerSlot*config.AttestationDueBps/(clparams.BpsFactor/1000), config.AttestationDueMs(false))
			require.Equal(t, config.SecondsPerSlot*config.AttestationDueBpsGloas/(clparams.BpsFactor/1000), config.AttestationDueMs(true))
			require.Equal(t, config.SecondsPerSlot*config.PayloadAttestationDueBps/(clparams.BpsFactor/1000), config.PayloadAttestationDueMs())
		})
	}
}

func TestCustomConfigOmittingAddedKeysUsesMainnetDefaults(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte("CONFIG_NAME: custom\n"), 0o644))
	config, network, err := clparams.CustomConfig(configPath)
	require.NoError(t, err)
	mainnetNetwork, mainnetConfig, _, err := clparams.GetConfigsByNetworkName("mainnet")
	require.NoError(t, err)
	require.Equal(t, mainnetConfig.AttestationDueMs(false), config.AttestationDueMs(false))
	require.Equal(t, mainnetConfig.AttestationDueMs(true), config.AttestationDueMs(true))
	require.Equal(t, mainnetConfig.PayloadAttestationDueMs(), config.PayloadAttestationDueMs())
	require.Equal(t, mainnetConfig.ReorgHeadWeightThreshold, config.ReorgHeadWeightThreshold)

	spec := specData(t, &config, &network)
	mainnetSpec := specData(t, mainnetConfig, mainnetNetwork)
	for _, key := range []string{
		"SLOT_DURATION_MS", "PROPOSER_REORG_CUTOFF_BPS", "ATTESTATION_DUE_BPS", "AGGREGATE_DUE_BPS",
		"SYNC_MESSAGE_DUE_BPS", "CONTRIBUTION_DUE_BPS", "ATTESTATION_DUE_BPS_GLOAS", "AGGREGATE_DUE_BPS_GLOAS",
		"SYNC_MESSAGE_DUE_BPS_GLOAS", "CONTRIBUTION_DUE_BPS_GLOAS", "PAYLOAD_ATTESTATION_DUE_BPS",
		"REORG_HEAD_WEIGHT_THRESHOLD", "REORG_PARENT_WEIGHT_THRESHOLD", "REORG_MAX_EPOCHS_SINCE_FINALIZATION",
		"CONFIRMATION_BYZANTINE_THRESHOLD", "ATTESTATION_SUBNET_EXTRA_BITS", "EPOCHS_PER_SUBNET_SUBSCRIPTION",
		"MAX_ATTESTER_SLASHING_SIZE", "MAX_SIGNED_AGGREGATE_AND_PROOF_SIZE", "MAX_SIGNED_EXECUTION_PAYLOAD_BID_SIZE", "UPDATE_TIMEOUT",
	} {
		require.JSONEq(t, string(mainnetSpec[key]), string(spec[key]), key)
	}
}
