// Copyright 2022 The Erigon Authors
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

package clparams

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func testConfig(t *testing.T, n NetworkType) {
	network, beacon := GetConfigsByNetwork(n)

	require.Equal(t, *network, NetworkConfigs[n])
	require.Equal(t, *beacon, BeaconConfigs[n])
}

func TestGetConfigsByNetwork(t *testing.T) {
	testConfig(t, chainspec.MainnetChainID)
	testConfig(t, chainspec.SepoliaChainID)
	testConfig(t, chainspec.GnosisChainID)
	testConfig(t, chainspec.ChiadoChainID)
	testConfig(t, chainspec.HoodiChainID)
}

// TestCustomConfigMinimalPreset verifies that CustomConfig() correctly loads
// a minimal-preset YAML config with GLOAS parameters. This simulates what
// epbs-devnet-1 will use: SLOTS_PER_EPOCH=8, GLOAS_FORK_EPOCH=1, etc.
func TestCustomConfigMinimalPreset(t *testing.T) {
	// Write a minimal-preset config YAML to a temp file.
	yamlContent := `
PRESET_BASE: minimal
CONFIG_NAME: epbs-devnet-1

# Minimal preset overrides
SLOTS_PER_EPOCH: 8
EPOCHS_PER_HISTORICAL_VECTOR: 64
SLOTS_PER_HISTORICAL_ROOT: 64
EPOCHS_PER_SLASHINGS_VECTOR: 64
SECONDS_PER_SLOT: 6
MIN_SEED_LOOKAHEAD: 1
MAX_SEED_LOOKAHEAD: 4
GENESIS_DELAY: 20

# Fork schedule — GLOAS at epoch 1
PHASE0_FORK_VERSION: 0x10000038
GENESIS_FORK_VERSION: 0x10000038
ALTAIR_FORK_VERSION: 0x20000038
ALTAIR_FORK_EPOCH: 0
BELLATRIX_FORK_VERSION: 0x30000038
BELLATRIX_FORK_EPOCH: 0
CAPELLA_FORK_VERSION: 0x40000038
CAPELLA_FORK_EPOCH: 0
DENEB_FORK_VERSION: 0x50000038
DENEB_FORK_EPOCH: 0
ELECTRA_FORK_VERSION: 0x60000038
ELECTRA_FORK_EPOCH: 0
FULU_FORK_VERSION: 0x70000038
FULU_FORK_EPOCH: 0
GLOAS_FORK_VERSION: 0x80000038
GLOAS_FORK_EPOCH: 1

# GLOAS-specific
PAYLOAD_DUE_BPS: 7500
MAX_PAYLOAD_ATTESTATIONS: 4
BUILDER_REGISTRY_LIMIT: 1099511627776
BUILDER_PENDING_WITHDRAWALS_LIMIT: 1048576
MAX_BUILDERS_PER_WITHDRAWALS_SWEEP: 16384
MIN_BUILDER_WITHDRAWABILITY_DELAY: 8192
`
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(yamlContent), 0644))

	beaconCfg, _, err := CustomConfig(configPath)
	require.NoError(t, err)

	// Verify minimal preset values were loaded.
	require.Equal(t, "minimal", beaconCfg.PresetBase)
	require.Equal(t, uint64(8), beaconCfg.SlotsPerEpoch)
	require.Equal(t, uint64(64), beaconCfg.EpochsPerHistoricalVector)
	require.Equal(t, uint64(64), beaconCfg.SlotsPerHistoricalRoot)
	require.Equal(t, uint64(64), beaconCfg.EpochsPerSlashingsVector)
	require.Equal(t, uint64(6), beaconCfg.SecondsPerSlot)
	require.Equal(t, uint64(20), beaconCfg.GenesisDelay)

	// Verify GLOAS fork is at epoch 1.
	require.Equal(t, uint64(1), beaconCfg.GloasForkEpoch)
	require.NotEqual(t, uint64(math.MaxUint64), beaconCfg.GloasForkEpoch)

	// Verify GLOAS-specific parameters.
	require.Equal(t, uint64(7500), beaconCfg.PayloadDueBps)
	require.Equal(t, uint64(4), beaconCfg.MaxPayloadAttestations)
	require.Equal(t, uint64(1099511627776), beaconCfg.BuilderRegistryLimit)
	require.Equal(t, uint64(8192), beaconCfg.MinBuilderWithdrawabilityDelay)

	// Verify MinSeedLookahead is 1 (inherited from mainnet defaults or overridden).
	require.Equal(t, uint64(1), beaconCfg.MinSeedLookahead)

	// Verify ptc_window size calculation:
	// (2 + MIN_SEED_LOOKAHEAD) * SLOTS_PER_EPOCH = (2 + 1) * 8 = 24
	expectedPtcWindowSlots := (2 + beaconCfg.MinSeedLookahead) * beaconCfg.SlotsPerEpoch
	require.Equal(t, uint64(24), expectedPtcWindowSlots)

	// Verify fork schedule was initialized properly.
	// Epoch 0 should be Fulu (last pre-GLOAS fork), epoch 1+ should be GLOAS.
	require.Equal(t, FuluVersion, beaconCfg.GetCurrentStateVersion(0))
	require.Equal(t, GloasVersion, beaconCfg.GetCurrentStateVersion(1))
	require.Equal(t, GloasVersion, beaconCfg.GetCurrentStateVersion(100))
}

func TestMaxBlobsPerBlockUpperBound(t *testing.T) {
	// The max is taken across the base fields and every BlobSchedule entry, not just the
	// last (highest-epoch) one — here the peak (48) sits in the middle of the schedule.
	cfg := &BeaconChainConfig{
		MaxBlobsPerBlock:        6,
		MaxBlobsPerBlockElectra: 9,
		BlobSchedule: []BlobParameters{
			{Epoch: 100, MaxBlobsPerBlock: 12},
			{Epoch: 200, MaxBlobsPerBlock: 48},
			{Epoch: 300, MaxBlobsPerBlock: 24},
		},
	}
	require.EqualValues(t, 48, cfg.MaxBlobsPerBlockUpperBound())

	// With no schedule it falls back to the larger of the base limits.
	noSchedule := &BeaconChainConfig{MaxBlobsPerBlock: 6, MaxBlobsPerBlockElectra: 9}
	require.EqualValues(t, 9, noSchedule.MaxBlobsPerBlockUpperBound())
}
