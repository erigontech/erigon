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

func TestChiadoUsesBootnodesAndStaticPeers(t *testing.T) {
	network, _ := GetConfigsByNetwork(chainspec.ChiadoChainID)

	require.NotEmpty(t, network.BootNodes)
	require.Equal(t, []string{
		"/ip4/65.109.93.224/tcp/9000/p2p/16Uiu2HAmEG2vHsiGdask9Weg5qVCsxtrezWCde1WArakqSNCY1EA",
		"/ip4/185.127.230.20/tcp/9000/p2p/16Uiu2HAkzVyapm35N6PFLVVgPvfaR4PngLkzH1rQznHTNQk73hwt",
		"/ip4/51.68.224.153/tcp/9000/p2p/16Uiu2HAkxcBE3LK7zhnyZERguonkKmXLgYPRcuDPaF6C2vaigYuT",
		"/ip4/57.128.194.213/tcp/9000/p2p/16Uiu2HAmH8EQ7XHrz72cEspr6G7xHipCgV55gf211t6B9AnbRcgo",
		"/ip4/23.92.177.94/tcp/29410/p2p/16Uiu2HAm2XtDtp1FvSSorMp6A7qStBSLxHvdSQewtWm8VpLA5yke",
		"/ip4/134.65.192.121/tcp/19000/p2p/16Uiu2HAm1TwYCeKTdwYayAr8Vru7Ku9HKHH7MiWS6G5QuVsVqUy2",
		"/ip4/103.219.170.121/tcp/12000/p2p/16Uiu2HAmN8DWDZprSvsM3ZTYDm5FmPsaDAYs4DmvVtzSwBnetrNG",
		"/ip4/40.160.27.251/tcp/9001/p2p/16Uiu2HAmA6dX87khYTKGownJmmCjp9chuTJyZ5T3bqh15bX8q5as",
	}, network.StaticPeers)
}

func TestCaplinConfigCanDisableDefaultStaticPeers(t *testing.T) {
	network := NetworkConfigs[chainspec.ChiadoChainID]

	CaplinConfig{}.ApplyNetworkOverrides(&network)
	require.Equal(t, ChiadoStaticPeers, network.StaticPeers)

	CaplinConfig{StaticPeers: []string{}}.ApplyNetworkOverrides(&network)
	require.Empty(t, network.StaticPeers)
	require.NotEmpty(t, network.BootNodes)

	CaplinConfig{StaticPeers: []string{"replacement"}}.ApplyNetworkOverrides(&network)
	require.Equal(t, []string{"replacement"}, network.StaticPeers)
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
	require.NoError(t, os.WriteFile(configPath, []byte(yamlContent), 0o644))

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

func TestCustomConfigRejectsGloasRequestTypeMismatch(t *testing.T) {
	yamlContent := `
PRESET_BASE: minimal
GLOAS_FORK_EPOCH: 1
BUILDER_DEPOSIT_REQUEST_TYPE: 0x09
`
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(yamlContent), 0o644))

	_, _, err := CustomConfig(configPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "BUILDER_DEPOSIT_REQUEST_TYPE mismatch")
}

func TestCustomConfigRejectsElectraRequestTypeMismatchWithoutGloas(t *testing.T) {
	yamlContent := `
PRESET_BASE: minimal
ELECTRA_FORK_EPOCH: 1
DEPOSIT_REQUEST_TYPE: 0x09
`
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(yamlContent), 0o644))

	_, _, err := CustomConfig(configPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "DEPOSIT_REQUEST_TYPE mismatch")
}

func TestCustomConfigRejectsBaseRequestTypeMismatchWhenOnlyGloasScheduled(t *testing.T) {
	yamlContent := `
PRESET_BASE: minimal
GLOAS_FORK_EPOCH: 1
DEPOSIT_REQUEST_TYPE: 0x09
`
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(yamlContent), 0o644))

	_, _, err := CustomConfig(configPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "DEPOSIT_REQUEST_TYPE mismatch")
}

// TestCustomConfigUnsetForksAreFarFuture verifies that fork epochs omitted from a
// custom config default to far-future, like other clients, rather than inheriting
// the finite epochs of the mainnet base config.
func TestCustomConfigUnsetForksAreFarFuture(t *testing.T) {
	yamlContent := `
PRESET_BASE: minimal
ALTAIR_FORK_EPOCH: 0
BELLATRIX_FORK_EPOCH: 0
CAPELLA_FORK_EPOCH: 0
DENEB_FORK_EPOCH: 0
ELECTRA_FORK_EPOCH: 100000000
`
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(yamlContent), 0o644))

	beaconCfg, _, err := CustomConfig(configPath)
	require.NoError(t, err)

	// Explicitly-set forks are preserved.
	require.Equal(t, uint64(0), beaconCfg.DenebForkEpoch)
	require.Equal(t, uint64(100000000), beaconCfg.ElectraForkEpoch)
	// Omitted forks are far-future, not the inherited mainnet epoch (Fulu=411392).
	require.Equal(t, uint64(math.MaxUint64), beaconCfg.FuluForkEpoch)
	require.Equal(t, uint64(math.MaxUint64), beaconCfg.GloasForkEpoch)
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

func TestForkSchemaMatchesSlot(t *testing.T) {
	cfg := MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 1
	cfg.GloasForkEpoch = 2
	cfg.InitializeForkSchedule()
	spe := cfg.SlotsPerEpoch

	for _, tc := range []struct {
		name           string
		slot           uint64
		decodedVersion StateVersion
		want           bool
	}{
		// Schemas diverge only across the Gloas boundary, so a disagreement
		// below it is not a mismatch.
		{"both pre-Gloas", spe, DenebVersion, true},
		{"both Gloas", 2 * spe, GloasVersion, true},
		{"Gloas schema at a pre-Gloas slot", spe, GloasVersion, false},
		{"pre-Gloas schema at a Gloas slot", 2 * spe, FuluVersion, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, cfg.ForkSchemaMatchesSlot(tc.slot, tc.decodedVersion))
		})
	}
}

// Mainnet keeps Gloas at FAR_FUTURE_EPOCH, so no slot maps to it and every
// Gloas-schema object is inconsistent whatever slot it claims.
func TestForkSchemaMatchesSlotFarFutureGloas(t *testing.T) {
	cfg := MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	require.Equal(t, uint64(math.MaxUint64), cfg.GloasForkEpoch)

	fuluSlot := cfg.FuluForkEpoch * cfg.SlotsPerEpoch
	require.True(t, cfg.ForkSchemaMatchesSlot(fuluSlot, FuluVersion))
	require.False(t, cfg.ForkSchemaMatchesSlot(fuluSlot, GloasVersion))
	require.False(t, cfg.ForkSchemaMatchesSlot(math.MaxUint64/cfg.SlotsPerEpoch, GloasVersion))
}
