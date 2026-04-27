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

package raw

import (
	_ "embed"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestGetters(t *testing.T) {
	state := GetTestState()
	require.NotNil(t, state.BeaconConfig())
	valLength := state.ValidatorLength()
	require.Equal(t, state.balances.Length(), valLength)

	val, err := state.ValidatorBalance(0)
	require.NoError(t, err)
	require.Equal(t, uint64(0x3d5972c17), val)

	root, err := state.BlockRoot()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), common.HexToHash("0xfc09c7ba749aa6d19ff97e8199c768dcb626df924060806b763ca2e7c8385805"))

	copied, err := state.Copy()
	require.NoError(t, err)

	root, err = copied.BlockRoot()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), common.HexToHash("0xfc09c7ba749aa6d19ff97e8199c768dcb626df924060806b763ca2e7c8385805"))
}

// TestNewBeaconStateMinimalPreset verifies that creating a BeaconState with
// minimal-preset config (SLOTS_PER_EPOCH=8) does not panic and produces
// correctly-sized data structures, especially the GLOAS ptc_window.
func TestNewBeaconStateMinimalPreset(t *testing.T) {
	// Start from mainnet defaults and override to minimal preset values.
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	cfg.GenesisDelay = 20
	cfg.GloasForkEpoch = 1
	cfg.GloasForkVersion = 0x80000038
	cfg.InitializeForkSchedule()

	// This must not panic — it exercises all vector/list allocations.
	state := New(&cfg)
	require.NotNil(t, state)

	// Verify ptc_window dimensions.
	// Expected: (2 + MIN_SEED_LOOKAHEAD) * SLOTS_PER_EPOCH = (2 + 1) * 8 = 24 slots
	ptcWindow := state.GetPtcWindow()
	require.NotNil(t, ptcWindow)
	require.Equal(t, 24, ptcWindow.Length(), "ptc_window should have 24 slots under minimal preset")

	// Each slot in ptc_window should be a vector of PTC_SIZE (2 for minimal preset).
	for i := 0; i < ptcWindow.Length(); i++ {
		vec := ptcWindow.Get(i)
		require.Equal(t, int(cfg.PtcSize), vec.Length(),
			"ptc_window[%d] should have PTC_SIZE=%d entries", i, cfg.PtcSize)
	}

	// Verify other minimal-preset-sensitive structures.
	// blockRoots and stateRoots should have SlotsPerHistoricalRoot=64 entries.
	require.Equal(t, 64, state.blockRoots.Length(), "blockRoots length")
	require.Equal(t, 64, state.stateRoots.Length(), "stateRoots length")

	// slashings should use EpochsPerSlashingsVector from config, not hardcoded 8192.
	require.Equal(t, 64, state.slashings.Length(),
		"slashings length should match EpochsPerSlashingsVector=64 (minimal preset)")

	// randaoMixes should have EpochsPerHistoricalVector=64 entries.
	require.Equal(t, 64, state.randaoMixes.Length(), "randaoMixes length")

	// executionPayloadAvailability should be a BitVector of SlotsPerHistoricalRoot=64.
	epAvail := state.GetExecutionPayloadAvailability()
	require.NotNil(t, epAvail)

	// builderPendingPayments should have 2 * SlotsPerEpoch = 16 entries.
	require.Equal(t, 16, state.builderPendingPayments.Cap(),
		"builderPendingPayments capacity should be 2*SlotsPerEpoch=16")

	// proposerLookahead should have (MIN_SEED_LOOKAHEAD + 1) * SLOTS_PER_EPOCH = 2 * 8 = 16.
	require.Equal(t, 16, state.proposerLookahead.Length(),
		"proposerLookahead should have 16 entries under minimal preset")
}

// TestNewBeaconStateMainnetPtcWindow verifies ptc_window dimensions for mainnet config.
func TestNewBeaconStateMainnetPtcWindow(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()

	state := New(&cfg)
	require.NotNil(t, state)

	// Mainnet: (2 + 1) * 32 = 96 slots
	ptcWindow := state.GetPtcWindow()
	require.Equal(t, 96, ptcWindow.Length(), "ptc_window should have 96 slots under mainnet")
}
