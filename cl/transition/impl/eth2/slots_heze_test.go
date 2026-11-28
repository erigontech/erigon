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

package eth2_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

func fuluStateForForkBoundary(t *testing.T, cfg *clparams.BeaconChainConfig) *state.CachingBeaconState {
	t.Helper()
	s := state.New(cfg)
	require.NoError(t, utils.DecodeSSZSnappy(s, capellaState, int(clparams.CapellaVersion)))
	require.NoError(t, s.UpgradeToDeneb())
	require.NoError(t, s.UpgradeToElectra())
	require.NoError(t, s.UpgradeToFulu())
	s.SetDepositRequestsStartIndex(s.Eth1DepositIndex())
	return s
}

func requireHezeAt(t *testing.T, s *state.CachingBeaconState, slot uint64) {
	t.Helper()
	require.Equal(t, slot, s.Slot())
	require.Equal(t, clparams.HezeVersion, s.Version())
	require.Equal(t, common.Bytes4(utils.Uint32ToBytes4(uint32(s.BeaconConfig().HezeForkVersion))), s.Fork().CurrentVersion)
	require.NotNil(t, s.GetLatestExecutionPayloadBid().InclusionListBits)
}

func TestProcessSlotsUpgradesToHezeAfterGloasEpoch(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := fuluStateForForkBoundary(t, &cfg)
	gloasEpoch := state.Epoch(s.BeaconState)
	cfg.GloasForkEpoch = gloasEpoch
	cfg.HezeForkEpoch = gloasEpoch + 1
	require.NoError(t, s.UpgradeToGloas())

	slot := cfg.HezeForkEpoch * cfg.SlotsPerEpoch
	require.NoError(t, transition.ValidatingMachine.ProcessSlots(s, slot))
	requireHezeAt(t, s, slot)
}

func TestProcessSlotsUpgradesThroughGloasToHezeInOneSlot(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := fuluStateForForkBoundary(t, &cfg)
	forkEpoch := state.Epoch(s.BeaconState) + 1
	cfg.GloasForkEpoch = forkEpoch
	cfg.HezeForkEpoch = forkEpoch

	slot := forkEpoch * cfg.SlotsPerEpoch
	require.NoError(t, transition.ValidatingMachine.ProcessSlots(s, slot))
	requireHezeAt(t, s, slot)
}
