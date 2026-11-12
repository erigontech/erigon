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

package historical_states_reader

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

// The epoch 2 seed uses the final randao mix of epoch 0, which the antiquary stores at RandaoMixes[slot 0].
func TestReadRandaoMixBySlotAndIndex_Epoch2SeedMix(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	genesisState, err := initial_state.GetGenesisState(t.Context(), chainspec.MainnetChainID)
	require.NoError(t, err)

	epoch0Mix := common.HexToHash("0xaa")
	getter := func(table string, key []byte) ([]byte, error) {
		if table == kv.RandaoMixes && bytes.Equal(key, base_encoding.Encode64ToBytes4(0)) {
			return epoch0Mix[:], nil
		}
		return nil, nil
	}
	r := NewHistoricalStatesReader(cfg, nil, nil, genesisState, nil, nil)

	epoch := uint64(2)
	mixPosition := (epoch + cfg.EpochsPerHistoricalVector - cfg.MinSeedLookahead - 1) % cfg.EpochsPerHistoricalVector
	mix, err := r.ReadRandaoMixBySlotAndIndex(getter, epoch*cfg.SlotsPerEpoch, mixPosition)
	require.NoError(t, err)
	require.Equal(t, epoch0Mix, mix)
}

func TestReadRandaoMixBySlotAndIndex_GenesisFallbackUsesIndex(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	genesisState, err := initial_state.GetGenesisState(t.Context(), chainspec.MainnetChainID)
	require.NoError(t, err)

	const index = 5
	genesisMix := common.HexToHash("0xbb")
	require.NoError(t, genesisState.SetRandaoMixAt(index, genesisMix))
	getter := func(table string, key []byte) ([]byte, error) { return nil, nil }
	r := NewHistoricalStatesReader(cfg, nil, nil, genesisState, nil, nil)

	mix, err := r.ReadRandaoMixBySlotAndIndex(getter, cfg.SlotsPerEpoch, index)
	require.NoError(t, err)
	require.Equal(t, genesisMix, mix)
}

func TestReadRandaoMixBySlotAndIndex_WrappedIndex(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	genesisState, err := initial_state.GetGenesisState(t.Context(), chainspec.MainnetChainID)
	require.NoError(t, err)

	n := cfg.EpochsPerHistoricalVector
	storedMix := common.HexToHash("0xdd")
	getter := func(table string, key []byte) ([]byte, error) {
		if table == kv.RandaoMixes && bytes.Equal(key, base_encoding.Encode64ToBytes4((n-1)*cfg.SlotsPerEpoch)) {
			return storedMix[:], nil
		}
		return nil, nil
	}
	r := NewHistoricalStatesReader(cfg, nil, nil, genesisState, nil, nil)

	mix, err := r.ReadRandaoMixBySlotAndIndex(getter, (n+1)*cfg.SlotsPerEpoch, n-1)
	require.NoError(t, err)
	require.Equal(t, storedMix, mix)
}
