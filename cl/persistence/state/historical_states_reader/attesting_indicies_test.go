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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

func TestReadHistoricalBlockRootAtGenesisSlot(t *testing.T) {
	expected := common.HexToHash("0x1234")
	getter := mockGetValFn(map[string]map[string][]byte{
		kv.BlockRoot: {string(base_encoding.Encode64ToBytes4(0)): expected[:]},
	})
	genesisState := state.New(&clparams.MainnetBeaconConfig)
	reader := NewHistoricalStatesReader(&clparams.MainnetBeaconConfig, nil, nil, genesisState, nil, nil)

	got, err := reader.readHistoricalBlockRoot(getter, 5, 0)
	require.NoError(t, err)
	require.Equal(t, expected, got)
}

func TestGloasSameSlotAttestationRejectsNonzeroIndex(t *testing.T) {
	root := common.HexToHash("0x1234")
	getter := mockGetValFn(map[string]map[string][]byte{
		kv.BlockRoot: {string(base_encoding.Encode64ToBytes4(0)): root[:]},
	})
	genesisState := state.New(&clparams.MainnetBeaconConfig)
	reader := NewHistoricalStatesReader(&clparams.MainnetBeaconConfig, nil, nil, genesisState, nil, nil)
	data := solid.AttestationData{
		Slot:            0,
		CommitteeIndex:  1,
		BeaconBlockRoot: root,
		Target:          solid.Checkpoint{Root: root},
	}

	_, err := reader.getAttestationParticipationFlagIndicies(getter, clparams.GloasVersion, 1, data, 1, false, true)
	require.ErrorContains(t, err, "committee index is not zero")
}

func TestGloasSameSlotAttestationAfterGenesisSlot(t *testing.T) {
	previousRoot := common.HexToHash("0x1234")
	headRoot := common.HexToHash("0x5678")
	getter := mockGetValFn(map[string]map[string][]byte{
		kv.BlockRoot: {
			string(base_encoding.Encode64ToBytes4(0)): previousRoot[:],
			string(base_encoding.Encode64ToBytes4(1)): headRoot[:],
		},
	})
	cfg := &clparams.MainnetBeaconConfig
	reader := NewHistoricalStatesReader(cfg, nil, nil, state.New(cfg), nil, nil)
	data := solid.AttestationData{
		Slot:            1,
		BeaconBlockRoot: headRoot,
		Target:          solid.Checkpoint{Root: previousRoot},
	}

	t.Run("rejects nonzero index", func(t *testing.T) {
		data := data
		data.CommitteeIndex = 1
		_, err := reader.getAttestationParticipationFlagIndicies(getter, clparams.GloasVersion, 2, data, 1, false, true)
		require.ErrorContains(t, err, "committee index is not zero")
	})

	t.Run("ignores parent payload availability", func(t *testing.T) {
		flags, err := reader.getAttestationParticipationFlagIndicies(getter, clparams.GloasVersion, 2, data, 1, true, true)
		require.NoError(t, err)
		require.Contains(t, flags, cfg.TimelyHeadFlagIndex)
	})
}

func TestPayloadMatchAppliesFromGloas(t *testing.T) {
	root := common.HexToHash("0x1234")
	getter := mockGetValFn(map[string]map[string][]byte{
		kv.BlockRoot: {
			string(base_encoding.Encode64ToBytes4(0)): root[:],
			string(base_encoding.Encode64ToBytes4(1)): root[:],
		},
	})
	cfg := &clparams.MainnetBeaconConfig
	reader := NewHistoricalStatesReader(cfg, nil, nil, state.New(cfg), nil, nil)
	data := solid.AttestationData{
		Slot:            1,
		BeaconBlockRoot: root,
		Target:          solid.Checkpoint{Root: root},
	}

	for _, tc := range []struct {
		version    clparams.StateVersion
		timelyHead bool
	}{
		{clparams.FuluVersion, true},
		{clparams.GloasVersion, false},
	} {
		t.Run(tc.version.String(), func(t *testing.T) {
			flags, err := reader.getAttestationParticipationFlagIndicies(getter, tc.version, 2, data, 1, true, true)
			require.NoError(t, err)
			require.Equal(t, tc.timelyHead, slices.Contains(flags, cfg.TimelyHeadFlagIndex))
		})
	}
}
