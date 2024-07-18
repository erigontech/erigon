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

package state

import (
	"bytes"
	_ "embed"
	"testing"

	libcommon "github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/stretchr/testify/require"
)

//go:embed tests/capella.ssz_snappy
var capellaBeaconSnappyTest []byte

//go:embed tests/phase0.ssz_snappy
var phase0BeaconSnappyTest []byte

func TestBeaconStateCapellaEncodingDecoding(t *testing.T) {
	state := New(&clparams.MainnetBeaconConfig)
	decodedSSZ, err := utils.DecompressSnappy(capellaBeaconSnappyTest)
	require.NoError(t, err)
	require.NoError(t, state.DecodeSSZ(decodedSSZ, int(clparams.CapellaVersion)))
	root, err := state.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, libcommon.Hash(root), libcommon.HexToHash("0xb3012b73c02ab66b2779d996f9d33d36e58bf71ffc8f3e12e07024606617a9c0"))

}

func TestBeaconStatePhase0EncodingDecoding(t *testing.T) {
	state := New(&clparams.MainnetBeaconConfig)
	decodedSSZ, err := utils.DecompressSnappy(phase0BeaconSnappyTest)
	require.NoError(t, err)
	state.DecodeSSZ(decodedSSZ, int(clparams.Phase0Version))
	root, err := state.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, libcommon.Hash(root), libcommon.HexToHash("0xf23b6266af40567516afeee250c1f8c06e9800f34a990a210604c380b506e053"))
	// Lets test the caches too
	var w bytes.Buffer
	require.NoError(t, state.EncodeCaches(&w))
	values1 := state.activeValidatorsCache.Values()
	keys1 := state.activeValidatorsCache.Keys()
	values2 := state.shuffledSetsCache.Values()
	keys2 := state.shuffledSetsCache.Keys()

	require.NoError(t, state.DecodeCaches(&w))
	require.Equal(t, values1, state.activeValidatorsCache.Values())
	require.Equal(t, keys1, state.activeValidatorsCache.Keys())
	require.Equal(t, values2, state.shuffledSetsCache.Values())
	require.Equal(t, keys2, state.shuffledSetsCache.Keys())
}
