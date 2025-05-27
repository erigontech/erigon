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

package ssz2_test

import (
	_ "embed"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/serialized.ssz_snappy
var beaconState []byte

func TestEncodeDecode(t *testing.T) {
	bs := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(bs, beaconState, int(clparams.CapellaVersion)))
	root, err := bs.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), common.HexToHash("0x36eb1bb5b4616f9d5046b2a719a8c4217f3dc40c1b7dff7abcc55c47f142a78b"))
	d, err := bs.EncodeSSZ(nil)
	require.NoError(t, err)
	dec, _ := utils.DecompressSnappy(beaconState, true)
	require.Equal(t, dec, d)
}
