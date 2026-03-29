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

package utils_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

func TestSSZSnappy(t *testing.T) {
	verySussyMessage := &cltypes.Metadata{
		SeqNumber: 69,          // :D
		Attnets:   [8]byte{96}, // :(
	}
	sussyEncoded, err := utils.EncodeSSZSnappy(verySussyMessage)
	require.NoError(t, err)
	sussyDecoded := &cltypes.Metadata{}
	require.NoError(t, utils.DecodeSSZSnappy(sussyDecoded, sussyEncoded, 0))
	require.Equal(t, verySussyMessage.SeqNumber, sussyDecoded.SeqNumber)
	require.Equal(t, verySussyMessage.Attnets, sussyDecoded.Attnets)
}

func TestPlainSnappy(t *testing.T) {
	msg := common.Hex2Bytes("10103849358111387348383738784374783811111754097864786873478675489485765483936576486387645456876772090909090ff")
	sussyEncoded := utils.CompressSnappy(msg)
	sussyDecoded, err := utils.DecompressSnappy(sussyEncoded, false)
	require.NoError(t, err)
	require.Equal(t, msg, sussyDecoded)
}

func TestLiteralConverters(t *testing.T) {
	require.Equal(t, [4]byte{0x0, 0x0, 0x2, 0x58}, utils.Uint32ToBytes4(600))
	require.Equal(t, [4]byte{10, 23, 56, 7}, utils.BytesToBytes4([]byte{10, 23, 56, 7, 8, 5}))
	require.Equal(t, []byte{0x58, 0x2, 0x0, 0x0, 0x0, 0x0, 0x00, 0x00}, utils.Uint64ToLE(600))
}

func TestReverseBytes(t *testing.T) {
	input := common.HexToHash("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	reversed := common.HexToHash("1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100")
	utils.ReverseBytes(&input)
	require.Equal(t, reversed, input)
}
