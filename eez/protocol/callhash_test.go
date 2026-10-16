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

package protocol

import (
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

// TestCallHashSolidityVectors checks the hashes eez-rollup0 a270a06
// contracts/test/CallHashVectors.t.sol asserts for EEZ and EEZL2
// computeCrossChainCallHash at core-protocol 855fe06.
func TestCallHashSolidityVectors(t *testing.T) {
	source, target := common.HexToAddress("0xbb"), common.HexToAddress("0xaa")
	data := common.FromHex("0x010203")
	oneEther := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	for _, c := range []struct {
		in   CallHashInput
		want string
	}{
		{
			in:   CallHashInput{false, source, 7, target, 1, new(big.Int), 0, data},
			want: "0x16b1575ff5a4ec44167aebf047dd46f77db3766f7481445ad09c8136bff735a8",
		},
		{
			in:   CallHashInput{true, source, 7, target, 1, new(big.Int), 0, data},
			want: "0x4cf0f2738ced4dcd497cf8a081030f41c5dc588fbdcac75f3a217e979d19abe7",
		},
		{
			in:   CallHashInput{false, source, 1, target, 7, oneEther, 0, data},
			want: "0x9fd05cd7eebaf1d08b2961cb5d1237ef586cea58141270697a5509c6f3a03a37",
		},
		{
			in:   CallHashInput{false, source, 1, target, 7, oneEther, 123456, data},
			want: "0x25400cdd749a1c3ac82f4e3093f0460afe21e718a545a96f9399b9ae486c99e4",
		},
		{
			in:   CallHashInput{false, source, math.MaxUint64, target, math.MaxUint64 - 1, maxUint256, 0, nil},
			want: "0x414b9d6bf91a3e266bcd34ddd870a53332107a606b6eda618455f9f940291e2b",
		},
		{
			in:   CallHashInput{false, source, math.MaxUint64, target, math.MaxUint64 - 1, maxUint256, math.MaxUint64, nil},
			want: "0x7f04915c437db6536fe9d746b135ed834b391532e4be8beadd898ad1f592895f",
		},
	} {
		h, err := CrossChainCallHash(&c.in)
		require.NoError(t, err)
		require.Equal(t, common.HexToHash(c.want), h)
	}
}

func TestCallHashMatchesReference(t *testing.T) {
	g := newValueGen(6)
	for range 200 {
		in := CallHashInput{g.bool(), g.address(), g.r.Uint64(), g.address(), g.r.Uint64(), g.uint256(), g.r.Uint64(), g.bytes()}
		h, err := CrossChainCallHash(&in)
		require.NoError(t, err)
		want := crypto.Keccak256Hash(refEncode(refBool(in.IsStatic), refAddress(in.SourceAddress), refUint(in.SourceRollupID),
			refAddress(in.TargetAddress), refUint(in.TargetRollupID), refInt(in.Value), refUint(in.CallGas), refBytes(in.Data)))
		require.Equal(t, want, h)
	}
}

func TestCallHashRejectsInvalidValue(t *testing.T) {
	_, err := CrossChainCallHash(&CallHashInput{})
	require.ErrorIs(t, err, ErrNilInteger)
	_, err = CrossChainCallHash(&CallHashInput{Value: big.NewInt(-1)})
	require.ErrorIs(t, err, ErrIntegerRange)
}
