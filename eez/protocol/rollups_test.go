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
	"github.com/erigontech/erigon/common/hexutil"
)

// Known answers from Foundry's cast: `cast calldata "rollups(uint64)" <id>`
// and `cast abi-encode "f(address,bytes32,uint256)" <record>`.

func TestEncodeRollups(t *testing.T) {
	require.Equal(t, [4]byte{0xef, 0x67, 0x8d, 0x27}, RollupsSelector)
	require.Equal(t, "0xef678d270000000000000000000000000000000000000000000000000000000000000007", hexutil.Encode(EncodeRollups(7)))
	require.Equal(t, "0xef678d27000000000000000000000000000000000000000000000000ffffffffffffffff", hexutil.Encode(EncodeRollups(math.MaxUint64)))
}

func TestDecodeRollupsReturn(t *testing.T) {
	out := hexutil.MustDecode("0x0000000000000000000000005fbdb2315678afecb367f032d93f642f64180aa3" +
		"1111111111111111111111111111111111111111111111111111111111111111" +
		"0000000000000000000000000000000000000000000000000de0b6b3a7640000")
	want := &RollupConfig{
		RollupContract: common.HexToAddress("0x5FbDB2315678afecb367f032d93F642f64180aa3"),
		Root:           common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
		EtherBalance:   big.NewInt(1_000_000_000_000_000_000),
	}
	got, err := DecodeRollupsReturn(out)
	require.NoError(t, err)
	require.Equal(t, want, got)

	// As Solidity's decoder, bytes past the record are ignored.
	got, err = DecodeRollupsReturn(append(out, 0xff))
	require.NoError(t, err)
	require.Equal(t, want, got)

	for _, short := range [][]byte{nil, {}, out[:64], out[:95]} {
		_, err := DecodeRollupsReturn(short)
		require.Error(t, err, "%d bytes", len(short))
	}
}
