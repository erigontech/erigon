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

package ethapi

import (
	"encoding/json"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestFrameRPCTransaction(t *testing.T) {
	target := common.HexToAddress("0x1234")
	tx := &types.FrameTransaction{
		NonceKeys: []uint256.Int{{}}, ChainID: *uint256.NewInt(1), SenderAddress: common.Address{1},
		Frames: []types.Frame{
			{Mode: types.FrameVerify, Flags: 3, Limits: mdgas.MdGas{Execution: 100, State: 200}},
			{Mode: types.FrameSender, Target: &target, Limits: mdgas.MdGas{Execution: 300, State: 400}, Value: *uint256.NewInt(5), Data: []byte{0xab}},
		},
		Signatures: []types.FrameSignature{{Signature: []byte{1}}},
	}
	result := NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)
	data, err := json.Marshal(result)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &fields))
	require.JSONEq(t, `["0x0"]`, string(fields["nonceKeys"]))
	require.JSONEq(t, `[
		{"mode":"0x1","flags":"0x3","target":null,"executionGas":"0x64","stateGas":"0xc8","value":"0x0","data":"0x"},
		{"mode":"0x2","flags":"0x0","target":"0x0000000000000000000000000000000000001234","executionGas":"0x12c","stateGas":"0x190","value":"0x5","data":"0xab"}
	]`, string(fields["frames"]))
	require.NotContains(t, fields, "sender")
	require.JSONEq(t, `"0x0100000000000000000000000000000000000000"`, string(fields["from"]))
	require.Contains(t, fields, "signatures")
	require.JSONEq(t, `"0x0"`, string(fields["maxFeePerBlobGas"]))
	require.Equal(t, string(data), fastJSON(t, result))
	var decoded RPCTransaction
	require.NoError(t, json.Unmarshal(data, &decoded))
	roundTrip, err := json.Marshal(&decoded)
	require.NoError(t, err)
	require.JSONEq(t, string(data), string(roundTrip))
}
