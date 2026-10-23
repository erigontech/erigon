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

package ethutils

import (
	"encoding/json"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestFrameRPCReceipt(t *testing.T) {
	payer := common.Address{1}
	tx := &types.FrameTransaction{NonceKeys: []uint256.Int{{}}, SenderAddress: payer, Frames: []types.Frame{{}}}
	receipt := &types.Receipt{Type: types.FrameTxType, Payer: &payer, BlockNumber: uint256.NewInt(1), FrameReceipts: []types.FrameReceipt{{Status: 1, Logs: types.Logs{}}}}
	result := MarshalReceipt(receipt, tx, chain.AllProtocolChanges, &types.Header{Number: *uint256.NewInt(1), BaseFee: uint256.NewInt(0)}, true, false)
	data, err := json.Marshal(result)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &fields))
	require.Contains(t, fields, "payer")
	require.Contains(t, fields, "frameReceipts")
}
