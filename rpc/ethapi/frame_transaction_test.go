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
	"github.com/erigontech/erigon/execution/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestFrameRPCTransaction(t *testing.T) {
	tx := &types.FrameTransaction{NonceKeys: []uint256.Int{{}}, ChainID: *uint256.NewInt(1), SenderAddress: common.Address{1}, Frames: []types.Frame{{Mode: 1, Flags: 3}}, Signatures: []types.FrameSignature{{Signature: []byte{1}}}}
	result := NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)
	data, err := json.Marshal(result)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &fields))
	require.JSONEq(t, `["0x0"]`, string(fields["nonceKeys"]))
	require.Contains(t, fields, "frames")
	require.Contains(t, fields, "signatures")
	require.JSONEq(t, `"0x0"`, string(fields["maxFeePerBlobGas"]))
}
