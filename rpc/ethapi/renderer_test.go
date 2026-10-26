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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

const taggedTxType = 0x7d

// taggedTx is a dynamic-fee transaction under a registered type id.
type taggedTx struct{ *types.DynamicFeeTransaction }

func (taggedTx) Type() byte { return taggedTxType }

func renderedKeys(t *testing.T, txn types.Transaction) map[string]any {
	t.Helper()
	b, err := json.Marshal(NewRPCTransaction(txn, common.Hash{1}, 0, 1, 0, uint256.NewInt(1)))
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(b, &fields))
	return fields
}

func TestRenderer(t *testing.T) {
	require.Panics(t, func() { RegisterTxRenderer(taggedTxType, func(types.Transaction, *RPCTransaction) {}) }, "unregistered type")
	types.RegisterTxType(taggedTxType, types.TxTypeSpec{New: func() types.Transaction { return taggedTx{new(types.DynamicFeeTransaction)} }})
	t.Cleanup(func() { types.UnregisterTxTypeForTesting(taggedTxType) })

	to := common.Address{2}
	inner := &types.DynamicFeeTransaction{CommonTx: types.CommonTx{To: &to}, ChainID: *uint256.NewInt(1)}
	require.Contains(t, renderedKeys(t, taggedTx{inner}), "yParity")

	RegisterTxRenderer(taggedTxType, func(_ types.Transaction, r *RPCTransaction) { r.YParity, r.MaxFeePerGas = nil, nil })
	t.Cleanup(func() { delete(txRenderers, taggedTxType) })
	fields := renderedKeys(t, taggedTx{inner})
	require.NotContains(t, fields, "yParity")
	require.NotContains(t, fields, "maxFeePerGas")
	require.Equal(t, "0x7d", fields["type"])
	require.Contains(t, renderedKeys(t, inner), "yParity", "Ethereum types render as before")
}
