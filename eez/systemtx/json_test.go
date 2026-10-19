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
package systemtx_test

import (
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/ethutils"
	"github.com/erigontech/erigon/rpc/ethapi"
)

func jsonFields(t *testing.T, v any) map[string]any {
	t.Helper()
	enc, err := json.Marshal(v)
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(enc, &fields))
	return fields
}

// TestJSON follows rpc_serialization_has_native_execution_fields_and_zero_signature_placeholders
// in eez-rollup0 a270a06 crates/eez-primitives/src/tests.rs.
func TestJSON(t *testing.T) {
	tx := mustNew(t, protocol.SystemTx{ChainID: 1, To: protocol.EEZL2Address, Input: []byte{1, 2, 3, 4}})

	enc, err := json.Marshal(tx)
	require.NoError(t, err)
	fields := jsonFields(t, tx)
	require.Equal(t, map[string]any{
		"type":     "0x76",
		"hash":     tx.Hash().Hex(),
		"chainId":  "0x1",
		"nonce":    "0x0",
		"to":       "0x4200000000000000000000000000000000000007",
		"value":    "0x0",
		"input":    "0x01020304",
		"gas":      "0x1e8480",
		"gasPrice": "0x0",
		"v":        "0x0",
		"r":        "0x0",
		"s":        "0x0",
	}, fields)

	dec, err := types.UnmarshalTransactionFromJSON(enc)
	require.NoError(t, err)
	require.IsType(t, &systemtx.Transaction{}, dec)
	require.Equal(t, tx.Body(), dec.(*systemtx.Transaction).Body())
	require.Equal(t, tx.Hash(), dec.Hash())

	for _, bad := range []string{
		`{"type":"0x76","chainId":"0x1","nonce":"0x0","to":"0x4200000000000000000000000000000000000007","value":"0x0"}`,
		`{"type":"0x76","chainId":"0x1","nonce":"0x0","to":"0x4200000000000000000000000000000000000008","value":"0x0","input":"0x"}`,
	} {
		_, err := types.UnmarshalTransactionFromJSON([]byte(bad))
		require.ErrorIs(t, err, protocol.ErrSystemTx)
	}
}

func TestRPCTransaction(t *testing.T) {
	tx := mustNew(t, protocol.SystemTx{ChainID: 1, Nonce: 2, To: protocol.EEZL2Address, Value: *uint256.NewInt(3), Input: []byte{1}})
	fields := jsonFields(t, ethapi.NewRPCTransaction(tx, common.Hash{1}, 10, 5, 0, uint256.NewInt(100)))

	require.Equal(t, "0x76", fields["type"])
	require.Equal(t, tx.Hash().Hex(), fields["hash"])
	require.Equal(t, "0x1e8480", fields["gas"])
	require.Equal(t, "0x0", fields["gasPrice"])
	require.Equal(t, "0x2", fields["nonce"])
	require.Equal(t, "0x3", fields["value"])
	require.Equal(t, "0x1", fields["chainId"])
	require.Equal(t, protocol.SystemAddress.Hex(), common.HexToAddress(fields["from"].(string)).Hex())
	for _, f := range []string{"v", "r", "s"} {
		require.Equal(t, "0x0", fields[f], f)
	}
}

func TestRPCReceipt(t *testing.T) {
	tx := mustNew(t, protocol.SystemTx{ChainID: 1, To: protocol.EEZL2Address})
	header := &types.Header{Number: *uint256.NewInt(5), BaseFee: uint256.NewInt(100)}
	receipt := &types.Receipt{
		Type:              protocol.SystemTxType,
		Status:            types.ReceiptStatusSuccessful,
		CumulativeGasUsed: 50_000,
		GasUsed:           50_000,
		TxHash:            tx.Hash(),
		BlockNumber:       uint256.NewInt(5),
	}
	fields := jsonFields(t, ethutils.MarshalReceipt(receipt, tx, chain.TestChainOsakaConfig, header, true, false))

	require.Equal(t, "0x76", fields["type"])
	require.Equal(t, "0x0", fields["effectiveGasPrice"])
	require.Equal(t, protocol.SystemAddress.Hex(), common.HexToAddress(fields["from"].(string)).Hex())
	require.Equal(t, protocol.EEZL2Address.Hex(), common.HexToAddress(fields["to"].(string)).Hex())
}
