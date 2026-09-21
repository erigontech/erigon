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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// MarshalReceipt must reuse a Bloom the receipt already carries instead of
// hashing the logs again on every call, and compute it only when unset.
func TestMarshalReceiptReusesReceiptBloom(t *testing.T) {
	var preset types.Bloom
	preset[0] = 0x80
	preset[types.BloomByteLength-1] = 0x01
	receipt := &types.Receipt{
		Status:            types.ReceiptStatusSuccessful,
		CumulativeGasUsed: 21_000,
		Bloom:             preset,
		Logs: []*types.Log{{
			Address: common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Topics:  []common.Hash{common.HexToHash("0x01")},
		}},
		BlockNumber:      uint256.NewInt(1),
		TransactionIndex: 0,
	}
	txn := &types.LegacyTx{
		CommonTx: types.CommonTx{GasLimit: 21_000},
		GasPrice: *uint256.NewInt(1),
	}
	header := &types.Header{Number: *uint256.NewInt(1)}
	config := chain.TestChainBerlinConfig

	fields := MarshalReceipt(receipt, txn, config, header, common.HexToHash("0xbeef"), false, false)
	assert.Equal(t, preset, *fields.LogsBloom)

	receipt.Bloom = types.Bloom{}
	fields = MarshalReceipt(receipt, txn, config, header, common.HexToHash("0xbeef"), false, false)
	assert.Equal(t, types.CreateBloom(types.Receipts{receipt}), *fields.LogsBloom)
}

// The fast marshaller has to produce the bytes encoding/json produced, field order and
// omitempty included, for both log shapes MarshalReceipt can put in Logs.
func TestRPCReceiptMarshalFastJSONTo(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	txn := dynamicFeeTx(&to)
	txn.SetSender(accounts.InternAddress(common.HexToAddress("0xabcdef0123456789abcdef0123456789abcdef03")))
	header := &types.Header{Number: *uint256.NewInt(7), Time: 1_750_000_000, BaseFee: uint256.NewInt(50)}

	for _, tc := range []struct {
		name               string
		logs               int
		withBlockTimestamp bool
		nilLogs            bool
	}{
		{"no logs", 0, false, false},
		{"two logs", 2, false, false},
		{"two logs with timestamp", 2, true, false},
		{"nil logs", 0, false, true},
		{"nil logs with timestamp", 0, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := make(types.Logs, tc.logs)
			for i := range logs {
				logs[i] = &types.Log{Address: to, Topics: []common.Hash{{0x01}, {0x02}}, Data: make([]byte, 64)}
			}
			if tc.nilLogs {
				logs = nil
			}
			receipt := &types.Receipt{
				Status:            types.ReceiptStatusSuccessful,
				CumulativeGasUsed: 42_000,
				Logs:              logs,
				TxHash:            common.HexToHash("0xbeef"),
				GasUsed:           21_000,
				BlockHash:         common.HexToHash("0xb10c"),
				BlockNumber:       uint256.NewInt(7),
				TransactionIndex:  3,
			}
			receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
			r := MarshalReceipt(receipt, &txn, chain.TestChainOsakaConfig, header, receipt.TxHash, true, tc.withBlockTimestamp)

			want, err := json.Marshal(r)
			require.NoError(t, err)

			s := jsonstream.Get(nil)
			defer jsonstream.Put(s)
			require.NoError(t, r.MarshalFastJSONTo(s))
			require.NoError(t, s.Flush())

			require.Equal(t, string(want), string(s.Buffer()))
		})
	}
}

// encoding/json writes null for a typed nil slice, not []. MarshalReceipt normalizes nil
// receipt logs to an empty slice, so only a direct construction reaches these branches.
func TestRPCReceiptMarshalFastJSONToTypedNilLogs(t *testing.T) {
	for name, logs := range map[string]any{
		"nil types.Logs": types.Logs(nil),
		"nil []*Log":     []*types.Log(nil),
		"nil []*RPCLog":  []*types.RPCLog(nil),
		"nil []map":      []map[string]any(nil),
		"untyped nil":    nil,
	} {
		t.Run(name, func(t *testing.T) {
			r := &RPCReceipt{Logs: logs}
			want, err := json.Marshal(r)
			require.NoError(t, err)

			s := jsonstream.Get(nil)
			defer jsonstream.Put(s)
			require.NoError(t, r.MarshalFastJSONTo(s))
			require.NoError(t, s.Flush())
			require.Equal(t, string(want), string(s.Buffer()))
		})
	}
}

// eth_sendRawTransactionSync answers with MarshalSubscribeReceipt, whose logs are maps.
func TestRPCReceiptMarshalFastJSONToSubscribeLogs(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	r := &RPCReceipt{
		TransactionHash: common.HexToHash("0xbeef"),
		Logs: []map[string]any{
			{
				"address":         to,
				"topics":          []common.Hash{{0x01}, {0x02}},
				"data":            hexutil.Bytes{0xaa, 0xbb},
				"transactionHash": common.HexToHash("0xbeef"),
			},
			{ // address is only set when the proto log carried one
				"topics":          []common.Hash{},
				"data":            hexutil.Bytes{},
				"transactionHash": common.HexToHash("0xbeef"),
			},
			nil, // a nil map is null, not {}
			{},  // an empty one is {}
		},
	}
	want, err := json.Marshal(r)
	require.NoError(t, err)

	s := jsonstream.Get(nil)
	defer jsonstream.Put(s)
	require.NoError(t, r.MarshalFastJSONTo(s))
	require.NoError(t, s.Flush())
	require.Equal(t, string(want), string(s.Buffer()))
}

// The list is what eth_getBlockReceipts returns; the encoder only sees the top-level type.
func TestRPCReceiptsMarshalFastJSONTo(t *testing.T) {
	for name, rs := range map[string]RPCReceipts{
		"nil":         nil,
		"empty":       {},
		"two":         {{TransactionHash: common.HexToHash("0x1")}, {TransactionHash: common.HexToHash("0x2")}},
		"nil element": {nil},
	} {
		t.Run(name, func(t *testing.T) {
			want, err := json.Marshal(rs)
			require.NoError(t, err)

			s := jsonstream.Get(nil)
			defer jsonstream.Put(s)
			require.NoError(t, rs.MarshalFastJSONTo(s))
			require.NoError(t, s.Flush())
			require.Equal(t, string(want), string(s.Buffer()))
		})
	}
}

// A key MarshalSubscribeReceipt might add later must fail loudly rather than be dropped.
func TestRPCReceiptMarshalFastJSONToRejectsUnknownSubscribeKey(t *testing.T) {
	r := &RPCReceipt{Logs: []map[string]any{{
		"data":     hexutil.Bytes{0x01},
		"newField": "surprise",
	}}}
	s := jsonstream.Get(nil)
	defer jsonstream.Put(s)
	require.ErrorContains(t, r.MarshalFastJSONTo(s), "keys")
}

// The optional fields decide whether a key is written at all, and To and
// ContractAddress decide null vs a hex string.
func TestRPCReceiptMarshalFastJSONToOptionalFields(t *testing.T) {
	addr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	status := hexutil.Uint64(1)
	price := hexutil.U256(*uint256.NewInt(7))
	blobGas := hexutil.Uint64(131072)
	bloom := types.Bloom{}
	for _, tc := range []struct {
		name string
		r    RPCReceipt
	}{
		{"nil to and contract", RPCReceipt{}},
		{"contract creation", RPCReceipt{ContractAddress: &addr}},
		{"call", RPCReceipt{To: &addr}},
		{"bloom", RPCReceipt{LogsBloom: &bloom}},
		{"effective gas price", RPCReceipt{EffectiveGasPrice: &price}},
		{"status", RPCReceipt{Status: &status}},
		{"pre-byzantium root", RPCReceipt{Root: hexutil.Bytes{0x01, 0x02}}},
		{"blob fields", RPCReceipt{BlobGasPrice: &price, BlobGasUsed: &blobGas}},
		{"everything", RPCReceipt{
			To: &addr, ContractAddress: &addr, LogsBloom: &bloom, EffectiveGasPrice: &price,
			Status: &status, Root: hexutil.Bytes{0x03}, BlobGasPrice: &price, BlobGasUsed: &blobGas,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.r
			r.Logs = types.Logs{}
			want, err := json.Marshal(&r)
			require.NoError(t, err)

			s := jsonstream.Get(nil)
			defer jsonstream.Put(s)
			require.NoError(t, r.MarshalFastJSONTo(s))
			require.NoError(t, s.Flush())
			require.Equal(t, string(want), string(s.Buffer()))
		})
	}
}

// A logs shape the writer rejects must fail before anything is written, or the
// response carries a partial result next to the error.
func TestRPCReceiptMarshalFastJSONToRejectsBadLogsBeforeWriting(t *testing.T) {
	for name, logs := range map[string]any{
		"unknown shape": []string{"nope"},
		"unknown key":   []map[string]any{{"address": common.Address{}, "surprise": 1}},
		"unknown value": []map[string]any{{"data": 42}},
	} {
		t.Run(name, func(t *testing.T) {
			r := &RPCReceipt{Logs: logs}
			s := jsonstream.Get(nil)
			defer jsonstream.Put(s)
			require.Error(t, r.MarshalFastJSONTo(s))
			require.NoError(t, s.Flush())
			require.Empty(t, s.Buffer())
		})
	}
}
