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
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/jsonstream/ethjsontest"
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

	fields := MarshalReceipt(receipt, txn, config, header, false, false)
	assert.Equal(t, preset, *fields.LogsBloom)

	receipt.Bloom = types.Bloom{}
	fields = MarshalReceipt(receipt, txn, config, header, false, false)
	assert.Equal(t, types.CreateBloom(types.Receipts{receipt}), *fields.LogsBloom)
}

// The backend leaves "from" unset when it cannot recover the sender. The zero
// address is a valid address, so an unknown sender must marshal to null like
// "to" and "contractAddress" do, not to 0x00..00.
func TestMarshalSubscribeReceiptWithoutSender(t *testing.T) {
	reply := &remoteproto.SubscribeReceiptsReply{
		BlockHash:       gointerfaces.ConvertHashToH256(common.Hash{1}),
		TransactionHash: gointerfaces.ConvertHashToH256(common.Hash{2}),
	}
	receipt := MarshalSubscribeReceipt(reply)
	assert.Nil(t, receipt.From)

	encoded, err := json.Marshal(receipt)
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"from":null`)
}

// A sender the backend did set must survive as an address, the zero one
// included: only an unset "from" marshals to null.
func TestMarshalSubscribeReceiptKeepsZeroSender(t *testing.T) {
	reply := &remoteproto.SubscribeReceiptsReply{
		BlockHash:       gointerfaces.ConvertHashToH256(common.Hash{1}),
		TransactionHash: gointerfaces.ConvertHashToH256(common.Hash{2}),
		From:            gointerfaces.ConvertAddressToH160(common.Address{}),
	}
	receipt := MarshalSubscribeReceipt(reply)
	require.NotNil(t, receipt.From)
	assert.Equal(t, common.Address{}, *receipt.From)
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
				// Every derived field distinct, so a field written from the wrong source shows.
				logs[i] = &types.Log{
					Address: to, Topics: []common.Hash{{0x01}, {0x02}}, Data: make([]byte, 64),
					BlockNumber: 7, TxHash: common.HexToHash("0xbeef"), TxIndex: 3, BlockHash: common.HexToHash("0xb10c"),
					Index: hexutil.Uint(10 + i), Removed: i == 1,
				}
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
			r := MarshalReceipt(receipt, &txn, chain.TestChainOsakaConfig, header, true, tc.withBlockTimestamp)

			requireFastJSONMatches(t, r)
		})
	}
}

// Every Logs shape against encoding/json. MarshalReceipt never builds a nil slice, so only a
// direct construction reaches the typed-nil branches.
func TestRPCReceiptMarshalFastJSONToLogShapes(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	for name, logs := range map[string]any{
		"nil types.Logs":    types.Logs(nil),
		"nil []*Log":        []*types.Log(nil),
		"nil []*RPCLog":     []*types.RPCLog(nil),
		"untyped nil":       nil,
		"nil in types.Logs": types.Logs{nil, {Address: to}},
		"nil in []*Log":     []*types.Log{{Address: to}, nil},
		"unknown shape":     []string{"a"},
	} {
		t.Run(name, func(t *testing.T) { requireFastJSONMatches(t, &RPCReceipt{Logs: logs}) })
	}
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
			requireFastJSONMatches(t, rs)
		})
	}
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
			requireFastJSONMatches(t, &r)
		})
	}
}

func requireFastJSONMatches(t *testing.T, v interface {
	MarshalFastJSONTo(*jsonstream.StackStream) error
},
) {
	t.Helper()
	want, err := json.Marshal(v)
	require.NoError(t, err)

	s := jsonstream.Get(nil)
	defer jsonstream.Put(s)
	require.NoError(t, v.MarshalFastJSONTo(s))
	require.NoError(t, s.Flush())
	require.Equal(t, string(want), string(s.Buffer()))
}

// Subscription receipts carry the same log objects as eth_getTransactionReceipt.
func TestMarshalSubscribeReceiptFullLogs(t *testing.T) {
	addr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	blockHash, txHash, topic := common.HexToHash("0xb1"), common.HexToHash("0xaa"), common.HexToHash("0x01")
	r := MarshalSubscribeReceipt(&remoteproto.SubscribeReceiptsReply{
		BlockHash:       gointerfaces.ConvertHashToH256(blockHash),
		TransactionHash: gointerfaces.ConvertHashToH256(txHash),
		From:            gointerfaces.ConvertAddressToH160(addr),
		Logs: []*remoteproto.SubscribeLogsReply{{
			Address:          gointerfaces.ConvertAddressToH160(addr),
			BlockHash:        gointerfaces.ConvertHashToH256(blockHash),
			BlockNumber:      7,
			Data:             []byte{0x2a},
			LogIndex:         3,
			Topics:           []*typesproto.H256{gointerfaces.ConvertHashToH256(topic)},
			TransactionHash:  gointerfaces.ConvertHashToH256(txHash),
			TransactionIndex: 2,
			Removed:          true,
			BlockTimestamp:   99,
		}},
	})
	require.Equal(t, []*types.RPCLog{{
		Log: types.Log{
			Address: addr, Topics: []common.Hash{topic}, Data: []byte{0x2a}, BlockNumber: 7,
			TxHash: txHash, TxIndex: 2, BlockHash: blockHash, Index: 3, Removed: true,
		},
		BlockTimestamp: 99,
	}}, r.Logs)
}

// effectiveGasPrice is the base fee plus the tip, which only the backend can compute. A backend
// that does not send it yet leaves the base fee as the best approximation available.
func TestMarshalSubscribeReceiptEffectiveGasPrice(t *testing.T) {
	for name, tc := range map[string]struct {
		baseFee, effectiveGasPrice *uint256.Int
		want                       *uint256.Int
	}{
		"effective gas price": {uint256.NewInt(7), uint256.NewInt(9), uint256.NewInt(9)},
		"base fee only":       {uint256.NewInt(7), nil, uint256.NewInt(7)},
		"neither":             {nil, nil, nil},
	} {
		t.Run(name, func(t *testing.T) {
			reply := &remoteproto.SubscribeReceiptsReply{
				BlockHash:       gointerfaces.ConvertHashToH256(common.Hash{1}),
				TransactionHash: gointerfaces.ConvertHashToH256(common.Hash{2}),
			}
			if tc.baseFee != nil {
				reply.BaseFee = gointerfaces.ConvertUint256IntToH256(tc.baseFee)
			}
			if tc.effectiveGasPrice != nil {
				reply.EffectiveGasPrice = gointerfaces.ConvertUint256IntToH256(tc.effectiveGasPrice)
			}
			got := MarshalSubscribeReceipt(reply).EffectiveGasPrice
			if tc.want == nil {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, tc.want, (*uint256.Int)(got))
		})
	}
}

// Every RPCReceipt field says which of the spec's forms it is written as, and the encoder is
// held to that: a dropped field, a wrong form or a reordered key fails here.
func TestRPCReceiptMatchesItsTags(t *testing.T) {
	t.Parallel()
	addr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	bloom := types.Bloom{1, 2, 3}
	status := hexutil.Uint64(1)
	price := hexutil.U256(*uint256.NewInt(7))
	blobGas := hexutil.Uint64(9)
	logs := []*types.RPCLog{{Log: types.Log{
		Address: addr, Topics: []common.Hash{{0x01}}, Data: []byte{1, 2},
		BlockNumber: 7, TxHash: common.HexToHash("0xbeef"), TxIndex: 3,
		BlockHash: common.HexToHash("0xb10c"), Index: 4, Removed: true,
	}, BlockTimestamp: 1_750_000_000}}

	for name, r := range map[string]*RPCReceipt{
		"every field set": {
			BlockHash: common.HexToHash("0xb10c"), BlockNumber: 7,
			TransactionHash: common.HexToHash("0xbeef"), TransactionIndex: 3,
			From: &addr, To: &addr, Type: 2, GasUsed: 21_000, CumulativeGasUsed: 42_000,
			ContractAddress: &addr, Logs: logs, LogsBloom: &bloom,
			EffectiveGasPrice: &price, Status: &status, Root: hexutil.Bytes{9},
			BlobGasPrice: &price, BlobGasUsed: &blobGas,
		},
		"optional fields absent": {Logs: []*types.RPCLog{}},
		// The timestamp-less shape goes through its own writer, held to the same tags.
		"plain logs": {Logs: types.Logs{{
			Address: addr, Topics: []common.Hash{{0x01}}, Data: []byte{1, 2},
			BlockNumber: 7, TxHash: common.HexToHash("0xbeef"), TxIndex: 3,
			BlockHash: common.HexToHash("0xb10c"), Index: 4, Removed: true,
		}}},
	} {
		t.Run(name, func(t *testing.T) {
			want, err := ethjsontest.ExpectedJSON(r)
			require.NoError(t, err)
			got, err := jsonstream.Marshal(r)
			require.NoError(t, err)
			require.Equal(t, string(want), string(got))
		})
	}
}
