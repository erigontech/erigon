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
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestFrameRPCReceipt(t *testing.T) {
	payer := common.Address{1}
	tx := &types.FrameTransaction{NonceKeys: nil, SenderAddress: payer, Frames: []types.Frame{{}}}
	receipt := &types.Receipt{Type: types.FrameTxType, Payer: &payer, BlockNumber: uint256.NewInt(1), FrameReceipts: []types.FrameReceipt{{Status: 1, Logs: types.Logs{}}}}
	result := MarshalReceipt(receipt, tx, chain.AllProtocolChanges, &types.Header{Number: *uint256.NewInt(1), BaseFee: uint256.NewInt(0)}, true, false)
	data, err := json.Marshal(result)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &fields))
	require.Contains(t, fields, "payer")
	require.Contains(t, fields, "frameReceipts")
}

func TestFrameRPCReceiptLogTimestamps(t *testing.T) {
	t.Parallel()
	logs := types.Logs{{Index: 1, Topics: []common.Hash{}}, {Index: 2, Topics: []common.Hash{}}, {Index: 3, Topics: []common.Hash{}}}
	receipt := &types.Receipt{
		Type: types.FrameTxType, BlockNumber: uint256.NewInt(1), Logs: logs,
		FrameReceipts: []types.FrameReceipt{{}, {Status: 1, Logs: logs[:2:2]}, {}, {Status: 1, Logs: logs[2:]}},
	}
	before, err := json.Marshal(receipt)
	require.NoError(t, err)
	txn := &types.FrameTransaction{}
	for _, tc := range []struct {
		name  string
		stamp bool
		time  uint64
	}{
		{"stamped", true, 42},
		{"unstamped", false, 42},
		{"zero timestamp", true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			header := &types.Header{Number: *uint256.NewInt(1), Time: tc.time, BaseFee: uint256.NewInt(0)}
			result := MarshalReceipt(receipt, txn, chain.AllProtocolChanges, header, false, tc.stamp)
			data, err := json.Marshal(result)
			require.NoError(t, err)
			var decoded struct {
				Logs          types.Logs
				FrameReceipts types.FrameReceipts
			}
			require.NoError(t, json.Unmarshal(data, &decoded))
			require.Len(t, decoded.FrameReceipts, len(receipt.FrameReceipts))
			offset := 0
			for i, frame := range decoded.FrameReceipts {
				require.Len(t, frame.Logs, len(receipt.FrameReceipts[i].Logs))
				for _, log := range frame.Logs {
					require.Equal(t, decoded.Logs[offset], log)
					if tc.stamp {
						require.Equal(t, stampedAt(tc.time), log.BlockTimestamp)
					} else {
						require.Nil(t, log.BlockTimestamp)
					}
					offset++
				}
			}
			require.Equal(t, len(logs), offset)
			after, err := json.Marshal(receipt)
			require.NoError(t, err)
			require.Equal(t, string(before), string(after), "RPC rendering must not change the cached receipt")
			requireFastJSONMatches(t, result)
		})
	}
}

// Receipts in the RPC cache carry logs already stamped with the block time: they are rendered as
// they are when the timestamp is wanted, and without it otherwise.
func TestMarshalReceiptWithStampedLogs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		stamp    bool
		time     uint64
		wantSame bool
	}{
		{"stamped with the block time", true, 42, true},
		{"unstamped", false, 42, false},
		{"stamped with another time", true, 43, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := types.Logs{{Index: 1, Topics: []common.Hash{}}, {Index: 2, Topics: []common.Hash{}}, {Index: 3, Topics: []common.Hash{}}}
			logs.SetBlockTimestamp(42)
			receipt := &types.Receipt{
				Type: types.FrameTxType, BlockNumber: uint256.NewInt(1), Logs: logs,
				FrameReceipts: []types.FrameReceipt{{Status: 1, Logs: logs[:2:2]}, {Status: 1, Logs: logs[2:]}},
			}
			header := &types.Header{Number: *uint256.NewInt(1), Time: tc.time, BaseFee: uint256.NewInt(0)}
			result := MarshalReceipt(receipt, &types.FrameTransaction{}, chain.AllProtocolChanges, header, false, tc.stamp)
			require.Len(t, result.Logs, len(logs))
			for i, l := range result.Logs {
				if tc.stamp {
					require.Equal(t, stampedAt(tc.time), l.BlockTimestamp)
				} else {
					require.Nil(t, l.BlockTimestamp)
				}
				require.Equal(t, tc.wantSame, l == logs[i])
				require.Equal(t, stampedAt(42), logs[i].BlockTimestamp, "the cached log keeps its timestamp")
			}
			require.Same(t, result.Logs[0], result.FrameReceipts[0].Logs[0])
			require.Same(t, result.Logs[2], result.FrameReceipts[1].Logs[0])
			requireFastJSONMatches(t, result)
		})
	}
}

// MarshalReceiptWithoutLogs renders the receipt as a signed MarshalReceipt without timestamp, minus
// the logs and the bloom, and leaves the cached logs untouched.
func TestMarshalReceiptWithoutLogs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		txn  types.Transaction
	}{
		{"legacy", &types.LegacyTx{}},
		{"frame", &types.FrameTransaction{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := types.Logs{{Index: 1, Topics: []common.Hash{}}, {Index: 2, Topics: []common.Hash{}}}
			logs.SetBlockTimestamp(42)
			receipt := &types.Receipt{Type: tc.txn.Type(), BlockNumber: uint256.NewInt(1), Logs: logs}
			if tc.txn.Type() == types.FrameTxType {
				receipt.FrameReceipts = []types.FrameReceipt{{Status: 1, Logs: logs[:1:1]}, {Status: 1, Logs: logs[1:]}}
			}
			header := &types.Header{Number: *uint256.NewInt(1), Time: 42, BaseFee: uint256.NewInt(0)}

			want := MarshalReceipt(receipt, tc.txn, chain.AllProtocolChanges, header, true, false)
			want.Logs, want.LogsBloom = nil, nil
			got := MarshalReceiptWithoutLogs(receipt, tc.txn, chain.AllProtocolChanges, header)
			require.Equal(t, want, got)
			for _, l := range logs {
				require.Equal(t, stampedAt(42), l.BlockTimestamp, "the cached log keeps its timestamp")
			}
		})
	}
}

func TestFrameReceiptEffectiveGasPrice(t *testing.T) {
	for _, blobs := range []bool{false, true} {
		t.Run(map[bool]string{false: "unused gas backs tip", true: "blob fee uses budget"}[blobs], func(t *testing.T) {
			tx := &types.FrameTransaction{Frames: []types.Frame{{Limits: mdgas.MdGas{Execution: 100_000}}}, Fees: types.FrameFees{TipCap: uint256.Int{2}}}
			header := &types.Header{Number: *uint256.NewInt(1), BaseFee: uint256.NewInt(10)}
			used, want := uint64(25_000), uint64(12)
			if blobs {
				tx.BlobVersionedHashes = []common.Hash{{1}}
				header.ExcessBlobGas = common.NewUint64(0)
				used = tx.GetGasLimit()
				want = 10
			}
			tx.Fees.MaxFee.SetUint64(tx.GetGasLimit()*10 + tx.GetBlobGas())
			receipt := &types.Receipt{Type: types.FrameTxType, BlockNumber: uint256.NewInt(1), GasUsed: used}
			got := MarshalReceipt(receipt, tx, chain.AllProtocolChanges, header, false, false)
			require.Equal(t, want, (*uint256.Int)(got.EffectiveGasPrice).Uint64())
		})
	}
}
