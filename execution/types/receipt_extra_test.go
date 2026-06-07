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

package types

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
)

func sampleReceipt() *Receipt {
	return &Receipt{
		Type:              LegacyTxType,
		Status:            ReceiptStatusSuccessful,
		CumulativeGasUsed: 100,
		GasUsed:           60,
		BlockNumber:       uint256.NewInt(7),
		TransactionIndex:  2,
		Logs: Logs{{
			Address: common.HexToAddress("0x01"),
			Topics:  []common.Hash{common.HexToHash("0xaa")},
			Data:    []byte{1, 2, 3},
		}},
	}
}

func TestNewReceipt(t *testing.T) {
	t.Parallel()
	ok := NewReceipt(false, 21000)
	require.Equal(t, ReceiptStatusSuccessful, ok.Status)
	require.Equal(t, uint64(21000), ok.CumulativeGasUsed)
	require.Equal(t, uint8(LegacyTxType), ok.Type)

	failed := NewReceipt(true, 1)
	require.Equal(t, ReceiptStatusFailed, failed.Status)
}

func TestReceipt_Copy(t *testing.T) {
	t.Parallel()
	require.Nil(t, (*Receipt)(nil).Copy())

	orig := sampleReceipt()
	cp := orig.Copy()
	require.Equal(t, orig.CumulativeGasUsed, cp.CumulativeGasUsed)
	require.Len(t, cp.Logs, 1)

	// Deep copy: mutating the original's logs/blockNumber must not affect the copy.
	orig.Logs[0].Data[0] = 9
	orig.BlockNumber.SetUint64(999)
	require.NotEqual(t, orig.Logs[0].Data[0], cp.Logs[0].Data[0])
	require.NotEqual(t, orig.BlockNumber.Uint64(), cp.BlockNumber.Uint64())
}

func TestReceipts_LenCopyCumulative(t *testing.T) {
	t.Parallel()
	require.Nil(t, Receipts(nil).Copy())
	require.Equal(t, uint64(0), Receipts(nil).CumulativeGasUsed())

	rs := Receipts{sampleReceipt(), {CumulativeGasUsed: 250, BlockNumber: uint256.NewInt(1)}}
	require.Equal(t, 2, rs.Len())
	require.Equal(t, uint64(250), rs.CumulativeGasUsed())

	cp := rs.Copy()
	require.Len(t, cp, 2)
	require.Equal(t, rs[0].CumulativeGasUsed, cp[0].CumulativeGasUsed)
}

func TestReceipt_SetStatus(t *testing.T) {
	t.Parallel()
	var r Receipt

	require.NoError(t, r.setStatus(receiptStatusSuccessfulRLP))
	require.Equal(t, ReceiptStatusSuccessful, r.Status)

	require.NoError(t, r.setStatus(receiptStatusFailedRLP))
	require.Equal(t, ReceiptStatusFailed, r.Status)

	postState := make([]byte, 32)
	require.NoError(t, r.setStatus(postState))
	require.Equal(t, postState, r.PostState)

	require.ErrorContains(t, r.setStatus([]byte{0x02, 0x03}), "invalid receipt status")
}

func TestReceipt_StatusEncoding(t *testing.T) {
	t.Parallel()
	success := &Receipt{Status: ReceiptStatusSuccessful}
	require.Equal(t, receiptStatusSuccessfulRLP, success.statusEncoding())

	failed := &Receipt{Status: ReceiptStatusFailed}
	require.Equal(t, receiptStatusFailedRLP, failed.statusEncoding())

	withState := &Receipt{PostState: make([]byte, 32)}
	require.Equal(t, withState.PostState, withState.statusEncoding())
}

func TestReceipt_MarshalBinaryRoundTrip(t *testing.T) {
	t.Parallel()
	for name, typ := range map[string]uint8{
		"legacy":     LegacyTxType,
		"dynamicfee": DynamicFeeTxType,
	} {
		typ := typ
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := sampleReceipt()
			r.Type = typ

			enc, err := r.MarshalBinary()
			require.NoError(t, err)

			var got Receipt
			require.NoError(t, got.UnmarshalBinary(enc))
			require.Equal(t, r.Type, got.Type)
			require.Equal(t, r.Status, got.Status)
			require.Equal(t, r.CumulativeGasUsed, got.CumulativeGasUsed)
			require.Len(t, got.Logs, 1)
		})
	}
}

func TestReceipt_UnmarshalBinary_Errors(t *testing.T) {
	t.Parallel()
	require.ErrorIs(t, (&Receipt{}).UnmarshalBinary([]byte{DynamicFeeTxType}), errShortTypedReceipt)
	require.ErrorIs(t, (&Receipt{}).UnmarshalBinary([]byte{0x09, 0xc0}), ErrTxTypeNotSupported)
}

func TestReceiptForStorage_RLPRoundTrip(t *testing.T) {
	t.Parallel()
	r := sampleReceipt()
	enc, err := rlp.EncodeToBytes((*ReceiptForStorage)(r))
	require.NoError(t, err)

	var got ReceiptForStorage
	require.NoError(t, rlp.DecodeBytes(enc, &got))
	require.Equal(t, r.Type, got.Type)
	require.Equal(t, r.CumulativeGasUsed, got.CumulativeGasUsed)
	require.Equal(t, r.GasUsed, got.GasUsed)
	require.Len(t, got.Logs, 1)
}

func TestReceipt_String(t *testing.T) {
	t.Parallel()
	require.Contains(t, sampleReceipt().String(), "cumulativeGasUsed")
}

func TestReceipts_AssertLogIndex_NoopWhenDisabled(t *testing.T) {
	t.Parallel()
	// dbg.AssertEnabled is off by default, so this must not panic.
	require.NotPanics(t, func() { Receipts{sampleReceipt()}.AssertLogIndex(1) })
}

func TestReceipt_DeriveFieldsV4ForCachedReceipt(t *testing.T) {
	t.Parallel()
	r := sampleReceipt()
	r.TransactionIndex = 3
	blockHash := common.HexToHash("0xbeef")
	txHash := common.HexToHash("0xfeed")

	r.DeriveFieldsV4ForCachedReceipt(blockHash, 42, txHash, true)

	require.Equal(t, blockHash, r.BlockHash)
	require.Equal(t, uint64(42), r.BlockNumber.Uint64())
	require.Equal(t, txHash, r.TxHash)
	require.Equal(t, blockHash, r.Logs[0].BlockHash)
	require.Equal(t, uint64(42), uint64(r.Logs[0].BlockNumber))
	require.Equal(t, uint(3), uint(r.Logs[0].TxIndex))
	require.NotEqual(t, Bloom{}, r.Bloom)
}
