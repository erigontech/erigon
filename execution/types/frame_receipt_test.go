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
	"bytes"
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/rlp"
)

func TestFrameReceiptEncoding(t *testing.T) {
	t.Parallel()
	payer := common.HexToAddress("0x1234")
	log := &Log{Address: payer, Topics: []common.Hash{{1}}, Data: []byte{2}}
	frames := []FrameReceipt{{Status: 1, GasUsed: mdgas.MdGas{Execution: 100}, Logs: Logs{log}}, {Status: 2, Logs: Logs{}}}
	r := &Receipt{Type: FrameTxType, CumulativeGasUsed: 12_000, Payer: &payer, FrameReceipts: frames, Logs: Logs{log}}
	data, err := rlp.EncodeToBytes([]any{uint64(12_000), payer, []any{
		[]any{uint64(1), []uint64{100, 0}, []*Log{log}},
		[]any{uint64(2), []uint64{0, 0}, []*Log{}},
	}})
	require.NoError(t, err)
	want := append([]byte{6}, data...)
	got, err := r.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, want, got)
	var trie bytes.Buffer
	Receipts{r}.EncodeIndex(0, &trie)
	require.Equal(t, want, trie.Bytes())
	var decoded Receipt
	require.NoError(t, decoded.UnmarshalBinary(want))
	require.Equal(t, r.Payer, decoded.Payer)
	require.Equal(t, r.FrameReceipts, decoded.FrameReceipts)
	require.Equal(t, r.Logs, decoded.Logs)
	require.Equal(t, CreateBloom(Receipts{r}), decoded.Bloom)
	enveloped, err := rlp.EncodeToBytes(r)
	require.NoError(t, err)
	decoded = Receipt{}
	require.NoError(t, rlp.DecodeBytes(enveloped, &decoded))
	require.Equal(t, r.FrameReceipts, decoded.FrameReceipts)
	require.Equal(t, CreateBloom(Receipts{r}), decoded.Bloom)
	var network bytes.Buffer
	require.NoError(t, r.EncodeRLP69(&network))
	wantNetwork, err := rlp.EncodeToBytes([]any{uint64(6), uint64(12_000), payer, frames})
	require.NoError(t, err)
	require.Equal(t, wantNetwork, network.Bytes())
}

func TestFrameReceiptCopy(t *testing.T) {
	t.Parallel()
	payer := common.HexToAddress("0x1234")
	log := &Log{Address: payer, Topics: []common.Hash{}, Data: []byte{1}}
	r := &Receipt{
		Type: FrameTxType, Payer: &payer, BlockNumber: uint256.NewInt(1), Logs: Logs{log}, BlobGasUsed: 131_072,
		FrameReceipts: []FrameReceipt{{Status: 1, Logs: Logs{log}}},
	}
	copy := r.Copy()
	require.Equal(t, r.Payer, copy.Payer)
	require.Equal(t, r.FrameReceipts, copy.FrameReceipts)
	require.Same(t, copy.Logs[0], copy.FrameReceipts[0].Logs[0])
	require.Equal(t, r.BlobGasUsed, copy.BlobGasUsed)
	copy.Payer[0] = 1
	copy.FrameReceipts[0].Logs[0].Data[0] = 2
	require.NotEqual(t, r.Payer, copy.Payer)
	require.Equal(t, byte(1), r.FrameReceipts[0].Logs[0].Data[0])
}

func TestFrameReceiptStorage(t *testing.T) {
	t.Parallel()
	payer := common.HexToAddress("0x1234")
	log := &Log{Address: payer, Topics: []common.Hash{{1}}, Data: []byte{2}}
	r := &Receipt{
		Type: FrameTxType, CumulativeGasUsed: 12_000, Payer: &payer,
		FrameReceipts: []FrameReceipt{{Status: 1, GasUsed: mdgas.MdGas{Execution: 100, State: 200}, Logs: Logs{log}}},
		Logs:          Logs{log},
		GasUsed:       300, FirstLogIndexWithinBlock: 4, TransactionIndex: 5,
	}
	b, err := rlp.EncodeToBytes((*ReceiptForStorage)(r))
	require.NoError(t, err)
	var stored ReceiptForStorage
	require.NoError(t, rlp.DecodeBytes(b, &stored))
	require.Equal(t, r.Payer, stored.Payer)
	require.Equal(t, r.FrameReceipts, stored.FrameReceipts)
	require.Equal(t, r.GasUsed, stored.GasUsed)
	require.Equal(t, r.FirstLogIndexWithinBlock, stored.FirstLogIndexWithinBlock)
	require.Equal(t, r.TransactionIndex, stored.TransactionIndex)
	require.Equal(t, r.Logs, stored.Logs)
	require.True(t, stored.Bloom.IsEmpty(), "storage decoding must leave bloom calculation to the caller")
	require.Nil(t, stored.derivedBloom.Load())
	require.Equal(t, CreateBloom(Receipts{r}), *(*Receipt)(&stored).LogsBloom())
	require.True(t, stored.Bloom.IsEmpty(), "lazy bloom calculation must not modify the shared Bloom field")
}

func TestFrameReceiptJSON(t *testing.T) {
	payer := common.Address{1}
	receipt := &Receipt{Type: FrameTxType, Payer: &payer, Logs: Logs{}, FrameReceipts: []FrameReceipt{{Status: 1, GasUsed: mdgas.MdGas{Execution: 100, State: 200}}}}
	data, err := json.Marshal(receipt)
	require.NoError(t, err)
	var decoded Receipt
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, receipt.Payer, decoded.Payer)
	require.Len(t, decoded.FrameReceipts, 1)
	require.Equal(t, receipt.FrameReceipts[0].GasUsed, decoded.FrameReceipts[0].GasUsed)
}

func TestFrameReceiptDecodeReuse(t *testing.T) {
	payer := common.Address{1}
	frame := &Receipt{Type: FrameTxType, Payer: &payer, FrameReceipts: []FrameReceipt{{Status: 1, Logs: Logs{}}}}
	legacy := &Receipt{Status: ReceiptStatusSuccessful}
	for _, envelope := range []bool{false, true} {
		var decoded Receipt
		for _, receipt := range []*Receipt{frame, legacy} {
			var data []byte
			var err error
			if envelope {
				data, err = rlp.EncodeToBytes(receipt)
				require.NoError(t, err)
				err = rlp.DecodeBytes(data, &decoded)
			} else {
				data, err = receipt.MarshalBinary()
				require.NoError(t, err)
				err = decoded.UnmarshalBinary(data)
			}
			require.NoError(t, err)
			require.Equal(t, receipt.Payer, decoded.Payer)
			require.Equal(t, receipt.FrameReceipts, decoded.FrameReceipts)
		}
	}
}
