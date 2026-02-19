// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/rlp"
)

func TestDecodeEmptyTypedReceipt(t *testing.T) {
	t.Parallel()
	input := []byte{0x80}
	var r Receipt
	err := rlp.DecodeBytes(input, &r)
	if !errors.Is(err, rlp.EOL) {
		t.Fatal("wrong error:", err)
	}
}

func TestLegacyReceiptDecoding(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		encode func(*Receipt) ([]byte, error)
	}{
		{
			"StoredReceiptRLP",
			encodeAsStoredReceiptRLP,
		},
		// Erigon: all the legacy formats are removed intentionally
	}

	tx := NewTransaction(1, common.HexToAddress("0x1"), &u256.Num1, 1, &u256.Num1, nil)
	receipt := &Receipt{
		Status:            ReceiptStatusFailed,
		CumulativeGasUsed: 1,
		Logs: []*Log{
			{
				Address: common.BytesToAddress([]byte{0x11}),
				Topics:  []common.Hash{common.HexToHash("dead"), common.HexToHash("beef")},
				Data:    []byte{0x01, 0x00, 0xff},
				Index:   999,
			},
			{
				Address: common.BytesToAddress([]byte{0x01, 0x11}),
				Topics:  []common.Hash{common.HexToHash("dead"), common.HexToHash("beef")},
				Data:    []byte{0x01, 0x00, 0xff},
				Index:   1000,
			},
		},
		TxHash:          tx.Hash(),
		ContractAddress: common.BytesToAddress([]byte{0x01, 0x11, 0x11}),
		GasUsed:         111111,
		BlockNumber:     uint256.NewInt(1),
	}
	receipt.Bloom = CreateBloom(Receipts{receipt})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			enc, err := tc.encode(receipt)
			if err != nil {
				t.Fatalf("Error encoding receipt: %v", err)
			}
			var dec ReceiptForStorage
			if err := rlp.DecodeBytes(enc, &dec); err != nil {
				t.Fatalf("Error decoding RLP receipt: %v", err)
			}
			// Check whether all consensus fields are correct.
			if dec.Status != receipt.Status {
				t.Fatalf("Receipt status mismatch, want %v, have %v", receipt.Status, dec.Status)
			}
			if dec.CumulativeGasUsed != receipt.CumulativeGasUsed {
				t.Fatalf("Receipt CumulativeGasUsed mismatch, want %v, have %v", receipt.CumulativeGasUsed, dec.CumulativeGasUsed)
			}
			assert.Equal(t, uint32(receipt.Logs[0].Index), dec.FirstLogIndexWithinBlock)
			//if len(dec.Logs) != len(receipt.Logs) {
			//	t.Fatalf("Receipt log number mismatch, want %v, have %v", len(receipt.Logs), len(dec.Logs))
			//}
			//for i := 0; i < len(dec.Logs); i++ {
			//	if dec.Logs[i].Address != receipt.Logs[i].Address {
			//		t.Fatalf("Receipt log %d address mismatch, want %v, have %v", i, receipt.Logs[i].Address, dec.Logs[i].Address)
			//	}
			//	if !reflect.DeepEqual(dec.Logs[i].Topics, receipt.Logs[i].Topics) {
			//		t.Fatalf("Receipt log %d topics mismatch, want %v, have %v", i, receipt.Logs[i].Topics, dec.Logs[i].Topics)
			//	}
			//	if !bytes.Equal(dec.Logs[i].Data, receipt.Logs[i].Data) {
			//		t.Fatalf("Receipt log %d data mismatch, want %v, have %v", i, receipt.Logs[i].Data, dec.Logs[i].Data)
			//	}
			//}
		})
	}
}

func encodeAsStoredReceiptRLP(want *Receipt) ([]byte, error) {
	w := bytes.NewBuffer(nil)
	casted := ReceiptForStorage(*want)
	err := casted.EncodeRLP(w)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// TestTypedReceiptEncodingDecoding reproduces a flaw that existed in the receipt
// rlp decoder, which failed due to a shadowing error.
func TestTypedReceiptEncodingDecoding(t *testing.T) {
	t.Parallel()
	var payload = common.FromHex("f9043eb9010c01f90108018262d4b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c0b9010c01f901080182cd14b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c0b9010d01f901090183013754b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c0b9010d01f90109018301a194b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c0")
	check := func(bundle []*Receipt) {
		t.Helper()
		for i, receipt := range bundle {
			if got, want := receipt.Type, uint8(1); got != want {
				t.Fatalf("bundle %d: got %x, want %x", i, got, want)
			}
		}
	}
	{
		var bundle []*Receipt
		if err := rlp.DecodeBytes(payload, &bundle); err != nil {
			t.Fatal(err)
		}
		check(bundle)
	}
	{
		var bundle []*Receipt
		r := bytes.NewReader(payload)
		s := rlp.NewStream(r, uint64(len(payload)))
		if err := s.Decode(&bundle); err != nil {
			t.Fatal(err)
		}
		check(bundle)
	}
}

func clearComputedFieldsOnReceipts(t *testing.T, receipts Receipts) {
	t.Helper()

	for _, receipt := range receipts {
		clearComputedFieldsOnReceipt(t, receipt)
	}
}

func clearComputedFieldsOnReceipt(t *testing.T, receipt *Receipt) {
	t.Helper()

	receipt.TxHash = common.Hash{}
	receipt.BlockHash = common.Hash{}
	receipt.BlockNumber = uint256.NewInt(math.MaxUint32)
	receipt.TransactionIndex = math.MaxUint32
	receipt.ContractAddress = common.Address{}
	receipt.GasUsed = 0

	clearComputedFieldsOnLogs(t, receipt.Logs)
}

func clearComputedFieldsOnLogs(t *testing.T, logs []*Log) {
	t.Helper()

	for _, log := range logs {
		clearComputedFieldsOnLog(t, log)
	}
}

func clearComputedFieldsOnLog(t *testing.T, log *Log) {
	t.Helper()

	log.BlockNumber = math.MaxUint32
	log.BlockHash = common.Hash{}
	log.TxHash = common.Hash{}
	log.TxIndex = math.MaxUint32
	log.Index = math.MaxUint32
}

func TestReceiptUnmarshalBinary(t *testing.T) {
	legacyReceipt := &Receipt{
		Status:            ReceiptStatusFailed,
		CumulativeGasUsed: 1,
		Logs: []*Log{
			{
				Address: common.BytesToAddress([]byte{0x11}),
				Topics:  []common.Hash{common.HexToHash("dead"), common.HexToHash("beef")},
				Data:    []byte{0x01, 0x00, 0xff},
			},
			{
				Address: common.BytesToAddress([]byte{0x01, 0x11}),
				Topics:  []common.Hash{common.HexToHash("dead"), common.HexToHash("beef")},
				Data:    []byte{0x01, 0x00, 0xff},
			},
		},
	}
	accessListReceipt := &Receipt{
		Status:            ReceiptStatusFailed,
		CumulativeGasUsed: 1,
		Logs: []*Log{
			{
				Address: common.BytesToAddress([]byte{0x11}),
				Topics:  []common.Hash{common.HexToHash("dead"), common.HexToHash("beef")},
				Data:    []byte{0x01, 0x00, 0xff},
			},
			{
				Address: common.BytesToAddress([]byte{0x01, 0x11}),
				Topics:  []common.Hash{common.HexToHash("dead"), common.HexToHash("beef")},
				Data:    []byte{0x01, 0x00, 0xff},
			},
		},
		Type: AccessListTxType,
	}
	eip1559Receipt := &Receipt{
		Status:            ReceiptStatusFailed,
		CumulativeGasUsed: 1,
		Logs: []*Log{
			{
				Address: common.BytesToAddress([]byte{0x11}),
				Topics:  []common.Hash{common.HexToHash("dead"), common.HexToHash("beef")},
				Data:    []byte{0x01, 0x00, 0xff},
			},
			{
				Address: common.BytesToAddress([]byte{0x01, 0x11}),
				Topics:  []common.Hash{common.HexToHash("dead"), common.HexToHash("beef")},
				Data:    []byte{0x01, 0x00, 0xff},
			},
		},
		Type: DynamicFeeTxType,
	}

	t.Run("MarshalBinary", func(t *testing.T) {
		// Legacy Receipt
		legacyReceipt.Bloom = CreateBloom(Receipts{legacyReceipt})
		have, err := legacyReceipt.MarshalBinary()
		if err != nil {
			t.Fatalf("marshal binary error: %v", err)
		}
		legacyReceipts := Receipts{legacyReceipt}
		buf := new(bytes.Buffer)
		legacyReceipts.EncodeIndex(0, buf)
		haveEncodeIndex := buf.Bytes()
		if !bytes.Equal(have, haveEncodeIndex) {
			t.Errorf("BinaryMarshal and EncodeIndex mismatch, got %x want %x", have, haveEncodeIndex)
		}
		buf.Reset()
		if err := legacyReceipt.EncodeRLP(buf); err != nil {
			t.Fatalf("encode rlp error: %v", err)
		}
		haveRLPEncode := buf.Bytes()
		if !bytes.Equal(have, haveRLPEncode) {
			t.Errorf("BinaryMarshal and EncodeRLP mismatch for legacy tx, got %x want %x", have, haveRLPEncode)
		}
		legacyWant := common.FromHex("f901c58001b9010000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000500000000000000000000000000000000000014000000000000000000000000000000000000000000000000000000000000000000000000000010000080000000000000000000004000000000000000000000000000040000000000000000000000000000800000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000f8bef85d940000000000000000000000000000000000000011f842a0000000000000000000000000000000000000000000000000000000000000deada0000000000000000000000000000000000000000000000000000000000000beef830100fff85d940000000000000000000000000000000000000111f842a0000000000000000000000000000000000000000000000000000000000000deada0000000000000000000000000000000000000000000000000000000000000beef830100ff")
		if !bytes.Equal(have, legacyWant) {
			t.Errorf("encoded RLP mismatch, got %x want %x", have, legacyWant)
		}

		// 2930 Receipt
		buf.Reset()
		accessListReceipt.Bloom = CreateBloom(Receipts{accessListReceipt})
		have, err = accessListReceipt.MarshalBinary()
		if err != nil {
			t.Fatalf("marshal binary error: %v", err)
		}
		accessListReceipts := Receipts{accessListReceipt}
		accessListReceipts.EncodeIndex(0, buf)
		haveEncodeIndex = buf.Bytes()
		if !bytes.Equal(have, haveEncodeIndex) {
			t.Errorf("BinaryMarshal and EncodeIndex mismatch, got %x want %x", have, haveEncodeIndex)
		}
		accessListWant := common.FromHex("01f901c58001b9010000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000500000000000000000000000000000000000014000000000000000000000000000000000000000000000000000000000000000000000000000010000080000000000000000000004000000000000000000000000000040000000000000000000000000000800000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000f8bef85d940000000000000000000000000000000000000011f842a0000000000000000000000000000000000000000000000000000000000000deada0000000000000000000000000000000000000000000000000000000000000beef830100fff85d940000000000000000000000000000000000000111f842a0000000000000000000000000000000000000000000000000000000000000deada0000000000000000000000000000000000000000000000000000000000000beef830100ff")
		if !bytes.Equal(have, accessListWant) {
			t.Errorf("encoded RLP mismatch, got %x want %x", have, accessListWant)
		}

		// 1559 Receipt
		buf.Reset()
		eip1559Receipt.Bloom = CreateBloom(Receipts{eip1559Receipt})
		have, err = eip1559Receipt.MarshalBinary()
		if err != nil {
			t.Fatalf("marshal binary error: %v", err)
		}
		eip1559Receipts := Receipts{eip1559Receipt}
		eip1559Receipts.EncodeIndex(0, buf)
		haveEncodeIndex = buf.Bytes()
		if !bytes.Equal(have, haveEncodeIndex) {
			t.Errorf("BinaryMarshal and EncodeIndex mismatch, got %x want %x", have, haveEncodeIndex)
		}
		eip1559Want := common.FromHex("02f901c58001b9010000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000500000000000000000000000000000000000014000000000000000000000000000000000000000000000000000000000000000000000000000010000080000000000000000000004000000000000000000000000000040000000000000000000000000000800000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000f8bef85d940000000000000000000000000000000000000011f842a0000000000000000000000000000000000000000000000000000000000000deada0000000000000000000000000000000000000000000000000000000000000beef830100fff85d940000000000000000000000000000000000000111f842a0000000000000000000000000000000000000000000000000000000000000deada0000000000000000000000000000000000000000000000000000000000000beef830100ff")
		if !bytes.Equal(have, eip1559Want) {
			t.Errorf("encoded RLP mismatch, got %x want %x", have, eip1559Want)
		}
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		// Legacy Receipt
		legacyBinary := common.FromHex("f901c58001b9010000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000500000000000000000000000000000000000014000000000000000000000000000000000000000000000000000000000000000000000000000010000080000000000000000000004000000000000000000000000000040000000000000000000000000000800000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000f8bef85d940000000000000000000000000000000000000011f842a0000000000000000000000000000000000000000000000000000000000000deada0000000000000000000000000000000000000000000000000000000000000beef830100fff85d940000000000000000000000000000000000000111f842a0000000000000000000000000000000000000000000000000000000000000deada0000000000000000000000000000000000000000000000000000000000000beef830100ff")
		gotLegacyReceipt := new(Receipt)
		if err := gotLegacyReceipt.UnmarshalBinary(legacyBinary); err != nil {
			t.Fatalf("unmarshal binary error: %v", err)
		}
		legacyReceipt.Bloom = CreateBloom(Receipts{legacyReceipt})
		if !reflect.DeepEqual(gotLegacyReceipt, legacyReceipt) {
			t.Errorf("receipt unmarshalled from binary mismatch, got %v want %v", gotLegacyReceipt, legacyReceipt)
		}

		// 2930 Receipt
		accessListBinary := common.FromHex("01f901c58001b9010000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000500000000000000000000000000000000000014000000000000000000000000000000000000000000000000000000000000000000000000000010000080000000000000000000004000000000000000000000000000040000000000000000000000000000800000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000f8bef85d940000000000000000000000000000000000000011f842a0000000000000000000000000000000000000000000000000000000000000deada0000000000000000000000000000000000000000000000000000000000000beef830100fff85d940000000000000000000000000000000000000111f842a0000000000000000000000000000000000000000000000000000000000000deada0000000000000000000000000000000000000000000000000000000000000beef830100ff")
		gotAccessListReceipt := new(Receipt)
		if err := gotAccessListReceipt.UnmarshalBinary(accessListBinary); err != nil {
			t.Fatalf("unmarshal binary error: %v", err)
		}
		accessListReceipt.Bloom = CreateBloom(Receipts{accessListReceipt})
		if !reflect.DeepEqual(gotAccessListReceipt, accessListReceipt) {
			t.Errorf("receipt unmarshalled from binary mismatch, got %v want %v", gotAccessListReceipt, accessListReceipt)
		}

		// 1559 Receipt
		eip1559RctBinary := common.FromHex("02f901c58001b9010000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000500000000000000000000000000000000000014000000000000000000000000000000000000000000000000000000000000000000000000000010000080000000000000000000004000000000000000000000000000040000000000000000000000000000800000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000f8bef85d940000000000000000000000000000000000000011f842a0000000000000000000000000000000000000000000000000000000000000deada0000000000000000000000000000000000000000000000000000000000000beef830100fff85d940000000000000000000000000000000000000111f842a0000000000000000000000000000000000000000000000000000000000000deada0000000000000000000000000000000000000000000000000000000000000beef830100ff")
		got1559Receipt := new(Receipt)
		if err := got1559Receipt.UnmarshalBinary(eip1559RctBinary); err != nil {
			t.Fatalf("unmarshal binary error: %v", err)
		}
		eip1559Receipt.Bloom = CreateBloom(Receipts{eip1559Receipt})
		if !reflect.DeepEqual(got1559Receipt, eip1559Receipt) {
			t.Errorf("receipt unmarshalled from binary mismatch, got %v want %v", got1559Receipt, eip1559Receipt)
		}
	})
}

func TestReceiptEncode(t *testing.T) {
	t.Run("Enc.FirstLogIndexWithinBlock", func(t *testing.T) {
		r1 := &ReceiptForStorage{FirstLogIndexWithinBlock: 1}
		buf, err := rlp.EncodeToBytes(r1)
		require.NoError(t, err)
		r2 := &ReceiptForStorage{}
		err = rlp.DecodeBytes(buf, r2)
		require.NoError(t, err)
		require.Equal(t, r1.FirstLogIndexWithinBlock, r2.FirstLogIndexWithinBlock)
	})

	t.Run("Enc.Empty.FirstLogIndexWithinBlock", func(t *testing.T) {
		r1 := &ReceiptForStorage{Logs: Logs{
			&Log{Index: 1},
		}}
		buf, err := rlp.EncodeToBytes(r1)
		require.NoError(t, err)
		r2 := &ReceiptForStorage{}
		err = rlp.DecodeBytes(buf, r2)
		require.NoError(t, err)
		require.Equal(t, int(r1.Logs[0].Index), int(r2.FirstLogIndexWithinBlock))
	})

	t.Run("Enc.EmptyLogs", func(t *testing.T) {
		r1 := &ReceiptForStorage{FirstLogIndexWithinBlock: 1,
			Logs: Logs{
				&Log{Index: 1},
			},
		}
		buf, err := rlp.EncodeToBytes(r1)
		require.NoError(t, err)
		r2 := &ReceiptForStorage{}
		err = rlp.DecodeBytes(buf, r2)
		require.NoError(t, err)
		require.Equal(t, r1.FirstLogIndexWithinBlock, r2.FirstLogIndexWithinBlock)
	})
	t.Run("Enc.List", func(t *testing.T) {
		r1 := &ReceiptForStorage{FirstLogIndexWithinBlock: 1}
		for i := 0; i < 13; i++ {
			r1.Logs = append(r1.Logs, &Log{Topics: make([]common.Hash, 300)})
		}

		buf, err := rlp.EncodeToBytes(r1)
		require.NoError(t, err)
		r2 := &ReceiptForStorage{}
		err = rlp.DecodeBytes(buf, r2)
		require.NoError(t, err)
		require.Equal(t, r1.FirstLogIndexWithinBlock, r2.FirstLogIndexWithinBlock)
		require.Equal(t, len(r1.Logs), len(r2.Logs))
		require.Equal(t, len(r1.Logs[0].Topics), len(r2.Logs[0].Topics))
	})
}
