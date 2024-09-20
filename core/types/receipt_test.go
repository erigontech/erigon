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
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	libcommon "github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/crypto"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rlp"
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

	tx := NewTransaction(1, libcommon.HexToAddress("0x1"), u256.Num1, 1, u256.Num1, nil)
	receipt := &Receipt{
		Status:            ReceiptStatusFailed,
		CumulativeGasUsed: 1,
		Logs: []*Log{
			{
				Address: libcommon.BytesToAddress([]byte{0x11}),
				Topics:  []libcommon.Hash{libcommon.HexToHash("dead"), libcommon.HexToHash("beef")},
				Data:    []byte{0x01, 0x00, 0xff},
				Index:   999,
			},
			{
				Address: libcommon.BytesToAddress([]byte{0x01, 0x11}),
				Topics:  []libcommon.Hash{libcommon.HexToHash("dead"), libcommon.HexToHash("beef")},
				Data:    []byte{0x01, 0x00, 0xff},
				Index:   1000,
			},
		},
		TxHash:          tx.Hash(),
		ContractAddress: libcommon.BytesToAddress([]byte{0x01, 0x11, 0x11}),
		GasUsed:         111111,
	}
	receipt.Bloom = CreateBloom(Receipts{receipt})

	for _, tc := range tests {
		tc := tc
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

// Tests that receipt data can be correctly derived from the contextual infos
func TestDeriveFields(t *testing.T) {
	t.Parallel()
	// Create a few transactions to have receipts for
	to2 := libcommon.HexToAddress("0x2")
	to3 := libcommon.HexToAddress("0x3")
	txs := Transactions{
		&LegacyTx{
			CommonTx: CommonTx{
				Nonce: 1,
				Value: u256.Num1,
				Gas:   1,
			},
			GasPrice: u256.Num1,
		},
		&LegacyTx{
			CommonTx: CommonTx{
				To:    &to2,
				Nonce: 2,
				Value: u256.Num2,
				Gas:   2,
			},
			GasPrice: u256.Num2,
		},
		&AccessListTx{
			LegacyTx: LegacyTx{
				CommonTx: CommonTx{
					To:    &to3,
					Nonce: 3,
					Value: uint256.NewInt(3),
					Gas:   3,
				},
				GasPrice: uint256.NewInt(3),
			},
		},
	}
	// Create the corresponding receipts
	receipts := Receipts{
		&Receipt{
			Status:            ReceiptStatusFailed,
			CumulativeGasUsed: 1,
			Logs: []*Log{
				{Address: libcommon.BytesToAddress([]byte{0x11})},
				{Address: libcommon.BytesToAddress([]byte{0x01, 0x11})},
			},
			TxHash:                   txs[0].Hash(),
			ContractAddress:          libcommon.BytesToAddress([]byte{0x01, 0x11, 0x11}),
			GasUsed:                  1,
			FirstLogIndexWithinBlock: 0,
		},
		&Receipt{
			PostState:         libcommon.Hash{2}.Bytes(),
			CumulativeGasUsed: 3,
			Logs: []*Log{
				{Address: libcommon.BytesToAddress([]byte{0x22})},
				{Address: libcommon.BytesToAddress([]byte{0x02, 0x22})},
			},
			TxHash:                   txs[1].Hash(),
			ContractAddress:          libcommon.BytesToAddress([]byte{0x02, 0x22, 0x22}),
			GasUsed:                  2,
			FirstLogIndexWithinBlock: 2,
		},
		&Receipt{
			Type:              AccessListTxType,
			PostState:         libcommon.Hash{3}.Bytes(),
			CumulativeGasUsed: 6,
			Logs: []*Log{
				{Address: libcommon.BytesToAddress([]byte{0x33})},
				{Address: libcommon.BytesToAddress([]byte{0x03, 0x33})},
			},
			TxHash:                   txs[2].Hash(),
			ContractAddress:          libcommon.BytesToAddress([]byte{0x03, 0x33, 0x33}),
			GasUsed:                  3,
			FirstLogIndexWithinBlock: 4,
		},
	}
	// Clear all the computed fields and re-derive them
	number := big.NewInt(1)
	hash := libcommon.BytesToHash([]byte{0x03, 0x14})

	t.Run("DeriveV1", func(t *testing.T) {
		clearComputedFieldsOnReceipts(t, receipts)
		if err := receipts.DeriveFields(hash, number.Uint64(), txs, []libcommon.Address{libcommon.BytesToAddress([]byte{0x0}), libcommon.BytesToAddress([]byte{0x0}), libcommon.BytesToAddress([]byte{0x0})}); err != nil {
			t.Fatalf("DeriveFields(...) = %v, want <nil>", err)
		}
		// Iterate over all the computed fields and check that they're correct
		signer := MakeSigner(params.TestChainConfig, number.Uint64(), 0)

		logIndex := uint(0)
		for i, r := range receipts {
			if r.Type != txs[i].Type() {
				t.Errorf("receipts[%d].Type = %d, want %d", i, r.Type, txs[i].Type())
			}
			if r.TxHash != txs[i].Hash() {
				t.Errorf("receipts[%d].TxHash = %s, want %s", i, r.TxHash.String(), txs[i].Hash().String())
			}
			if r.BlockHash != hash {
				t.Errorf("receipts[%d].BlockHash = %s, want %s", i, r.BlockHash.String(), hash.String())
			}
			if r.BlockNumber.Cmp(number) != 0 {
				t.Errorf("receipts[%c].BlockNumber = %s, want %s", i, r.BlockNumber.String(), number.String())
			}
			if r.TransactionIndex != uint(i) {
				t.Errorf("receipts[%d].TransactionIndex = %d, want %d", i, r.TransactionIndex, i)
			}
			if r.GasUsed != txs[i].GetGas() {
				t.Errorf("receipts[%d].GasUsed = %d, want %d", i, r.GasUsed, txs[i].GetGas())
			}
			if txs[i].GetTo() != nil && r.ContractAddress != (libcommon.Address{}) {
				t.Errorf("receipts[%d].ContractAddress = %s, want %s", i, r.ContractAddress.String(), (libcommon.Address{}).String())
			}
			from, _ := txs[i].Sender(*signer)
			contractAddress := crypto.CreateAddress(from, txs[i].GetNonce())
			if txs[i].GetTo() == nil && r.ContractAddress != contractAddress {
				t.Errorf("receipts[%d].ContractAddress = %s, want %s", i, r.ContractAddress.String(), contractAddress.String())
			}
			for j := range r.Logs {
				if r.Logs[j].BlockNumber != number.Uint64() {
					t.Errorf("receipts[%d].Logs[%d].BlockNumber = %d, want %d", i, j, r.Logs[j].BlockNumber, number.Uint64())
				}
				if r.Logs[j].BlockHash != hash {
					t.Errorf("receipts[%d].Logs[%d].BlockHash = %s, want %s", i, j, r.Logs[j].BlockHash.String(), hash.String())
				}
				if r.Logs[j].TxHash != txs[i].Hash() {
					t.Errorf("receipts[%d].Logs[%d].TxHash = %s, want %s", i, j, r.Logs[j].TxHash.String(), txs[i].Hash().String())
				}
				if r.Logs[j].TxHash != txs[i].Hash() {
					t.Errorf("receipts[%d].Logs[%d].TxHash = %s, want %s", i, j, r.Logs[j].TxHash.String(), txs[i].Hash().String())
				}
				if r.Logs[j].TxIndex != uint(i) {
					t.Errorf("receipts[%d].Logs[%d].TransactionIndex = %d, want %d", i, j, r.Logs[j].TxIndex, i)
				}
				if r.Logs[j].Index != logIndex {
					t.Errorf("receipts[%d].Logs[%d].Index = %d, want %d", i, j, r.Logs[j].Index, logIndex)
				}
				logIndex++
			}
		}
	})

	//t.Run("DeriveV3", func(t *testing.T) {
	//	clearComputedFieldsOnReceipts(t, receipts)
	//	// Iterate over all the computed fields and check that they're correct
	//	signer := MakeSigner(params.TestChainConfig, number.Uint64(), 0)
	//
	//	logIndex := uint(0)
	//	for i := range receipts {
	//		txs[i].SetSender(libcommon.BytesToAddress([]byte{0x0}))
	//		r, err := receipts.DeriveFieldsV3ForSingleReceipt(i, hash, number.Uint64(), txs[i])
	//		if err != nil {
	//			panic(err)
	//		}
	//
	//		if r.Type != txs[i].Type() {
	//			t.Errorf("receipts[%d].Type = %d, want %d", i, r.Type, txs[i].Type())
	//		}
	//		if r.TxHash != txs[i].Hash() {
	//			t.Errorf("receipts[%d].TxHash = %s, want %s", i, r.TxHash.String(), txs[i].Hash().String())
	//		}
	//		if r.BlockHash != hash {
	//			t.Errorf("receipts[%d].BlockHash = %s, want %s", i, r.BlockHash.String(), hash.String())
	//		}
	//		if r.BlockNumber.Cmp(number) != 0 {
	//			t.Errorf("receipts[%c].BlockNumber = %s, want %s", i, r.BlockNumber.String(), number.String())
	//		}
	//		if r.TransactionIndex != uint(i) {
	//			t.Errorf("receipts[%d].TransactionIndex = %d, want %d", i, r.TransactionIndex, i)
	//		}
	//		if r.GasUsed != txs[i].GetGas() {
	//			t.Errorf("receipts[%d].GasUsed = %d, want %d", i, r.GasUsed, txs[i].GetGas())
	//		}
	//		if txs[i].GetTo() != nil && r.ContractAddress != (libcommon.Address{}) {
	//			t.Errorf("receipts[%d].ContractAddress = %s, want %s", i, r.ContractAddress.String(), (libcommon.Address{}).String())
	//		}
	//		from, _ := txs[i].Sender(*signer)
	//		contractAddress := crypto.CreateAddress(from, txs[i].GetNonce())
	//		if txs[i].GetTo() == nil && r.ContractAddress != contractAddress {
	//			t.Errorf("receipts[%d].ContractAddress = %s, want %s", i, r.ContractAddress.String(), contractAddress.String())
	//		}
	//		for j := range r.Logs {
	//			if r.Logs[j].BlockNumber != number.Uint64() {
	//				t.Errorf("receipts[%d].Logs[%d].BlockNumber = %d, want %d", i, j, r.Logs[j].BlockNumber, number.Uint64())
	//			}
	//			if r.Logs[j].BlockHash != hash {
	//				t.Errorf("receipts[%d].Logs[%d].BlockHash = %s, want %s", i, j, r.Logs[j].BlockHash.String(), hash.String())
	//			}
	//			if r.Logs[j].TxHash != txs[i].Hash() {
	//				t.Errorf("receipts[%d].Logs[%d].TxHash = %s, want %s", i, j, r.Logs[j].TxHash.String(), txs[i].Hash().String())
	//			}
	//			if r.Logs[j].TxHash != txs[i].Hash() {
	//				t.Errorf("receipts[%d].Logs[%d].TxHash = %s, want %s", i, j, r.Logs[j].TxHash.String(), txs[i].Hash().String())
	//			}
	//			if r.Logs[j].TxIndex != uint(i) {
	//				t.Errorf("receipts[%d].Logs[%d].TransactionIndex = %d, want %d", i, j, r.Logs[j].TxIndex, i)
	//			}
	//			if r.Logs[j].Index != logIndex {
	//				t.Errorf("receipts[%d].Logs[%d].Index = %d, want %d", i, j, r.Logs[j].Index, logIndex)
	//			}
	//			logIndex++
	//		}
	//	}
	//})

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

	receipt.TxHash = libcommon.Hash{}
	receipt.BlockHash = libcommon.Hash{}
	receipt.BlockNumber = big.NewInt(math.MaxUint32)
	receipt.TransactionIndex = math.MaxUint32
	receipt.ContractAddress = libcommon.Address{}
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
	log.BlockHash = libcommon.Hash{}
	log.TxHash = libcommon.Hash{}
	log.TxIndex = math.MaxUint32
	log.Index = math.MaxUint32
}
