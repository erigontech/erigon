// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package types

import (
	"math"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
)

// Tests that receipt data can be correctly derived from the contextual infos
func TestDeriveFields_zkEvm_preForkId8(t *testing.T) {
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
			TxHash:          txs[0].Hash(),
			ContractAddress: libcommon.BytesToAddress([]byte{0x01, 0x11, 0x11}),
			GasUsed:         1,
		},
		&Receipt{
			PostState:         libcommon.Hash{2}.Bytes(),
			CumulativeGasUsed: 2,
			Logs: []*Log{
				{Address: libcommon.BytesToAddress([]byte{0x22})},
				{Address: libcommon.BytesToAddress([]byte{0x02, 0x22})},
			},
			TxHash:          txs[1].Hash(),
			ContractAddress: libcommon.BytesToAddress([]byte{0x02, 0x22, 0x22}),
			GasUsed:         2,
		},
		&Receipt{
			Type:              AccessListTxType,
			PostState:         libcommon.Hash{3}.Bytes(),
			CumulativeGasUsed: 3,
			Logs: []*Log{
				{Address: libcommon.BytesToAddress([]byte{0x33})},
				{Address: libcommon.BytesToAddress([]byte{0x03, 0x33})},
			},
			TxHash:          txs[2].Hash(),
			ContractAddress: libcommon.BytesToAddress([]byte{0x03, 0x33, 0x33}),
			GasUsed:         3,
		},
	}
	// Clear all the computed fields and re-derive them
	number := big.NewInt(1)
	hash := libcommon.BytesToHash([]byte{0x03, 0x14})

	clearComputedFieldsOnReceipts(t, receipts)
	if err := receipts.DeriveFields_zkEvm(math.MaxUint64, hash, number.Uint64(), txs, []libcommon.Address{libcommon.BytesToAddress([]byte{0x0}), libcommon.BytesToAddress([]byte{0x0}), libcommon.BytesToAddress([]byte{0x0})}); err != nil {
		t.Fatalf("DeriveFields_zkEvm(...) = %v, want <nil>", err)
	}
	// Iterate over all the computed fields and check that they're correct
	signer := MakeSigner(params.TestChainConfig, number.Uint64(), 0)

	logIndex := uint(0)
	for i := range receipts {
		if receipts[i].Type != txs[i].Type() {
			t.Errorf("receipts[%d].Type = %d, want %d", i, receipts[i].Type, txs[i].Type())
		}
		if receipts[i].TxHash != txs[i].Hash() {
			t.Errorf("receipts[%d].TxHash = %s, want %s", i, receipts[i].TxHash.String(), txs[i].Hash().String())
		}
		if receipts[i].BlockHash != hash {
			t.Errorf("receipts[%d].BlockHash = %s, want %s", i, receipts[i].BlockHash.String(), hash.String())
		}
		if receipts[i].BlockNumber.Cmp(number) != 0 {
			t.Errorf("receipts[%c].BlockNumber = %s, want %s", i, receipts[i].BlockNumber.String(), number.String())
		}
		if receipts[i].TransactionIndex != uint(i) {
			t.Errorf("receipts[%d].TransactionIndex = %d, want %d", i, receipts[i].TransactionIndex, i)
		}
		if receipts[i].GasUsed != txs[i].GetGas() {
			t.Errorf("receipts[%d].GasUsed = %d, want %d", i, receipts[i].GasUsed, txs[i].GetGas())
		}
		if txs[i].GetTo() != nil && receipts[i].ContractAddress != (libcommon.Address{}) {
			t.Errorf("receipts[%d].ContractAddress = %s, want %s", i, receipts[i].ContractAddress.String(), (libcommon.Address{}).String())
		}
		from, _ := txs[i].Sender(*signer)
		contractAddress := crypto.CreateAddress(from, txs[i].GetNonce())
		if txs[i].GetTo() == nil && receipts[i].ContractAddress != contractAddress {
			t.Errorf("receipts[%d].ContractAddress = %s, want %s", i, receipts[i].ContractAddress.String(), contractAddress.String())
		}

		for j := range receipts[i].Logs {
			if receipts[i].Logs[j].BlockNumber != number.Uint64() {
				t.Errorf("receipts[%d].Logs[%d].BlockNumber = %d, want %d", i, j, receipts[i].Logs[j].BlockNumber, number.Uint64())
			}
			if receipts[i].Logs[j].BlockHash != hash {
				t.Errorf("receipts[%d].Logs[%d].BlockHash = %s, want %s", i, j, receipts[i].Logs[j].BlockHash.String(), hash.String())
			}
			if receipts[i].Logs[j].TxHash != txs[i].Hash() {
				t.Errorf("receipts[%d].Logs[%d].TxHash = %s, want %s", i, j, receipts[i].Logs[j].TxHash.String(), txs[i].Hash().String())
			}
			if receipts[i].Logs[j].TxHash != txs[i].Hash() {
				t.Errorf("receipts[%d].Logs[%d].TxHash = %s, want %s", i, j, receipts[i].Logs[j].TxHash.String(), txs[i].Hash().String())
			}
			if receipts[i].Logs[j].TxIndex != uint(i) {
				t.Errorf("receipts[%d].Logs[%d].TransactionIndex = %d, want %d", i, j, receipts[i].Logs[j].TxIndex, i)
			}
			if receipts[i].Logs[j].Index != logIndex {
				t.Errorf("receipts[%d].Logs[%d].Index = %d, want %d", i, j, receipts[i].Logs[j].Index, logIndex)
			}
			logIndex++
		}
	}
}

// Tests that receipt data can be correctly derived from the contextual infos
func TestDeriveFields_zkEvmForkid8(t *testing.T) {
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
			TxHash:          txs[0].Hash(),
			ContractAddress: libcommon.BytesToAddress([]byte{0x01, 0x11, 0x11}),
			GasUsed:         1,
		},
		&Receipt{
			PostState:         libcommon.Hash{2}.Bytes(),
			CumulativeGasUsed: 3,
			Logs: []*Log{
				{Address: libcommon.BytesToAddress([]byte{0x22})},
				{Address: libcommon.BytesToAddress([]byte{0x02, 0x22})},
			},
			TxHash:          txs[1].Hash(),
			ContractAddress: libcommon.BytesToAddress([]byte{0x02, 0x22, 0x22}),
			GasUsed:         2,
		},
		&Receipt{
			Type:              AccessListTxType,
			PostState:         libcommon.Hash{3}.Bytes(),
			CumulativeGasUsed: 6,
			Logs: []*Log{
				{Address: libcommon.BytesToAddress([]byte{0x33})},
				{Address: libcommon.BytesToAddress([]byte{0x03, 0x33})},
			},
			TxHash:          txs[2].Hash(),
			ContractAddress: libcommon.BytesToAddress([]byte{0x03, 0x33, 0x33}),
			GasUsed:         3,
		},
	}
	// Clear all the computed fields and re-derive them
	number := big.NewInt(100)
	hash := libcommon.BytesToHash([]byte{0x03, 0x14})

	clearComputedFieldsOnReceipts(t, receipts)
	if err := receipts.DeriveFields_zkEvm(1, hash, number.Uint64(), txs, []libcommon.Address{libcommon.BytesToAddress([]byte{0x0}), libcommon.BytesToAddress([]byte{0x0}), libcommon.BytesToAddress([]byte{0x0})}); err != nil {
		t.Fatalf("DeriveFields_zkEvm(...) = %v, want <nil>", err)
	}
	// Iterate over all the computed fields and check that they're correct
	signer := MakeSigner(params.TestChainConfig, number.Uint64(), 0)

	logIndex := uint(0)
	for i := range receipts {
		if receipts[i].Type != txs[i].Type() {
			t.Errorf("receipts[%d].Type = %d, want %d", i, receipts[i].Type, txs[i].Type())
		}
		if receipts[i].TxHash != txs[i].Hash() {
			t.Errorf("receipts[%d].TxHash = %s, want %s", i, receipts[i].TxHash.String(), txs[i].Hash().String())
		}
		if receipts[i].BlockHash != hash {
			t.Errorf("receipts[%d].BlockHash = %s, want %s", i, receipts[i].BlockHash.String(), hash.String())
		}
		if receipts[i].BlockNumber.Cmp(number) != 0 {
			t.Errorf("receipts[%c].BlockNumber = %s, want %s", i, receipts[i].BlockNumber.String(), number.String())
		}
		if receipts[i].TransactionIndex != uint(i) {
			t.Errorf("receipts[%d].TransactionIndex = %d, want %d", i, receipts[i].TransactionIndex, i)
		}
		if receipts[i].GasUsed != txs[i].GetGas() {
			t.Errorf("receipts[%d].GasUsed = %d, want %d", i, receipts[i].GasUsed, txs[i].GetGas())
		}
		if txs[i].GetTo() != nil && receipts[i].ContractAddress != (libcommon.Address{}) {
			t.Errorf("receipts[%d].ContractAddress = %s, want %s", i, receipts[i].ContractAddress.String(), (libcommon.Address{}).String())
		}
		from, _ := txs[i].Sender(*signer)
		contractAddress := crypto.CreateAddress(from, txs[i].GetNonce())
		if txs[i].GetTo() == nil && receipts[i].ContractAddress != contractAddress {
			t.Errorf("receipts[%d].ContractAddress = %s, want %s", i, receipts[i].ContractAddress.String(), contractAddress.String())
		}

		for j := range receipts[i].Logs {
			if receipts[i].Logs[j].BlockNumber != number.Uint64() {
				t.Errorf("receipts[%d].Logs[%d].BlockNumber = %d, want %d", i, j, receipts[i].Logs[j].BlockNumber, number.Uint64())
			}
			if receipts[i].Logs[j].BlockHash != hash {
				t.Errorf("receipts[%d].Logs[%d].BlockHash = %s, want %s", i, j, receipts[i].Logs[j].BlockHash.String(), hash.String())
			}
			if receipts[i].Logs[j].TxHash != txs[i].Hash() {
				t.Errorf("receipts[%d].Logs[%d].TxHash = %s, want %s", i, j, receipts[i].Logs[j].TxHash.String(), txs[i].Hash().String())
			}
			if receipts[i].Logs[j].TxHash != txs[i].Hash() {
				t.Errorf("receipts[%d].Logs[%d].TxHash = %s, want %s", i, j, receipts[i].Logs[j].TxHash.String(), txs[i].Hash().String())
			}
			if receipts[i].Logs[j].TxIndex != uint(i) {
				t.Errorf("receipts[%d].Logs[%d].TransactionIndex = %d, want %d", i, j, receipts[i].Logs[j].TxIndex, i)
			}
			if receipts[i].Logs[j].Index != logIndex {
				t.Errorf("receipts[%d].Logs[%d].Index = %d, want %d", i, j, receipts[i].Logs[j].Index, logIndex)
			}
			logIndex++
		}
	}
}
