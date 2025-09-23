// Copyright 2024 The Erigon Authors
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
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/execution/types"
)

const BorTxKeyPrefix string = "matic-bor-receipt-"

// BorReceiptKey =  num (uint64 big endian)
func BorReceiptKey(number uint64) []byte {
	return dbutils.EncodeBlockNumber(number)
}

// ComputeBorTxHash get derived txn hash from block number and hash
func ComputeBorTxHash(blockNumber uint64, blockHash common.Hash) common.Hash {
	txKeyPlain := make([]byte, 0, len(BorTxKeyPrefix)+8+32)
	txKeyPlain = append(txKeyPlain, BorTxKeyPrefix...)
	txKeyPlain = append(txKeyPlain, BorReceiptKey(blockNumber)...)
	txKeyPlain = append(txKeyPlain, blockHash.Bytes()...)
	return common.BytesToHash(crypto.Keccak256(txKeyPlain))
}

// NewBorTransaction create new bor transaction for bor receipt
func NewBorTransaction() *types.LegacyTx {
	return types.NewTransaction(0, common.Address{}, uint256.NewInt(0), 0, uint256.NewInt(0), make([]byte, 0))
}

// DeriveFieldsForBorReceipt fills the receipts with their computed fields based on consensus
// data and contextual infos like containing block and transactions.
func DeriveFieldsForBorReceipt(receipt *types.Receipt, blockHash common.Hash, blockNumber uint64, receipts types.Receipts) {
	txHash := ComputeBorTxHash(blockNumber, blockHash)
	txIndex := uint(len(receipts))

	// set txn hash and txn index
	receipt.TxHash = txHash
	receipt.TransactionIndex = txIndex
	receipt.BlockHash = blockHash
	receipt.BlockNumber = big.NewInt(0).SetUint64(blockNumber)

	logIndex := 0
	for i := 0; i < len(receipts); i++ {
		logIndex += len(receipts[i].Logs)
	}

	// The derived log fields can simply be set from the block and transaction
	for j := 0; j < len(receipt.Logs); j++ {
		receipt.Logs[j].BlockNumber = blockNumber
		receipt.Logs[j].BlockHash = blockHash
		receipt.Logs[j].TxHash = txHash
		receipt.Logs[j].TxIndex = txIndex
		receipt.Logs[j].Index = uint(logIndex)
		logIndex++
	}
}
