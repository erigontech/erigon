package types

import (
	"math/big"
	"sort"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/crypto"
)

const BorTxKeyPrefix string = "matic-bor-receipt-"

// BorReceiptKey =  num (uint64 big endian)
func BorReceiptKey(number uint64) []byte {
	return dbutils.EncodeBlockNumber(number)
}

// ComputeBorTxHash get derived tx hash from block number and hash
func ComputeBorTxHash(blockNumber uint64, blockHash libcommon.Hash) libcommon.Hash {
	txKeyPlain := make([]byte, 0, len(BorTxKeyPrefix)+8+32)
	txKeyPlain = append(txKeyPlain, BorTxKeyPrefix...)
	txKeyPlain = append(txKeyPlain, BorReceiptKey(blockNumber)...)
	txKeyPlain = append(txKeyPlain, blockHash.Bytes()...)
	return libcommon.BytesToHash(crypto.Keccak256(txKeyPlain))
}

// NewBorTransaction create new bor transaction for bor receipt
func NewBorTransaction() *LegacyTx {
	return NewTransaction(0, libcommon.Address{}, uint256.NewInt(0), 0, uint256.NewInt(0), make([]byte, 0))
}

// DeriveFieldsForBorReceipt fills the receipts with their computed fields based on consensus
// data and contextual infos like containing block and transactions.
func DeriveFieldsForBorReceipt(receipt *Receipt, blockHash libcommon.Hash, blockNumber uint64, receipts Receipts) error {
	txHash := ComputeBorTxHash(blockNumber, blockHash)
	txIndex := uint(len(receipts))

	// set tx hash and tx index
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

	return nil
}

// DeriveFieldsForBorLogs fills the receipts with their computed fields based on consensus
// data and contextual infos like containing block and transactions.
func DeriveFieldsForBorLogs(logs []*Log, blockHash libcommon.Hash, blockNumber uint64, txIndex uint, logIndex uint) {
	txHash := ComputeBorTxHash(blockNumber, blockHash)

	// the derived log fields can simply be set from the block and transaction
	for j := 0; j < len(logs); j++ {
		logs[j].BlockNumber = blockNumber
		logs[j].BlockHash = blockHash
		logs[j].TxHash = txHash
		logs[j].TxIndex = txIndex
		logs[j].Index = logIndex
		logIndex++
	}
}

// MergeBorLogs merges receipt logs and block receipt logs
func MergeBorLogs(logs []*Log, borLogs []*Log) []*Log {
	result := append(logs, borLogs...)

	sort.SliceStable(result, func(i int, j int) bool {
		if result[i].BlockNumber == result[j].BlockNumber {
			return result[i].Index < result[j].Index
		}
		return result[i].BlockNumber < result[j].BlockNumber
	})

	return result
}
