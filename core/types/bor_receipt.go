package types

import (
	"math/big"
	"sort"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/crypto"
)

// TenToTheFive - To be used while sorting bor logs
//
// Sorted using ( blockNumber * (10 ** 5) + logIndex )
const TenToTheFive uint64 = 100000

var (
	// SystemAddress address for system sender
	SystemAddress = common.HexToAddress("0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE")
)

// BorReceiptKey =  num (uint64 big endian)
func BorReceiptKey(number uint64) []byte {
	return dbutils.EncodeBlockNumber(number)
}

// GetDerivedBorTxHash get derived tx hash from receipt key
func GetDerivedBorTxHash(receiptKey []byte) common.Hash {
	return common.BytesToHash(crypto.Keccak256(receiptKey))
}

// NewBorTransaction create new bor transaction for bor receipt
func NewBorTransaction() *LegacyTx {
	return NewTransaction(0, common.Address{}, uint256.NewInt(0), 0, uint256.NewInt(0), make([]byte, 0))
}

// DeriveFieldsForBorReceipt fills the receipts with their computed fields based on consensus
// data and contextual infos like containing block and transactions.
func DeriveFieldsForBorReceipt(receipt *Receipt, hash common.Hash, number uint64, receipts Receipts) error {
	// get derived tx hash
	borPrefix := []byte("matic-bor-receipt-")
	// hashing using prefix + number + hash
	txHash := GetDerivedBorTxHash((append(borPrefix, append(BorReceiptKey(number), hash.Bytes()...)...)))
	txIndex := uint(len(receipts))

	// set tx hash and tx index
	receipt.TxHash = txHash
	receipt.TransactionIndex = txIndex
	receipt.BlockHash = hash
	receipt.BlockNumber = big.NewInt(0).SetUint64(number)

	logIndex := 0
	for i := 0; i < len(receipts); i++ {
		logIndex += len(receipts[i].Logs)
	}

	// The derived log fields can simply be set from the block and transaction
	for j := 0; j < len(receipt.Logs); j++ {
		receipt.Logs[j].BlockNumber = number
		receipt.Logs[j].BlockHash = hash
		receipt.Logs[j].TxHash = txHash
		receipt.Logs[j].TxIndex = txIndex
		receipt.Logs[j].Index = uint(logIndex)
		logIndex++
	}
	return nil
}

// DeriveFieldsForBorLogs fills the receipts with their computed fields based on consensus
// data and contextual infos like containing block and transactions.
func DeriveFieldsForBorLogs(logs []*Log, hash common.Hash, number uint64, txIndex uint, logIndex uint) {
	// get derived tx hash
	txHash := GetDerivedBorTxHash(BorReceiptKey(number))

	// the derived log fields can simply be set from the block and transaction
	for j := 0; j < len(logs); j++ {
		logs[j].BlockNumber = number
		logs[j].BlockHash = hash
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
		return (result[i].BlockNumber*TenToTheFive + uint64(result[i].Index)) < (result[j].BlockNumber*TenToTheFive + uint64(result[j].Index))
	})

	return result
}
