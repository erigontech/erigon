// Copyright 2018 The go-ethereum Authors
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

package rawdb

import (
	"math/big"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
)

// TxLookupEntry is a positional metadata to help looking up the data content of
// a transaction or receipt given only its hash.
type TxLookupEntry struct {
	BlockHash  common.Hash
	BlockIndex uint64
	Index      uint64
}

// ReadTxLookupEntry retrieves the positional metadata associated with a transaction
// hash to allow retrieving the transaction or receipt by hash.
func ReadTxLookupEntry(db ethdb.Tx, txnHash common.Hash) *uint64 {
	data, _ := db.GetOne(dbutils.TxLookupPrefix, txnHash.Bytes())
	if len(data) == 0 {
		return nil
	}
	number := new(big.Int).SetBytes(data).Uint64()
	return &number
}

// WriteTxLookupEntries stores a positional metadata for every transaction from
// a block, enabling hash based transaction and receipt lookups.
func WriteTxLookupEntries(db ethdb.Putter, block *types.Block) {
	for _, tx := range block.Transactions() {
		data := block.Number().Bytes()
		if err := db.Put(dbutils.TxLookupPrefix, tx.Hash().Bytes(), data); err != nil {
			log.Crit("Failed to store transaction lookup entry", "err", err)
		}
	}
}

// DeleteTxLookupEntry removes all transaction data associated with a hash.
func DeleteTxLookupEntry(db ethdb.Deleter, hash common.Hash) error {
	return db.Delete(dbutils.TxLookupPrefix, hash.Bytes(), nil)
}

// ReadTransaction retrieves a specific transaction from the database, along with
// its added positional metadata.
func ReadTransaction(db ethdb.Tx, hash common.Hash) (types.Transaction, common.Hash, uint64, uint64) {
	blockNumber := ReadTxLookupEntry(db, hash)
	if blockNumber == nil {
		return nil, common.Hash{}, 0, 0
	}
	blockHash, err := ReadCanonicalHash(db, *blockNumber)
	if err != nil {
		log.Error("ReadCanonicalHash failed", "err", err)
		return nil, common.Hash{}, 0, 0
	}
	if blockHash == (common.Hash{}) {
		return nil, common.Hash{}, 0, 0
	}
	body := ReadBody(db, blockHash, *blockNumber)
	if body == nil {
		log.Error("Transaction referenced missing", "number", blockNumber, "hash", blockHash)
		return nil, common.Hash{}, 0, 0
	}
	senders, err1 := ReadSenders(db, blockHash, *blockNumber)
	if err1 != nil {
		log.Error("ReadSenders failed", "err", err)
		return nil, common.Hash{}, 0, 0
	}
	body.SendersToTxs(senders)
	for txIndex, tx := range body.Transactions {
		if tx.Hash() == hash {
			return tx, blockHash, *blockNumber, uint64(txIndex)
		}
	}
	log.Error("Transaction not found", "number", blockNumber, "hash", blockHash, "txhash", hash)
	return nil, common.Hash{}, 0, 0
}

// its added positional metadata.
func ReadReceipt(db ethdb.Tx, txHash common.Hash) (*types.Receipt, common.Hash, uint64, uint64) {
	// Retrieve the context of the receipt based on the transaction hash
	blockNumber := ReadTxLookupEntry(db, txHash)
	if blockNumber == nil {
		return nil, common.Hash{}, 0, 0
	}
	blockHash, err := ReadCanonicalHash(db, *blockNumber)
	if err != nil {
		log.Error("ReadCanonicalHash failed", "err", err)
		return nil, common.Hash{}, 0, 0
	}
	if blockHash == (common.Hash{}) {
		return nil, common.Hash{}, 0, 0
	}
	b, senders, err := ReadBlockWithSenders(db, blockHash, *blockNumber)
	if err != nil {
		log.Error("ReadReceipt", "err", err, "number", blockNumber, "hash", blockHash, "txhash", txHash)
		return nil, common.Hash{}, 0, 0
	}
	// Read all the receipts from the block and return the one with the matching hash
	receipts := ReadReceipts(db, b, senders)
	for receiptIndex, receipt := range receipts {
		if receipt.TxHash == txHash {
			return receipt, blockHash, *blockNumber, uint64(receiptIndex)
		}
	}
	log.Error("Receipt not found", "number", blockNumber, "hash", blockHash, "txhash", txHash)
	return nil, common.Hash{}, 0, 0
}

// ReadBloomBits retrieves the compressed bloom bit vector belonging to the given
// section and bit index from the.
func ReadBloomBits(db ethdb.DatabaseReader, bit uint, section uint64, head common.Hash) ([]byte, error) {
	return db.Get(dbutils.BloomBitsPrefix, dbutils.BloomBitsKey(bit, section, head))
}

// WriteBloomBits stores the compressed bloom bits vector belonging to the given
// section and bit index.
func WriteBloomBits(db ethdb.Putter, bit uint, section uint64, head common.Hash, bits []byte) {
	if err := db.Put(dbutils.BloomBitsPrefix, dbutils.BloomBitsKey(bit, section, head), bits); err != nil {
		log.Crit("Failed to store bloom bits", "err", err)
	}
}
