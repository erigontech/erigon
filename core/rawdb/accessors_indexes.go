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
// MERCHANTABILITY or FITNESS fFOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rawdb

import (
	"bytes"
	"encoding/binary"
	"math/big"
	"math/bits"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

// TxLookupEntry is a positional metadata to help looking up the data content of
// a transaction or receipt given only its hash.
type TxLookupEntry struct {
	BlockHash  common.Hash
	BlockIndex uint64
	Index      uint64
}

var memTxLookupEntries []uint64

// ReadTxLookupEntry retrieves the positional metadata associated with a transaction
// hash to allow retrieving the transaction or receipt by hash.
func ReadTxLookupEntry(db DatabaseReader, hash common.Hash) *uint64 {
	data, _ := db.Get(dbutils.TxLookupPrefix, hash.Bytes())
	if len(data) == 0 {
		return nil
	}
	number := new(big.Int).SetBytes(data).Uint64()
	return &number
}

// WriteTxLookupEntries stores a positional metadata for every transaction from
// a block, enabling hash based transaction and receipt lookups.
func WriteTxLookupEntriesInMemory(block *types.Block) {
	blockNumber := block.Number().Bytes()
	for txIndex, tx := range block.Transactions() {
		entry := make([]byte, 8)
		copy(entry[6:], tx.Hash().Bytes())
		copy(entry[(6-len(blockNumber)):], blockNumber)
		tdxBytes := uintToBytes(uint64(txIndex))
		copy(entry[2-len(tdxBytes):], tdxBytes)
		memTxLookupEntries = append(memTxLookupEntries, binary.LittleEndian.Uint64(entry))
	}
}

func WriteTxLookupEntries(db ethdb.DbWithPendingMutations) {
	var sets []uint64
	var prev []byte
	// sort the array
	sort.Slice(memTxLookupEntries, func(i, j int) bool {
		return memTxLookupEntries[i] < memTxLookupEntries[j]
	})
	for i, lookup := range memTxLookupEntries {
		entry := make([]byte, 8)
		binary.LittleEndian.PutUint64(entry, lookup)
		blockNumber := bytesToUint64(entry[2:6])
		tdx := int(bytesToUint64(entry[:2]))
		blockHash := ReadCanonicalHash(db, blockNumber)
		body := ReadBody(db, blockHash, blockNumber)
		var txHash []byte
		if body.Transactions == nil {
			log.Warn("Lookup: Block not found, Skipping.")
			continue
		}
		// Get Transaction hash from index and block number
		for txIndex, tx := range body.Transactions {
			if txIndex == tdx {
				txHash = tx.Hash().Bytes()
				break
			}
		}
		if txHash == nil {
			log.Warn("Lookup: Hash not found, Skipping.")
			continue
		}
		if prev == nil && i != len(memTxLookupEntries)-1 {
			prev = entry[6:]
			copy(entry[6:], txHash[2:])
			sets = []uint64{binary.LittleEndian.Uint64(entry)}
		} else if bytes.Equal(entry[6:], prev) && i != len(memTxLookupEntries)-1 {
			copy(entry[6:], txHash[2:])
			sets = append(sets, binary.LittleEndian.Uint64(entry))
		} else if i == len(memTxLookupEntries)-1 {
			if bytes.Equal(entry[6:], prev) {
				copy(entry[6:], txHash[2:])
				sets = append(sets, binary.LittleEndian.Uint64(entry))
				insertLookupSet(db, sets)
			}
			insertLookupSet(db, sets)
			insertLookupSet(db, []uint64{binary.LittleEndian.Uint64(entry)})
			break
		} else {
			prev = entry[6:]
			copy(entry[6:], txHash[2:])
			insertLookupSet(db, sets)
			sets = []uint64{binary.LittleEndian.Uint64(entry)}
		}
	}
	memTxLookupEntries = []uint64{}
}

func ResetLookupEntries() {
	memTxLookupEntries = nil
}

// DeleteTxLookupEntry removes all transaction data associated with a hash.
func DeleteTxLookupEntry(db DatabaseDeleter, hash common.Hash) error {
	return db.Delete(dbutils.TxLookupPrefix, hash.Bytes())
}

// ReadTransaction retrieves a specific transaction from the database, along with
// its added positional metadata.
func ReadTransaction(db DatabaseReader, hash common.Hash) (*types.Transaction, common.Hash, uint64, uint64) {
	blockNumber := ReadTxLookupEntry(db, hash)
	if blockNumber == nil {
		return nil, common.Hash{}, 0, 0
	}
	blockHash := ReadCanonicalHash(db, *blockNumber)
	if blockHash == (common.Hash{}) {
		return nil, common.Hash{}, 0, 0
	}
	body := ReadBody(db, blockHash, *blockNumber)
	if body == nil {
		log.Error("Transaction referenced missing", "number", blockNumber, "hash", blockHash)
		return nil, common.Hash{}, 0, 0
	}
	for txIndex, tx := range body.Transactions {
		if tx.Hash() == hash {
			return tx, blockHash, *blockNumber, uint64(txIndex)
		}
	}
	log.Error("Transaction not found", "number", blockNumber, "hash", blockHash, "txhash", hash)
	return nil, common.Hash{}, 0, 0
}

// ReadReceipt retrieves a specific transaction receipt from the database, along with
// its added positional metadata.
func ReadReceipt(db DatabaseReader, hash common.Hash, config *params.ChainConfig) (*types.Receipt, common.Hash, uint64, uint64) {
	// Retrieve the context of the receipt based on the transaction hash
	blockNumber := ReadTxLookupEntry(db, hash)
	if blockNumber == nil {
		return nil, common.Hash{}, 0, 0
	}
	blockHash := ReadCanonicalHash(db, *blockNumber)
	if blockHash == (common.Hash{}) {
		return nil, common.Hash{}, 0, 0
	}
	// Read all the receipts from the block and return the one with the matching hash
	receipts := ReadReceipts(db, blockHash, *blockNumber, config)
	for receiptIndex, receipt := range receipts {
		if receipt.TxHash == hash {
			return receipt, blockHash, *blockNumber, uint64(receiptIndex)
		}
	}
	log.Error("Receipt not found", "number", blockNumber, "hash", blockHash, "txhash", hash)
	return nil, common.Hash{}, 0, 0
}

// ReadBloomBits retrieves the compressed bloom bit vector belonging to the given
// section and bit index from the.
func ReadBloomBits(db DatabaseReader, bit uint, section uint64, head common.Hash) ([]byte, error) {
	return db.Get(dbutils.BloomBitsPrefix, dbutils.BloomBitsKey(bit, section, head))
}

// WriteBloomBits stores the compressed bloom bits vector belonging to the given
// section and bit index.
func WriteBloomBits(db DatabaseWriter, bit uint, section uint64, head common.Hash, bits []byte) {
	if err := db.Put(dbutils.BloomBitsPrefix, dbutils.BloomBitsKey(bit, section, head), bits); err != nil {
		log.Crit("Failed to store bloom bits", "err", err)
	}
}

func insertLookupSet(db ethdb.DbWithPendingMutations, sets []uint64) {
	//Perform quicksort of sets and block numbers
	sort.Slice(sets, func(i, j int) bool {
		return sets[i] < sets[j]
	})
	// Commit Lookups
	for _, set := range sets {
		entry := make([]byte, 8)
		binary.LittleEndian.PutUint64(entry, set)
		tdx := bytesToUint64(entry[:2])
		blockNumber := bytesToUint64(entry[2:6])
		blockHash := ReadCanonicalHash(db, blockNumber)
		body := ReadBody(db, blockHash, blockNumber)
		var txHash []byte
		for txIndex, tx := range body.Transactions {
			if txIndex == int(tdx) {
				txHash = tx.Hash().Bytes()
				break
			}
		}
		if err := db.Put(dbutils.TxLookupPrefix, txHash, uintToBytes(blockNumber)); err != nil {
			log.Crit("Failed to store transaction lookup entry", "err", err)
		}
	}
}

func uintToBytes(x uint64) []byte {
	nBytes := (bits.Len64(x) + 7) / 8
	res := make([]byte, nBytes)
	for i := nBytes; i > 0; i-- {
		res[i-1] = byte(x)
		x >>= 8
	}
	return res
}

func bytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}
