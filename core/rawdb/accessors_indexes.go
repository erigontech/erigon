// Copyright 2018 The go-ethereum Authors
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

package rawdb

import (
	"encoding/binary"
	"math/big"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/core/types"
)

// TxLookupEntry is a positional metadata to help looking up the data content of
// a transaction or receipt given only its hash.
type TxLookupEntry struct {
	BlockHash  libcommon.Hash
	BlockIndex uint64
	Index      uint64
}

// ReadTxLookupEntry retrieves the positional metadata associated with a transaction
// hash to allow retrieving the transaction or receipt by hash.
func ReadTxLookupEntry(db kv.Getter, txnHash libcommon.Hash) (*uint64, error) {
	dataBlockNum, err := db.GetOne(kv.TxLookup, txnHash.Bytes())
	if err != nil {
		return nil, err
	}
	if len(dataBlockNum) == 0 {
		return nil, nil
	}
	numberBlockNum := new(big.Int).SetBytes(dataBlockNum).Uint64()
	return &numberBlockNum, nil
}

// WriteTxLookupEntries stores a positional metadata for every transaction from
// a block, enabling hash based transaction and receipt lookups.
func WriteTxLookupEntries(db kv.Putter, block *types.Block) {
	for _, txn := range block.Transactions() {
		data := block.Number().Bytes()
		if err := db.Put(kv.TxLookup, txn.Hash().Bytes(), data); err != nil {
			log.Crit("Failed to store transaction lookup entry", "err", err)
		}
	}
}

// DeleteTxLookupEntry removes all transaction data associated with a hash.
func DeleteTxLookupEntry(db kv.Putter, hash libcommon.Hash) error {
	return db.Delete(kv.TxLookup, hash.Bytes())
}

// ReadTxIDLookupEntry retrieves the positional metadata associated with a transaction
// hash to allow retrieving the transaction or receipt by hash.
func ReadTxIDLookupEntry(db kv.Getter, txnHash libcommon.Hash) (*uint64, error) {
	data, err := db.GetOne(kv.TxIDLookUp, txnHash.Bytes())
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	number := new(big.Int).SetBytes(data).Uint64()
	return &number, nil
}

// WriteTxIDLookupEntries stores a positional metadata for every transaction from
// a block, enabling hash based transaction and receipt lookups.
func WriteTxIDLookupEntries(db kv.Putter, txn types.Transaction, txNum uint64) {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, txNum)
	if err := db.Put(kv.TxIDLookUp, txn.Hash().Bytes(), data); err != nil {
		log.Crit("Failed to store transaction id lookup entry", "err", err)
	}
}

// DeleteTxIDLookupEntry removes all transaction data associated with a hash.
func DeleteTxIDLookupEntry(db kv.Putter, hash libcommon.Hash) error {
	return db.Delete(kv.TxIDLookUp, hash.Bytes())
}
