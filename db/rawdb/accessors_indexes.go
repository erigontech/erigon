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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
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
func ReadTxLookupEntry(db kv.Getter, txnHash common.Hash) (blockNumber *uint64, txNum *uint64, err error) {
	data, err := db.GetOne(kv.TxLookup, txnHash.Bytes())
	if err != nil {
		return nil, nil, err
	}
	if len(data) != 16 {
		return nil, nil, nil
	}
	numberBlockNum := binary.BigEndian.Uint64(data[:8])
	numberTxNum := binary.BigEndian.Uint64(data[8:])

	return &numberBlockNum, &numberTxNum, nil
}

// WriteTxLookupEntries stores a positional metadata for every transaction from
// a block, enabling hash based transaction and receipt lookups.
func WriteTxLookupEntries(db kv.Putter, block *types.Block, txNum uint64) {
	data := make([]byte, 16)
	for i, txn := range block.Transactions() {
		binary.BigEndian.PutUint64(data[:8], block.NumberU64())
		binary.BigEndian.PutUint64(data[8:], txNum+uint64(i)+1)

		if err := db.Put(kv.TxLookup, txn.Hash().Bytes(), data); err != nil {
			log.Crit("Failed to store transaction lookup entry", "err", err)
		}
	}
}

// DeleteTxLookupEntry removes all transaction data associated with a hash.
func DeleteTxLookupEntry(db kv.Putter, hash common.Hash) error {
	return db.Delete(kv.TxLookup, hash.Bytes())
}
