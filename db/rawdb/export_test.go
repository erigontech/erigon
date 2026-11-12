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
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
)

func TxnByIdxInBlock(db kv.Getter, blockHash common.Hash, blockNum uint64, txIdxInBlock int) (types.Transaction, bool, error) {
	txnRlp, err := TxnRlpByIdxInBlock(db, blockHash, blockNum, txIdxInBlock)
	if err != nil || txnRlp == nil {
		return nil, false, err
	}
	txn, err := types.DecodeTransaction(txnRlp)
	if err != nil {
		return nil, false, err
	}
	return txn, true, nil
}

// DeleteTxLookupEntry removes all transaction data associated with a hash.
func DeleteTxLookupEntry(db kv.Putter, hash common.Hash) error {
	return db.Delete(kv.TxLookup, hash[:])
}
