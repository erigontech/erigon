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

package rawdb

import (
	"encoding/binary"
	"math/big"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/v3/core/types"
	bortypes "github.com/erigontech/erigon/v3/polygon/bor/types"
)

// HasBorReceipts verifies the existence of all block receipt belonging to a block.
func HasBorReceipts(db kv.Getter, number uint64) bool {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, number)
	if has, err := db.Has(kv.BorEventNums, k); !has || err != nil {
		return false
	}
	return true
}

func ReadBorTxLookupEntry(db kv.Getter, borTxHash libcommon.Hash) (*uint64, error) {
	blockNumBytes, err := db.GetOne(kv.BorTxLookup, borTxHash.Bytes())
	if err != nil {
		return nil, err
	}
	if blockNumBytes == nil {
		return nil, nil
	}

	blockNum := (new(big.Int).SetBytes(blockNumBytes)).Uint64()
	return &blockNum, nil
}

// ReadBorTransactionForBlock retrieves a specific bor (fake) transaction associated with a block, along with
// its added positional metadata.
func ReadBorTransactionForBlock(db kv.Tx, blockNum uint64) types.Transaction {
	if !HasBorReceipts(db, blockNum) {
		return nil
	}
	return bortypes.NewBorTransaction()
}
