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
	"math/big"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/state"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core/types"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
)

// HasBorReceipts verifies the existence of all block receipt belonging to a block.
func HasBorReceipts(tx kv.Tx, number uint64) bool {
	receiptReader := state.NewBorReceiptsReader(tx.(kv.TemporalTx))
	_, ok, err := receiptReader.ReadReceipt(number)
	if err != nil {
		return false
	}

	return ok
}

func ReadBorReceipt(tx kv.Tx, blockNumber uint64) (*types.Receipt, bool, error) {
	receiptReader := state.NewBorReceiptsReader(tx.(kv.TemporalTx))
	return receiptReader.ReadReceipt(blockNumber)
}

// ReadBorTransactionForBlock retrieves a specific bor (fake) transaction associated with a block, along with
// its added positional metadata.
func ReadBorTransactionForBlock(db kv.Tx, blockNum uint64) types.Transaction {
	if !HasBorReceipts(db, blockNum) {
		return nil
	}
	return bortypes.NewBorTransaction()
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
