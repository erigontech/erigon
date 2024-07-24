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

package state

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
)

//go:generate mockgen -typed=true -destination=./iters_mock.go -package=state . CanonicalsReader
type CanonicalsReader interface {
	// TxnIdsOfCanonicalBlocks - for given canonical blocks range returns non-canonical txnIds (not txNums)
	// [fromTxNum, toTxNum)
	// To get all canonical blocks, use fromTxNum=0, toTxNum=-1
	// For reverse iteration use order.Desc and fromTxNum=-1, toTxNum=-1
	TxnIdsOfCanonicalBlocks(tx kv.Tx, fromTxNum, toTxNum int, asc order.By, limit int) (stream.U64, error)
	BaseTxnID(tx kv.Tx, blockNum uint64, blockHash common.Hash) (kv.TxnId, error)
	TxNum2ID(tx kv.Tx, blockNum uint64, blockHash common.Hash, txNum uint64) (kv.TxnId, error)
	LastFrozenTxNum(tx kv.Tx) (kv.TxnId, error)
}
