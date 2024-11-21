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

package jsonrpc

import (
	"context"
	"slices"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethutils"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

type txNumsIterFactory func(tx kv.TemporalTx, txNumsReader rawdbv3.TxNumsReader, addr common.Address, fromTxNum int) (*rawdbv3.MapTxNum2BlockNumIter, error)

func (api *OtterscanAPIImpl) buildSearchResults(ctx context.Context, tx kv.TemporalTx, txNumsReader rawdbv3.TxNumsReader, iterFactory txNumsIterFactory, addr common.Address, fromTxNum int, pageSize uint16) ([]*RPCTransaction, []map[string]interface{}, bool, error) {
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, nil, false, err
	}

	txNumsIter, err := iterFactory(tx, txNumsReader, addr, fromTxNum)
	if err != nil {
		return nil, nil, false, err
	}

	var block *types.Block
	txs := make([]*RPCTransaction, 0, pageSize)
	receipts := make([]map[string]interface{}, 0, pageSize)
	resultCount := uint16(0)

	mustReadBlock := true
	reachedPageSize := false
	hasMore := false
	for txNumsIter.HasNext() {
		txNum, blockNum, txIndex, isFinalTxn, blockNumChanged, err := txNumsIter.Next()
		if err != nil {
			return nil, nil, false, err
		}

		// Even if the desired page size is reached, drain the entire matching
		// txs inside the block; reproduces e2 behavior. An e3/paginated-aware
		// ots spec could improve in this area.
		if blockNumChanged && reachedPageSize {
			hasMore = true
			break
		}

		// Avoid reading the same block multiple times; multiple matches in the same block
		// may be common.
		mustReadBlock = mustReadBlock || blockNumChanged

		// it is necessary to track dirty/lazy-must-read block headers
		// because we skip system txs like rewards (which are not "real" txs
		// for this rpc purposes)
		if isFinalTxn {
			continue
		}

		if mustReadBlock {
			block, err = api.blockByNumberWithSenders(ctx, tx, blockNum)
			if err != nil {
				return nil, nil, false, err
			}
			mustReadBlock = false
		}

		txn, err := api._txnReader.TxnByIdxInBlock(ctx, tx, blockNum, txIndex)
		if err != nil {
			return nil, nil, false, err
		}
		if txn == nil {
			log.Warn("[rpc] txn not found", "blockNum", blockNum, "txIndex", txIndex)
			continue
		}
		rpcTx := NewRPCTransaction(txn, block.Hash(), blockNum, uint64(txIndex), block.BaseFee())
		txs = append(txs, rpcTx)

		receipt, err := api.receiptsGenerator.GetReceipt(ctx, chainConfig, tx, block, txIndex, txNum+1, true)
		if err != nil {
			return nil, nil, false, err
		}

		mReceipt := ethutils.MarshalReceipt(receipt, txn, chainConfig, block.HeaderNoCopy(), txn.Hash(), true)
		mReceipt["timestamp"] = block.Time()
		receipts = append(receipts, mReceipt)

		resultCount++
		if resultCount >= pageSize {
			reachedPageSize = true
		}
	}

	return txs, receipts, hasMore, nil
}

func createBackwardTxNumIter(tx kv.TemporalTx, txNumsReader rawdbv3.TxNumsReader, addr common.Address, fromTxNum int) (*rawdbv3.MapTxNum2BlockNumIter, error) {
	// unbounded limit on purpose, since there could be e.g. block rewards system txs, we limit
	// results later
	itTo, err := tx.IndexRange(kv.TracesToIdx, addr[:], fromTxNum, -1, order.Desc, kv.Unlim)
	if err != nil {
		return nil, err
	}
	itFrom, err := tx.IndexRange(kv.TracesFromIdx, addr[:], fromTxNum, -1, order.Desc, kv.Unlim)
	if err != nil {
		return nil, err
	}
	txNums := stream.Union[uint64](itFrom, itTo, order.Desc, kv.Unlim)
	return rawdbv3.TxNums2BlockNums(tx, txNumsReader, txNums, order.Desc), nil
}

func (api *OtterscanAPIImpl) searchTransactionsBeforeV3(tx kv.TemporalTx, ctx context.Context, addr common.Address, fromBlockNum uint64, pageSize uint16) (*TransactionsWithReceipts, error) {
	isFirstPage := false
	if fromBlockNum == 0 {
		isFirstPage = true
	} else {
		// Internal search code considers blockNum [including], so adjust the value
		fromBlockNum--
	}
	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, api._blockReader))

	fromTxNum := -1
	if fromBlockNum != 0 {
		// from == 0 == magic number which means last; reproduce bug-compatibility for == 1
		// with e2 for now
		_txNum, err := txNumsReader.Max(tx, fromBlockNum)
		if err != nil {
			return nil, err
		}
		fromTxNum = int(_txNum)
	}

	txs, receipts, hasMore, err := api.buildSearchResults(ctx, tx, txNumsReader, createBackwardTxNumIter, addr, fromTxNum, pageSize)
	if err != nil {
		return nil, err
	}

	return &TransactionsWithReceipts{txs, receipts, isFirstPage, !hasMore}, nil
}

func createForwardTxNumIter(tx kv.TemporalTx, txNumsReader rawdbv3.TxNumsReader, addr common.Address, fromTxNum int) (*rawdbv3.MapTxNum2BlockNumIter, error) {
	// unbounded limit on purpose, since there could be e.g. block rewards system txs, we limit
	// results later
	itTo, err := tx.IndexRange(kv.TracesToIdx, addr[:], fromTxNum, -1, order.Asc, kv.Unlim)
	if err != nil {
		return nil, err
	}
	itFrom, err := tx.IndexRange(kv.TracesFromIdx, addr[:], fromTxNum, -1, order.Asc, kv.Unlim)
	if err != nil {
		return nil, err
	}
	txNums := stream.Union[uint64](itFrom, itTo, order.Asc, kv.Unlim)
	return rawdbv3.TxNums2BlockNums(tx, txNumsReader, txNums, order.Asc), nil
}

func (api *OtterscanAPIImpl) searchTransactionsAfterV3(tx kv.TemporalTx, ctx context.Context, addr common.Address, fromBlockNum uint64, pageSize uint16) (*TransactionsWithReceipts, error) {
	isLastPage := false
	fromTxNum := -1
	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, api._blockReader))

	if fromBlockNum == 0 {
		isLastPage = true
	} else {
		// Internal search code considers blockNum [including], so adjust the value
		_txNum, err := txNumsReader.Min(tx, fromBlockNum+1)
		if err != nil {
			return nil, err
		}
		fromTxNum = int(_txNum)
	}

	txs, receipts, hasMore, err := api.buildSearchResults(ctx, tx, txNumsReader, createForwardTxNumIter, addr, fromTxNum, pageSize)
	if err != nil {
		return nil, err
	}
	slices.Reverse(txs)
	slices.Reverse(receipts)

	return &TransactionsWithReceipts{txs, receipts, !hasMore, isLastPage}, nil
}
