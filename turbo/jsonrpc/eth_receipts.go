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
	"errors"
	"fmt"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethutils"
	"github.com/erigontech/erigon/eth/filters"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

// getReceipts - checking in-mem cache, or else fallback to db, or else fallback to re-exec of block to re-gen receipts
func (api *BaseAPI) getReceipts(ctx context.Context, tx kv.TemporalTx, block *types.Block) (types.Receipts, error) {
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	return api.receiptsGenerator.GetReceipts(ctx, chainConfig, tx, block)
}

func (api *BaseAPI) getReceipt(ctx context.Context, cc *chain.Config, tx kv.TemporalTx, header *types.Header, txn types.Transaction, index int, txNum uint64) (*types.Receipt, error) {
	return api.receiptsGenerator.GetReceipt(ctx, cc, tx, header, txn, index, txNum)
}

func (api *BaseAPI) getCachedReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, bool) {
	return api.receiptsGenerator.GetCachedReceipt(ctx, hash)
}

func (api *BaseAPI) getCachedReceipts(ctx context.Context, hash common.Hash) (types.Receipts, bool) {
	return api.receiptsGenerator.GetCachedReceipts(ctx, hash)
}

// GetLogs implements eth_getLogs. Returns an array of logs matching a given filter object.
func (api *APIImpl) GetLogs(ctx context.Context, crit filters.FilterCriteria) (types.Logs, error) {
	var begin, end uint64
	logs := types.Logs{}

	tx, beginErr := api.db.BeginTemporalRo(ctx)
	if beginErr != nil {
		return logs, beginErr
	}
	defer tx.Rollback()

	if crit.BlockHash != nil {
		block, err := api.blockByHashWithSenders(ctx, tx, *crit.BlockHash)
		if err != nil {
			return nil, err
		}
		if block == nil {
			return nil, fmt.Errorf("block not found: %x", *crit.BlockHash)
		}

		num := block.NumberU64()
		begin = num
		end = num
	} else {
		// Convert the RPC block numbers into internal representations
		latest, _, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(rpc.LatestExecutedBlockNumber), tx, api._blockReader, nil)
		if err != nil {
			return nil, err
		}

		begin = latest
		if crit.FromBlock != nil {
			fromBlock := crit.FromBlock.Int64()
			if fromBlock > 0 {
				begin = uint64(fromBlock)
			} else {
				blockNum := rpc.BlockNumber(fromBlock)
				begin, _, _, err = rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(blockNum), tx, api._blockReader, api.filters)
				if err != nil {
					return nil, err
				}
			}

			if uint64(fromBlock) > latest {
				return types.Logs{}, nil
			}
		}
		end = latest
		if crit.ToBlock != nil {
			toBlock := crit.ToBlock.Int64()
			if toBlock > 0 {
				end = uint64(toBlock)
			} else {
				blockNum := rpc.BlockNumber(toBlock)
				end, _, _, err = rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(blockNum), tx, api._blockReader, api.filters)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	if end < begin {
		return nil, fmt.Errorf("end (%d) < begin (%d)", end, begin)
	}
	if end > roaring.MaxUint32 {
		latest, err := rpchelper.GetLatestBlockNumber(tx)
		if err != nil {
			return nil, err
		}
		if begin > latest {
			return nil, fmt.Errorf("begin (%d) > latest (%d)", begin, latest)
		}
		end = latest
	}

	erigonLogs, err := api.getLogsV3(ctx, tx, begin, end, crit)
	if err != nil {
		return nil, err
	}
	logs = make(types.Logs, len(erigonLogs))
	for i, log := range erigonLogs {
		logs[i] = &types.Log{
			Address:     log.Address,
			Topics:      log.Topics,
			Data:        log.Data,
			BlockNumber: log.BlockNumber,
			TxHash:      log.TxHash,
			TxIndex:     log.TxIndex,
			BlockHash:   log.BlockHash,
			Index:       log.Index,
			Removed:     log.Removed,
		}
	}
	return logs, nil
}

func applyFiltersV3(txNumsReader rawdbv3.TxNumsReader, tx kv.TemporalTx, begin, end uint64, crit filters.FilterCriteria) (out stream.U64, err error) {
	//[from,to)
	var fromTxNum, toTxNum uint64
	if begin > 0 {
		fromTxNum, err = txNumsReader.Min(tx, begin)
		if err != nil {
			return out, err
		}
	}
	toTxNum, err = txNumsReader.Max(tx, end)
	if err != nil {
		return out, err
	}
	toTxNum++

	topicsBitmap, err := getTopicsBitmapV3(tx, crit.Topics, fromTxNum, toTxNum)
	if err != nil {
		return out, err
	}
	if topicsBitmap != nil {
		out = topicsBitmap
	}
	addrBitmap, err := getAddrsBitmapV3(tx, crit.Addresses, fromTxNum, toTxNum)
	if err != nil {
		return out, err
	}
	if addrBitmap != nil {
		if out == nil {
			out = addrBitmap
		} else {
			out = stream.Intersect[uint64](out, addrBitmap, -1)
		}
	}
	if out == nil {
		out = stream.Range[uint64](fromTxNum, toTxNum)
	}
	return out, nil
}

func (api *BaseAPI) getLogsV3(ctx context.Context, tx kv.TemporalTx, begin, end uint64, crit filters.FilterCriteria) ([]*types.ErigonLog, error) {
	logs := []*types.ErigonLog{} //nolint

	addrMap := make(map[common.Address]struct{}, len(crit.Addresses))
	for _, v := range crit.Addresses {
		addrMap[v] = struct{}{}
	}

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	var header *types.Header

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, api._blockReader))
	txNumbers, err := applyFiltersV3(txNumsReader, tx, begin, end, crit)
	if err != nil {
		return logs, err
	}

	it := rawdbv3.TxNums2BlockNums(tx,
		txNumsReader,
		txNumbers, order.Asc)
	defer it.Close()
	for it.HasNext() {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		txNum, blockNum, txIndex, isFinalTxn, blockNumChanged, err := it.Next()
		if err != nil {
			return nil, err
		}
		if isFinalTxn {
			continue
		}

		// if block number changed, calculate all related field

		if blockNumChanged {
			if header, err = api._blockReader.HeaderByNumber(ctx, tx, blockNum); err != nil {
				return nil, err
			}
			if header == nil {
				log.Warn("[rpc] header is nil", "blockNum", blockNum)
				continue
			}
		}

		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d, maxTxNumInBlock=%d,mixTxNumInBlock=%d\n", txNum, blockNum, txIndex, maxTxNumInBlock, minTxNumInBlock)
		txn, err := api._txnReader.TxnByIdxInBlock(ctx, tx, blockNum, txIndex)
		if err != nil {
			return nil, err
		}
		if txn == nil {
			continue
		}

		r, err := api.receiptsGenerator.GetReceipt(ctx, chainConfig, tx, header, txn, txIndex, txNum+1)
		if err != nil {
			return nil, err
		}
		if r == nil {
			return nil, err
		}
		filtered := r.Logs.Filter(addrMap, crit.Topics, 0)

		for _, filteredLog := range filtered {
			logs = append(logs, &types.ErigonLog{
				Address:     filteredLog.Address,
				Topics:      filteredLog.Topics,
				Data:        filteredLog.Data,
				BlockNumber: filteredLog.BlockNumber,
				TxHash:      filteredLog.TxHash,
				TxIndex:     filteredLog.TxIndex,
				BlockHash:   filteredLog.BlockHash,
				Index:       filteredLog.Index,
				Removed:     filteredLog.Removed,
				Timestamp:   header.Time,
			})
		}
	}

	// Get logs from state sync events for block range
	if chainConfig.Bor != nil {
		var allBorLogs []*types.ErigonLog
		for blockNum := begin; blockNum <= end; blockNum++ {
			header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNum)
			if err != nil {
				return nil, err
			}
			if header == nil {
				continue
			}

			// check for state sync event logs
			events, err := api.stateSyncEvents(ctx, tx, header.Hash(), blockNum, chainConfig)
			if err != nil {
				return nil, err
			}

			if len(events) == 0 {
				continue
			}

			lastTxNum, err := txNumsReader.Max(tx, blockNum)
			if err != nil {
				return nil, err
			}

			firstTxNum, err := txNumsReader.Min(tx, blockNum)
			if err != nil {
				return nil, err
			}
			txIndex := lastTxNum - firstTxNum - 1 // -1 to account for "final tx"

			_, _, logIndex, err := rawtemporaldb.ReceiptAsOf(tx, lastTxNum+1)
			if err != nil {
				return nil, err
			}

			borLogs, err := api.borReceiptGenerator.GenerateBorLogs(ctx, events, txNumsReader, tx, header, chainConfig, int(txIndex), int(logIndex))
			if err != nil {
				return logs, err
			}

			borLogs = borLogs.Filter(addrMap, crit.Topics, 0)
			for _, filteredLog := range borLogs {
				allBorLogs = append(allBorLogs, &types.ErigonLog{
					Address:     filteredLog.Address,
					Topics:      filteredLog.Topics,
					Data:        filteredLog.Data,
					BlockNumber: filteredLog.BlockNumber,
					TxHash:      filteredLog.TxHash,
					TxIndex:     filteredLog.TxIndex,
					BlockHash:   filteredLog.BlockHash,
					Index:       filteredLog.Index,
					Removed:     filteredLog.Removed,
					Timestamp:   header.Time,
				})
			}
		}

		// merge bor logs in the correct order
		logs = mergeSortedLogs(logs, allBorLogs)
	}

	return logs, nil
}

func mergeSortedLogs(orig, new []*types.ErigonLog) []*types.ErigonLog {
	merged := make([]*types.ErigonLog, 0, len(orig)+len(new))
	i, j := 0, 0

	for i < len(orig) && j < len(new) {
		if orig[i].BlockNumber <= new[j].BlockNumber {
			merged = append(merged, orig[i])
			i++
		} else {
			merged = append(merged, new[j])
			j++
		}
	}

	if i < len(orig) {
		merged = append(merged, orig[i:]...)
	}

	if j < len(new) {
		merged = append(merged, new[j:]...)
	}

	return merged
}

// The Topic list restricts matches to particular event topics. Each event has a list
// of topics. Topics matches a prefix of that list. An empty element slice matches any
// topic. Non-empty elements represent an alternative that matches any of the
// contained topics.
//
// Examples:
// {} or nil          matches any topic list
// {{A}}              matches topic A in first position
// {{}, {B}}          matches any topic in first position AND B in second position
// {{A}, {B}}         matches topic A in first position AND B in second position
// {{A, B}, {C, D}}   matches topic (A OR B) in first position AND (C OR D) in second position
func getTopicsBitmapV3(tx kv.TemporalTx, topics [][]common.Hash, from, to uint64) (res stream.U64, err error) {
	for _, sub := range topics {
		if len(sub) == 0 {
			continue
		}

		var topicsUnion stream.U64
		for _, topic := range sub {
			it, err := tx.IndexRange(kv.LogTopicIdx, topic.Bytes(), int(from), int(to), order.Asc, kv.Unlim)
			if err != nil {
				return nil, err
			}
			topicsUnion = stream.Union[uint64](topicsUnion, it, order.Asc, -1)
		}

		if res == nil {
			res = topicsUnion
			continue
		}
		res = stream.Intersect[uint64](res, topicsUnion, -1)
	}
	return res, nil
}

func getAddrsBitmapV3(tx kv.TemporalTx, addrs []common.Address, from, to uint64) (res stream.U64, err error) {
	for _, addr := range addrs {
		it, err := tx.IndexRange(kv.LogAddrIdx, addr[:], int(from), int(to), true, kv.Unlim)
		if err != nil {
			return nil, err
		}
		res = stream.Union[uint64](res, it, order.Asc, -1)
	}
	return res, nil
}

// GetTransactionReceipt implements eth_getTransactionReceipt. Returns the receipt of a transaction given the transaction's hash.
func (api *APIImpl) GetTransactionReceipt(ctx context.Context, txnHash common.Hash) (map[string]interface{}, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var blockNum, txNum uint64
	var ok bool

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	blockNum, txNum, ok, err = api.txnLookup(ctx, tx, txnHash)
	if err != nil {
		return nil, err
	}
	if !ok && chainConfig.Bor == nil {
		return nil, nil
	}

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, api._blockReader))

	txNumMin, err := txNumsReader.Min(tx, blockNum)
	if err != nil {
		return nil, err
	}

	// Private API returns 0 if transaction is not found.
	if blockNum == 0 && chainConfig.Bor != nil {
		if api.useBridgeReader {
			blockNum, ok, err = api.bridgeReader.EventTxnLookup(ctx, txnHash)
			if err != nil {
				return nil, err
			}
			if ok {
				txNumNextBlock, err := txNumsReader.Min(tx, blockNum+1)
				if err != nil {
					return nil, err
				}
				txNum = txNumNextBlock
			}
		} else {
			blockNum, ok, err = api._blockReader.EventLookup(ctx, tx, txnHash)
		}
		if err != nil {
			return nil, err
		}
	}

	if !ok {
		return nil, nil
	}

	if txNumMin+2 > txNum { //TODO: what a magic is this "2" and how to avoid it
		return nil, fmt.Errorf("uint underflow txnums error txNum: %d, txNumMin: %d, blockNum: %d", txNum, txNumMin, blockNum)
	}

	header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	var txnIndex = int(txNum - txNumMin - 2)

	txn, err := api._blockReader.TxnByIdxInBlock(ctx, tx, header.Number.Uint64(), txnIndex)
	if err != nil {
		return nil, err
	}

	if txn == nil && chainConfig.Bor != nil {
		block, err := api.blockByNumberWithSenders(ctx, tx, blockNum)
		if err != nil {
			return nil, err
		}
		if block == nil {
			return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
		}

		events, err := api.stateSyncEvents(ctx, tx, block.Hash(), blockNum, chainConfig)
		if err != nil {
			return nil, err
		}

		if len(events) == 0 {
			return nil, errors.New("tx not found")
		}

		borReceipt, err := api.borReceiptGenerator.GenerateBorReceipt(ctx, tx, block, events, chainConfig)
		if err != nil {
			return nil, err
		}

		return ethutils.MarshalReceipt(borReceipt, bortypes.NewBorTransaction(), chainConfig, block.HeaderNoCopy(), txnHash, false), nil
	}

	receipt, err := api.getReceipt(ctx, chainConfig, tx, header, txn, txnIndex, txNum)
	if err != nil {
		return nil, fmt.Errorf("getReceipt error: %w", err)
	}

	return ethutils.MarshalReceipt(receipt, txn, chainConfig, header, txnHash, true), nil
}

// GetBlockReceipts - receipts for individual block
func (api *APIImpl) GetBlockReceipts(ctx context.Context, numberOrHash rpc.BlockNumberOrHash) ([]map[string]interface{}, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blockNum, blockHash, _, err := rpchelper.GetBlockNumber(ctx, numberOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		bnh, _ := numberOrHash.Hash()
		if errors.Is(err, rpchelper.BlockNotFoundErr{Hash: bnh}) {
			return nil, nil
		}
		return nil, err
	}
	block, err := api.blockWithSenders(ctx, tx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	receipts, err := api.getReceipts(ctx, tx, block)
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %w", err)
	}
	result := make([]map[string]interface{}, 0, len(receipts))
	for _, receipt := range receipts {
		txn := block.Transactions()[receipt.TransactionIndex]
		result = append(result, ethutils.MarshalReceipt(receipt, txn, chainConfig, block.HeaderNoCopy(), txn.Hash(), true))
	}

	if chainConfig.Bor != nil {
		events, err := api.stateSyncEvents(ctx, tx, block.Hash(), blockNum, chainConfig)
		if err != nil {
			return nil, err
		}

		if len(events) != 0 {
			borReceipt, err := api.borReceiptGenerator.GenerateBorReceipt(ctx, tx, block, events, chainConfig)
			if err != nil {
				return nil, err
			}

			result = append(result, ethutils.MarshalReceipt(borReceipt, bortypes.NewBorTransaction(), chainConfig, block.HeaderNoCopy(), borReceipt.TxHash, false))
		}
	}

	return result, nil
}

// MapTxNum2BlockNumIter - enrich iterator by TxNumbers, adding more info:
//   - blockNum
//   - txIndex in block: -1 means first system tx
//   - isFinalTxn: last system-txn. BlockRewards and similar things - are attribute to this virtual txn.
//   - blockNumChanged: means this and previous txNum belongs to different blockNumbers
//
// Expect: `it` to return sorted txNums, then blockNum will not change until `it.Next() < maxTxNumInBlock`
//
//	it allow certain optimizations.
type MapTxNum2BlockNumIter struct {
	it          stream.U64
	tx          kv.Tx
	orderAscend bool

	blockNum                         uint64
	minTxNumInBlock, maxTxNumInBlock uint64

	txNumsReader rawdbv3.TxNumsReader
}

func MapTxNum2BlockNum(tx kv.Tx, txNumsReader rawdbv3.TxNumsReader, it stream.U64) *MapTxNum2BlockNumIter {
	return &MapTxNum2BlockNumIter{tx: tx, it: it, orderAscend: true, txNumsReader: txNumsReader}
}
func MapDescendTxNum2BlockNum(tx kv.Tx, txNumsReader rawdbv3.TxNumsReader, it stream.U64) *MapTxNum2BlockNumIter {
	return &MapTxNum2BlockNumIter{tx: tx, it: it, orderAscend: false, txNumsReader: txNumsReader}
}
func (i *MapTxNum2BlockNumIter) HasNext() bool { return i.it.HasNext() }
func (i *MapTxNum2BlockNumIter) Next() (txNum, blockNum uint64, txIndex int, isFinalTxn, blockNumChanged bool, err error) {
	txNum, err = i.it.Next()
	if err != nil {
		return txNum, blockNum, txIndex, isFinalTxn, blockNumChanged, err
	}

	// txNums are sorted, it means blockNum will not change until `txNum < maxTxNumInBlock`
	if i.maxTxNumInBlock == 0 || (i.orderAscend && txNum > i.maxTxNumInBlock) || (!i.orderAscend && txNum < i.minTxNumInBlock) {
		blockNumChanged = true

		var ok bool
		ok, i.blockNum, err = i.txNumsReader.FindBlockNum(i.tx, txNum)
		if err != nil {
			return
		}
		if !ok {
			_lb, _lt, _ := i.txNumsReader.Last(i.tx)
			_fb, _ft, _ := i.txNumsReader.First(i.tx)
			return txNum, i.blockNum, txIndex, isFinalTxn, blockNumChanged, fmt.Errorf("can't find blockNumber by txNum=%d; last in db: (%d-%d, %d-%d)", txNum, _fb, _lb, _ft, _lt)
		}
	}
	blockNum = i.blockNum

	// if block number changed, calculate all related field
	if blockNumChanged {
		i.minTxNumInBlock, err = i.txNumsReader.Min(i.tx, blockNum)
		if err != nil {
			return
		}
		i.maxTxNumInBlock, err = i.txNumsReader.Max(i.tx, blockNum)
		if err != nil {
			return
		}
	}

	txIndex = int(txNum) - int(i.minTxNumInBlock) - 1
	isFinalTxn = txNum == i.maxTxNumInBlock
	return
}
