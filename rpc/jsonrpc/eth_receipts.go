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
	"math"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/ethutils"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// ErrBlockRangeIntoFuture is the eth_getLogs message for a range past the
// executed head, exported so other packages can match on the condition.
const ErrBlockRangeIntoFuture = "block range extends beyond current head block"

// errBlockRangeIntoFuture rejects a range bound past the executed head as invalid params.
var errBlockRangeIntoFuture = &rpc.CustomError{Message: ErrBlockRangeIntoFuture, Code: rpc.ErrCodeInvalidParams}

var (
	errInvalidBlockRange               = "invalid block range params"
	errExceedBlockRange                = "query block range exceeds server limit, narrow your filter"
	errBlockHashWithRange              = "can't specify fromBlock/toBlock with blockHash"
	errExceedLogResults                = "query returns too many logs, narrow your filter"
	errRequestedBlockCountExceedsLimit = "requested blockCount exceeds server limit"
	errRequestedLogCountExceedsLimit   = "requested logCount exceeds server limit"
)

const (
	errExceedLogQueryLimit = "query exceeds the maximum of %d addresses or topics per search position"
)

// getReceipts - checking in-mem cache, or else fallback to db, or else fallback to re-exec of block to re-gen receipts
func (api *BaseAPI) getReceipts(ctx context.Context, tx kv.TemporalTx, block *types.Block) (types.Receipts, error) {
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	commitmentHistoryEnabled, err := api.commitmentHistoryEnabled(tx)
	if err != nil {
		return nil, err
	}
	return api.receiptsGenerator.GetReceipts(ctx, chainConfig, tx, block, eth.ReceiptsOpts{CommitmentHistoryEnabled: commitmentHistoryEnabled})
}

func (api *BaseAPI) getReceipt(ctx context.Context, cc *chain.Config, tx kv.TemporalTx, header *types.Header, txn types.Transaction, index int, txNum uint64, postState *receipts.PostStateInfo) (*types.Receipt, error) {
	return api.receiptsGenerator.GetReceipt(ctx, cc, tx, header, txn, index, txNum, postState)
}

func (api *BaseAPI) getReceiptsGasUsed(ctx context.Context, tx kv.TemporalTx, block *types.Block) (types.Receipts, error) {
	return api.receiptsGenerator.GetReceiptsGasUsed(ctx, tx, block, api._txNumReader)
}

func (api *BaseAPI) getCachedReceipts(ctx context.Context, hash common.Hash) (types.Receipts, bool) {
	return api.receiptsGenerator.GetCachedReceipts(ctx, hash)
}

// exceedsLogQueryLimit reports whether the filter has more addresses, or more
// alternatives in one topic position, than limit allows (<=0 = unlimited).
func exceedsLogQueryLimit(crit filters.FilterCriteria, limit int) bool {
	if limit <= 0 {
		return false
	}
	if len(crit.Addresses) > limit {
		return true
	}
	for _, topics := range crit.Topics {
		if len(topics) > limit {
			return true
		}
	}
	return false
}

// usesLogIndex reports whether the filter makes the query search LogAddrIdx or
// LogTopicIdx.
func usesLogIndex(crit filters.FilterCriteria) bool {
	return len(logIndexClauses(crit)) > 0
}

// errBlockHashNotFound marks a by-hash selector that names no canonical block
// with a body in the request's view.
var errBlockHashNotFound = errors.New("block not found")

// resolveLogsBlockHash resolves the block a by-hash log query names. The header-number
// index also covers non-canonical headers and the log scan is by block number, so a
// side-chain hash must not resolve to the canonical block at that height. Below the
// pruning boundary the body is gone by policy and the boundary is what the caller is
// told; elsewhere a missing body is what it says it is, and answering with an empty log
// array would hide it.
func (api *BaseAPI) resolveLogsBlockHash(ctx context.Context, tx kv.Tx, hash common.Hash) (uint64, error) {
	number, err := api._blockReader.HeaderNumber(ctx, tx, hash)
	if err != nil {
		return 0, err
	}
	if number == nil {
		return 0, fmt.Errorf("%w: %x", errBlockHashNotFound, hash)
	}
	canonicalHash, ok, err := api._blockReader.CanonicalHash(ctx, tx, *number)
	if err != nil {
		return 0, err
	}
	if !ok || canonicalHash != hash {
		return 0, fmt.Errorf("%w: %x", errBlockHashNotFound, hash)
	}
	body, err := api._blockReader.CanonicalBodyForStorage(ctx, tx, *number)
	if err != nil {
		return 0, err
	}
	if body == nil {
		if err := api.checkPruneBlocks(ctx, tx, *number); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("%w: %x", errBlockHashNotFound, hash)
	}
	return *number, nil
}

// resolveLogsRange resolves a filter's block range. A BlockHash pins the range to that
// block; otherwise negative tags are resolved against the chain, defaulting to the
// latest executed block. With checkFuture, ranges past the latest executed block are
// rejected as they are resolved. Tags resolve on the view tx exposes, since
// callers scan logs through that same tx.
func (api *BaseAPI) resolveLogsRange(ctx context.Context, tx kv.Tx, crit filters.FilterCriteria, checkFuture bool) (begin, end uint64, err error) {
	if crit.BlockHash != nil {
		number, err := api.resolveLogsBlockHash(ctx, tx, *crit.BlockHash)
		if err != nil {
			return 0, 0, err
		}
		return number, number, nil
	}

	latest, _, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(rpc.LatestExecutedBlockNumber), tx, api._blockReader)
	if err != nil {
		return 0, 0, err
	}

	begin = latest
	if crit.FromBlock != nil {
		fromBlock := crit.FromBlock.Int64()
		if fromBlock > 0 {
			begin = uint64(fromBlock)
		} else {
			blockNum := rpc.BlockNumber(fromBlock)
			begin, _, _, err = rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(blockNum), tx, api._blockReader)
			if err != nil {
				return 0, 0, err
			}
		}

		if checkFuture && begin > latest {
			return 0, 0, errBlockRangeIntoFuture
		}
	}
	end = latest
	if crit.ToBlock != nil {
		toBlock := crit.ToBlock.Int64()
		if toBlock > 0 {
			end = uint64(toBlock)
		} else {
			blockNum := rpc.BlockNumber(toBlock)
			end, _, _, err = rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(blockNum), tx, api._blockReader)
			if err != nil {
				return 0, 0, err
			}
		}

		if checkFuture && end > latest {
			return 0, 0, errBlockRangeIntoFuture
		}
	}
	return begin, end, nil
}

// GetLogs implements eth_getLogs. Returns an array of logs matching a given filter object.
func (api *APIImpl) GetLogs(ctx context.Context, crit filters.FilterCriteria) (types.Logs, error) {
	if err := crit.ValidateTopicPositions(); err != nil {
		return nil, err
	}

	tx, beginErr := api.db.BeginTemporalRo(ctx)
	if beginErr != nil {
		return nil, beginErr
	}
	defer tx.Rollback()

	if exceedsLogQueryLimit(crit, api.logQueryLimit) {
		return nil, &rpc.CustomError{Message: fmt.Sprintf(errExceedLogQueryLimit, api.logQueryLimit), Code: rpc.ErrCodeInvalidParams}
	}

	if crit.BlockHash != nil && (crit.FromBlock != nil || crit.ToBlock != nil) {
		return nil, &rpc.CustomError{Message: errBlockHashWithRange, Code: rpc.ErrCodeInvalidParams}
	}

	begin, end, err := api.resolveLogsRange(ctx, tx, crit, true)
	if err != nil {
		return nil, err
	}

	if end < begin {
		return nil, &rpc.CustomError{Message: errInvalidBlockRange, Code: rpc.ErrCodeInvalidParams}
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

	// Check if the requested blocks have been executed.
	// This prevents returning empty results when blocks exist but haven't been executed yet.
	latestExecuted, err := rpchelper.GetLatestExecutedBlockNumber(tx)
	if err != nil {
		return nil, err
	}
	if begin > latestExecuted || end > latestExecuted {
		return nil, fmt.Errorf("requested block range [%d, %d] is beyond latest executed block %d (node is still syncing)", begin, end, latestExecuted)
	}
	if begin == 0 && end == 0 && latestExecuted == 0 {
		return nil, fmt.Errorf("node is still initializing")
	}

	if err := api.BaseAPI.checkLogsAvailable(ctx, tx, begin, end, crit); err != nil {
		return nil, err
	}

	return api.getLogsV3(ctx, tx, begin, end, crit, api.BaseAPI.blockRangeLimit, api.BaseAPI.getLogsMaxResults)
}

func applyFiltersV3(txNumsReader rawdbv3.TxNumsReader, tx kv.TemporalTx, begin, end uint64, crit filters.FilterCriteria, asc order.By) (out stream.U64, err error) {
	//[from,to)
	var fromTxNum, toTxNum uint64
	c, err := tx.Cursor(kv.MaxTxNum)
	if err != nil {
		return out, err
	}
	defer c.Close()
	if begin > 0 {
		fromTxNum, err = txNumsReader.MinWithCursor(context.Background(), tx, c, begin)
		if err != nil {
			return out, err
		}
	}
	toTxNum, err = txNumsReader.MaxWithCursor(context.Background(), tx, c, end)
	if err != nil {
		return out, err
	}

	if asc {
		toTxNum++
	} else {
		toTxNum--
	}

	clauses := logIndexClauses(crit)
	if len(clauses) == 0 {
		if asc == order.Asc {
			return stream.Range[uint64](fromTxNum, toTxNum), nil
		}
		return stream.ReverseRange[uint64](toTxNum, fromTxNum), nil
	}
	q := kv.IndexQuery{Clauses: clauses, FromTs: int(fromTxNum), ToTs: int(toTxNum), Asc: asc}
	if !asc {
		q.FromTs, q.ToTs = int(toTxNum), int(fromTxNum)
	}
	return tx.IndexJoin(q)
}

func (api *BaseAPI) getLogsV3(ctx context.Context, tx kv.TemporalTx, begin, end uint64, crit filters.FilterCriteria, rangeLimit int, maxResults int) (types.Logs, error) {
	logs := types.Logs{}

	// Treat range-limit violations as invalid filter input to match eth_getLogs parameter validation.
	if rangeLimit != 0 && (end-begin) > uint64(rangeLimit) {
		return nil, &rpc.InvalidParamsError{
			Message: fmt.Sprintf("%s: %d", errExceedBlockRange, rangeLimit),
		}
	}

	filter := types.NewLogFilter(crit.Addresses, crit.Topics)

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	var header *types.Header

	txNumbers, err := applyFiltersV3(api._txNumReader, tx, begin, end, crit, order.Asc)
	if err != nil {
		return nil, err
	}

	it := rawdbv3.TxNums2BlockNums(ctx, tx, api._txNumReader, txNumbers, order.Asc)
	defer it.Close()

	for it.HasNext() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		txNum, blockNum, txIndex, _, isSystemTxn, blockNumChanged, err := it.Next()
		if err != nil {
			return nil, err
		}

		// if block number changed, calculate all related field
		if blockNumChanged {
			if header, err = api._blockReader.HeaderByNumber(ctx, tx, blockNum); err != nil {
				return nil, err
			}
			if header == nil {
				return nil, fmt.Errorf("header not found: %d", blockNum)
			}
		}

		if isSystemTxn {
			continue
		}

		if r, ok := api.receiptsGenerator.TryGetCachedReceipt(header.Hash(), txNum, txIndex); ok {
			logs, err = appendRPCLogs(logs, r.Logs, filter, header.Time, maxResults)
			if err != nil {
				return nil, err
			}
			continue
		}

		r, ok, err := api.receiptsGenerator.PersistedReceipt(ctx, tx, header, txIndex, txNum)
		if err != nil {
			return nil, err
		}
		if !ok {
			var txn types.Transaction
			if txn, ok, err = api._txnReader.TxnByIdxInBlock(ctx, tx, blockNum, txIndex); err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("transaction not found at block %d, index %d", blockNum, txIndex)
			}
			if r, err = api.receiptsGenerator.GetReceipt(ctx, chainConfig, tx, header, txn, txIndex, txNum, nil); err != nil {
				return nil, err
			}
		}

		logs, err = appendRPCLogs(logs, r.Logs, filter, header.Time, maxResults)
		if err != nil {
			return nil, err
		}
	}

	return logs, nil
}

func appendRPCLogs(logs types.Logs, receiptLogs types.Logs, filter *types.LogFilter, blockTime uint64, maxResults int) (types.Logs, error) {
	// One entry past the cap is enough to detect the overflow without converting the rest.
	// At math.MaxInt there is no such entry, so the append stays unlimited.
	var limit int
	if maxResults != 0 && maxResults != math.MaxInt {
		limit = maxResults + 1
	}
	logs = receiptLogs.AppendFilteredLogs(logs, filter, blockTime, limit)
	if maxResults != 0 && len(logs) > maxResults {
		return nil, &rpc.InvalidParamsError{
			Message: fmt.Sprintf("%s: %d", errExceedLogResults, maxResults),
		}
	}
	return logs, nil
}

// logIndexClauses returns the index query clauses of a log filter: one clause per topic position that
// is not empty, then one for the addresses. An empty topic position matches any topic, so it has
// no clause.
func logIndexClauses(crit filters.FilterCriteria) [][]kv.IndexKey {
	var clauses [][]kv.IndexKey
	for _, position := range crit.Topics {
		if len(position) == 0 {
			continue
		}
		clause := make([]kv.IndexKey, len(position))
		for i := range position {
			clause[i] = kv.IndexKey{Idx: kv.LogTopicIdx, Key: position[i][:]}
		}
		clauses = append(clauses, clause)
	}
	if len(crit.Addresses) > 0 {
		clause := make([]kv.IndexKey, len(crit.Addresses))
		for i := range crit.Addresses {
			clause[i] = kv.IndexKey{Idx: kv.LogAddrIdx, Key: crit.Addresses[i][:]}
		}
		clauses = append(clauses, clause)
	}
	return clauses
}

// GetTransactionReceipt implements eth_getTransactionReceipt. Returns the receipt of a transaction given the transaction's hash.
func (api *APIImpl) GetTransactionReceipt(ctx context.Context, txnHash common.Hash) (*ethutils.RPCReceipt, error) {
	tx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	blockNum, txNum, ok, err := api.txnLookup(ctx, tx, txnHash)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	err = api.BaseAPI.checkPruneBlocks(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	txnIndex, err := api.txnIndexInBlock(ctx, tx, blockNum, txNum)
	if err != nil {
		return nil, err
	}
	if err := api.checkReceiptAvailableAtIndex(ctx, tx, blockNum, uint64(txnIndex)); err != nil {
		return nil, err
	}

	header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, nil
	}

	txn, ok, err := api._blockReader.TxnByIdxInBlock(ctx, tx, header.Number.Uint64(), txnIndex)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	if txn.Hash() != txnHash {
		return nil, nil
	}

	// Check if we have commitment history: this is required to know if state root will be computed for historical state.
	commitmentHistory, err := api.commitmentHistoryEnabled(tx)
	if err != nil {
		return nil, err
	}

	var postState *receipts.PostStateInfo = nil
	if receipts.PostStateCalculated(chainConfig, blockNum, commitmentHistory, api._blockReader) {
		block, err := api.blockByNumberWithSenders(ctx, tx, blockNum)
		if err != nil {
			return nil, err
		}
		if block == nil {
			return nil, fmt.Errorf("getReceipt block not found: %d", blockNum)
		}
		txs := block.Transactions()
		postState = &receipts.PostStateInfo{
			Txns:              txs,
			CommitmentHistory: commitmentHistory,
		}
	}

	receipt, err := api.getReceipt(ctx, chainConfig, tx, header, txn, txnIndex, txNum, postState)
	if err != nil {
		return nil, err
	}

	return ethutils.MarshalReceipt(receipt, txn, chainConfig, header, true, true), nil
}

// GetBlockReceipts - receipts for individual block
func (api *APIImpl) GetBlockReceipts(ctx context.Context, numberOrHash rpc.BlockNumberOrHash) (ethutils.RPCReceipts, error) {
	tx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Pending block receipts are only available if the miner has a pending block
	// with executed transactions. Erigon receives only header+txs from the miner
	// and cannot compute receipts on-the-fly.
	if numberOrHash.BlockNumber != nil && *numberOrHash.BlockNumber == rpc.PendingBlockNumber {
		return nil, errors.New("pending receipts are not available")
	}

	blockNum, blockHash, _, err := rpchelper.GetCanonicalBlockNumber(ctx, numberOrHash, tx, api._blockReader)
	if err != nil {
		if errors.As(err, &rpc.BlockNotFoundErr{}) {
			return nil, nil // waiting for spec: not error, see Geth and https://github.com/erigontech/erigon/issues/1645
		}
		return nil, err
	}

	err = api.BaseAPI.checkBlockReceiptsAvailable(ctx, tx, blockNum)
	if err != nil {
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
		return nil, err
	}
	result := make(ethutils.RPCReceipts, 0, len(receipts))
	for _, receipt := range receipts {
		txn := block.Transactions()[receipt.TransactionIndex]
		result = append(result, ethutils.MarshalReceipt(receipt, txn, chainConfig, block.HeaderNoCopy(), true, true))
	}

	return result, nil
}
