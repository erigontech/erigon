package jsonrpc

import (
	"context"
	"slices"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon/cmd/state/exec3"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
)

type txNumsIterFactory func(tx kv.TemporalTx, addr common.Address, fromTxNum int) (*rawdbv3.MapTxNum2BlockNumIter, error)

func (api *OtterscanAPIImpl) buildSearchResults(ctx context.Context, tx kv.TemporalTx, iterFactory txNumsIterFactory, addr common.Address, fromTxNum int, pageSize uint16) ([]*RPCTransaction, []map[string]interface{}, bool, error) {
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, nil, false, err
	}

	txNumsIter, err := iterFactory(tx, addr, fromTxNum)
	if err != nil {
		return nil, nil, false, err
	}

	exec := exec3.NewTraceWorker(tx, chainConfig, api.engine(), api._blockReader, nil)
	var blockHash common.Hash
	var header *types.Header
	txs := make([]*RPCTransaction, 0, pageSize)
	receipts := make([]map[string]interface{}, 0, pageSize)
	resultCount := uint16(0)

	mustReadHeader := true
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

		// it is necessary to track dirty/lazy-must-read block headers
		// because we skip system txs like rewards (which are not "real" txs
		// for this rpc purposes)
		mustReadHeader = mustReadHeader || blockNumChanged
		if isFinalTxn {
			continue
		}

		if mustReadHeader {
			if header, err = api._blockReader.HeaderByNumber(ctx, tx, blockNum); err != nil {
				return nil, nil, false, err
			}
			if header == nil {
				log.Warn("[rpc] header is nil", "blockNum", blockNum)
				continue
			}
			blockHash = header.Hash()
			exec.ChangeBlock(header)
			mustReadHeader = false
		}

		txn, err := api._txnReader.TxnByIdxInBlock(ctx, tx, blockNum, txIndex)
		if err != nil {
			return nil, nil, false, err
		}
		if txn == nil {
			log.Warn("[rpc] txn not found", "blockNum", blockNum, "txIndex", txIndex)
			continue
		}
		res, err := exec.ExecTxn(txNum, txIndex, txn)
		if err != nil {
			return nil, nil, false, err
		}
		rawLogs := exec.GetLogs(txIndex, txn)
		rpcTx := NewRPCTransaction(txn, blockHash, blockNum, uint64(txIndex), header.BaseFee)
		txs = append(txs, rpcTx)
		receipt := &types.Receipt{
			Type:              txn.Type(),
			GasUsed:           res.UsedGas,
			CumulativeGasUsed: res.UsedGas, // TODO: cumulative gas is wrong, wait for cumulative gas index fix
			TransactionIndex:  uint(txIndex),
			BlockNumber:       header.Number,
			BlockHash:         blockHash,
			Logs:              rawLogs,
		}
		if res.Failed() {
			receipt.Status = types.ReceiptStatusFailed
		} else {
			receipt.Status = types.ReceiptStatusSuccessful
		}

		mReceipt := marshalReceipt(receipt, txn, chainConfig, header, txn.Hash(), true)
		mReceipt["timestamp"] = header.Time
		receipts = append(receipts, mReceipt)

		resultCount++
		if resultCount >= pageSize {
			reachedPageSize = true
		}
	}

	return txs, receipts, hasMore, nil
}

func createBackwardTxNumIter(tx kv.TemporalTx, addr common.Address, fromTxNum int) (*rawdbv3.MapTxNum2BlockNumIter, error) {
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
	txNums := iter.Union[uint64](itFrom, itTo, order.Desc, kv.Unlim)
	return rawdbv3.TxNums2BlockNums(tx, txNums, order.Desc), nil
}

func (api *OtterscanAPIImpl) searchTransactionsBeforeV3(tx kv.TemporalTx, ctx context.Context, addr common.Address, fromBlockNum uint64, pageSize uint16) (*TransactionsWithReceipts, error) {
	isFirstPage := false
	if fromBlockNum == 0 {
		isFirstPage = true
	} else {
		// Internal search code considers blockNum [including], so adjust the value
		fromBlockNum--
	}
	fromTxNum := -1
	if fromBlockNum != 0 {
		// from == 0 == magic number which means last; reproduce bug-compatibility for == 1
		// with e2 for now
		_txNum, err := rawdbv3.TxNums.Max(tx, fromBlockNum)
		if err != nil {
			return nil, err
		}
		fromTxNum = int(_txNum)
	}

	txs, receipts, hasMore, err := api.buildSearchResults(ctx, tx, createBackwardTxNumIter, addr, fromTxNum, pageSize)
	if err != nil {
		return nil, err
	}

	return &TransactionsWithReceipts{txs, receipts, isFirstPage, !hasMore}, nil
}

func createForwardTxNumIter(tx kv.TemporalTx, addr common.Address, fromTxNum int) (*rawdbv3.MapTxNum2BlockNumIter, error) {
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
	txNums := iter.Union[uint64](itFrom, itTo, order.Asc, kv.Unlim)
	return rawdbv3.TxNums2BlockNums(tx, txNums, order.Asc), nil
}

func (api *OtterscanAPIImpl) searchTransactionsAfterV3(tx kv.TemporalTx, ctx context.Context, addr common.Address, fromBlockNum uint64, pageSize uint16) (*TransactionsWithReceipts, error) {
	isLastPage := false
	fromTxNum := -1
	if fromBlockNum == 0 {
		isLastPage = true
	} else {
		// Internal search code considers blockNum [including], so adjust the value
		_txNum, err := rawdbv3.TxNums.Min(tx, fromBlockNum+1)
		if err != nil {
			return nil, err
		}
		fromTxNum = int(_txNum)
	}

	txs, receipts, hasMore, err := api.buildSearchResults(ctx, tx, createForwardTxNumIter, addr, fromTxNum, pageSize)
	if err != nil {
		return nil, err
	}
	slices.Reverse(txs)
	slices.Reverse(receipts)

	return &TransactionsWithReceipts{txs, receipts, !hasMore, isLastPage}, nil
}
