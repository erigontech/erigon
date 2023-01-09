package commands

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/holiman/uint256"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/transactions"
)

func (api *BaseAPI) getReceipts(ctx context.Context, tx kv.Tx, chainConfig *params.ChainConfig, block *types.Block, senders []common.Address) (types.Receipts, error) {
	if cached := rawdb.ReadReceipts(tx, block, senders); cached != nil {
		return cached, nil
	}
	engine := api.engine()

	_, _, _, ibs, _, err := transactions.ComputeTxEnv(ctx, engine, block, chainConfig, api._blockReader, tx, 0, api._agg, api.historyV3(tx))
	if err != nil {
		return nil, err
	}

	usedGas := new(uint64)
	gp := new(core.GasPool).AddGas(block.GasLimit())

	noopWriter := state.NewNoopWriter()

	receipts := make(types.Receipts, len(block.Transactions()))

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, e := api._blockReader.Header(ctx, tx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h
	}
	for i, txn := range block.Transactions() {
		ibs.Prepare(txn.Hash(), block.Hash(), i)
		header := block.Header()
		receipt, _, err := core.ApplyTransaction(chainConfig, core.GetHashFn(header, getHeader), engine, nil, gp, ibs, noopWriter, header, txn, usedGas, vm.Config{})
		if err != nil {
			return nil, err
		}
		receipt.BlockHash = block.Hash()
		receipts[i] = receipt
	}

	return receipts, nil
}

// GetLogs implements eth_getLogs. Returns an array of logs matching a given filter object.
func (api *APIImpl) GetLogs(ctx context.Context, crit filters.FilterCriteria) (types.Logs, error) {
	var begin, end uint64
	logs := types.Logs{}

	tx, beginErr := api.db.BeginRo(ctx)
	if beginErr != nil {
		return logs, beginErr
	}
	defer tx.Rollback()

	if crit.BlockHash != nil {
		header, err := api._blockReader.HeaderByHash(ctx, tx, *crit.BlockHash)
		if err != nil {
			return nil, err
		}
		if header == nil {
			return nil, fmt.Errorf("block not found: %x", *crit.BlockHash)
		}
		begin = header.Number.Uint64()
		end = header.Number.Uint64()
	} else {
		// Convert the RPC block numbers into internal representations
		latest, _, _, err := rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithNumber(rpc.LatestExecutedBlockNumber), tx, nil)
		if err != nil {
			return nil, err
		}

		begin = latest
		if crit.FromBlock != nil {
			if crit.FromBlock.Sign() >= 0 {
				begin = crit.FromBlock.Uint64()
			} else if !crit.FromBlock.IsInt64() || crit.FromBlock.Int64() != int64(rpc.LatestBlockNumber) {
				return nil, fmt.Errorf("negative value for FromBlock: %v", crit.FromBlock)
			}
		}
		end = latest
		if crit.ToBlock != nil {
			if crit.ToBlock.Sign() >= 0 {
				end = crit.ToBlock.Uint64()
			} else if !crit.ToBlock.IsInt64() || crit.ToBlock.Int64() != int64(rpc.LatestBlockNumber) {
				return nil, fmt.Errorf("negative value for ToBlock: %v", crit.ToBlock)
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

	if api.historyV3(tx) {
		return api.getLogsV3(ctx, tx.(kv.TemporalTx), begin, end, crit)
	}

	blockNumbers := bitmapdb.NewBitmap()
	defer bitmapdb.ReturnToPool(blockNumbers)
	blockNumbers.AddRange(begin, end+1) // [min,max)
	topicsBitmap, err := getTopicsBitmap(tx, crit.Topics, uint32(begin), uint32(end))
	if err != nil {
		return nil, err
	}

	if topicsBitmap != nil {
		blockNumbers.And(topicsBitmap)
	}

	rx := make([]*roaring.Bitmap, len(crit.Addresses))
	for idx, addr := range crit.Addresses {
		m, err := bitmapdb.Get(tx, kv.LogAddressIndex, addr[:], uint32(begin), uint32(end))
		if err != nil {
			return nil, err
		}
		rx[idx] = m
	}

	addrBitmap := roaring.FastOr(rx...)

	if len(rx) > 0 {
		blockNumbers.And(addrBitmap)
	}

	if blockNumbers.GetCardinality() == 0 {
		return logs, nil
	}
	addrMap := make(map[common.Address]struct{}, len(crit.Addresses))
	for _, v := range crit.Addresses {
		addrMap[v] = struct{}{}
	}
	iter := blockNumbers.Iterator()
	for iter.HasNext() {
		if err = ctx.Err(); err != nil {
			return nil, err
		}

		blockNumber := uint64(iter.Next())
		var logIndex uint
		var txIndex uint
		var blockLogs []*types.Log

		it, err := tx.Prefix(kv.Log, common2.EncodeTs(blockNumber))
		if err != nil {
			return nil, err
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				return logs, err
			}

			var logs types.Logs
			if err := cbor.Unmarshal(&logs, bytes.NewReader(v)); err != nil {
				return logs, fmt.Errorf("receipt unmarshal failed:  %w", err)
			}
			for _, log := range logs {
				log.Index = logIndex
				logIndex++
			}
			filtered := logs.Filter(addrMap, crit.Topics)
			if len(filtered) == 0 {
				continue
			}
			txIndex = uint(binary.BigEndian.Uint32(k[8:]))
			for _, log := range filtered {
				log.TxIndex = txIndex
			}
			blockLogs = append(blockLogs, filtered...)
		}
		if len(blockLogs) == 0 {
			continue
		}

		blockHash, err := rawdb.ReadCanonicalHash(tx, blockNumber)
		if err != nil {
			return nil, err
		}

		body, err := api._blockReader.BodyWithTransactions(ctx, tx, blockHash, blockNumber)
		if err != nil {
			return nil, err
		}
		if body == nil {
			return nil, fmt.Errorf("block not found %d", blockNumber)
		}
		for _, log := range blockLogs {
			log.BlockNumber = blockNumber
			log.BlockHash = blockHash
			// bor transactions are at the end of the bodies transactions (added manually but not actually part of the block)
			if log.TxIndex == uint(len(body.Transactions)) {
				log.TxHash = types.ComputeBorTxHash(blockNumber, blockHash)
			} else {
				log.TxHash = body.Transactions[log.TxIndex].Hash()
			}
		}
		logs = append(logs, blockLogs...)
	}

	return logs, nil
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
func getTopicsBitmap(c kv.Tx, topics [][]common.Hash, from, to uint32) (*roaring.Bitmap, error) {
	var result *roaring.Bitmap
	for _, sub := range topics {
		var bitmapForORing *roaring.Bitmap
		for _, topic := range sub {
			m, err := bitmapdb.Get(c, kv.LogTopicIndex, topic[:], from, to)
			if err != nil {
				return nil, err
			}
			if bitmapForORing == nil {
				bitmapForORing = m
				continue
			}
			bitmapForORing.Or(m)
		}

		if bitmapForORing == nil {
			continue
		}
		if result == nil {
			result = bitmapForORing
			continue
		}

		result = roaring.And(bitmapForORing, result)
	}
	return result, nil
}

func (api *APIImpl) getLogsV3(ctx context.Context, tx kv.TemporalTx, begin, end uint64, crit filters.FilterCriteria) ([]*types.Log, error) {
	logs := []*types.Log{}

	var fromTxNum, toTxNum uint64
	var err error
	if begin > 0 {
		fromTxNum, err = rawdb.TxNums.Min(tx, begin)
		if err != nil {
			return nil, err
		}
	}
	toTxNum, err = rawdb.TxNums.Max(tx, end) // end is an inclusive bound
	if err != nil {
		return nil, err
	}

	txNumbers := roaring64.New()
	txNumbers.AddRange(fromTxNum, toTxNum) // [min,max)

	topicsBitmap, err := getTopicsBitmapV3(tx, crit.Topics, fromTxNum, toTxNum)
	if err != nil {
		return nil, err
	}

	if topicsBitmap != nil {
		txNumbers.And(topicsBitmap)
	}

	var addrBitmap *roaring64.Bitmap
	for _, addr := range crit.Addresses {
		var bitmapForORing roaring64.Bitmap
		it, err := tx.IndexRange(temporal.LogAddrIdx, addr.Bytes(), fromTxNum, toTxNum)
		if err != nil {
			return nil, err
		}
		for it.HasNext() {
			n, err := it.NextBatch()
			if err != nil {
				return nil, err
			}
			bitmapForORing.AddMany(n)
		}
		if addrBitmap == nil {
			addrBitmap = &bitmapForORing
			continue
		}
		addrBitmap = roaring64.Or(addrBitmap, &bitmapForORing)
	}

	if addrBitmap != nil {
		txNumbers.And(addrBitmap)
	}

	if txNumbers.GetCardinality() == 0 {
		return logs, nil
	}
	var lastBlockNum uint64
	var blockHash common.Hash
	var header *types.Header
	var signer *types.Signer
	var rules *params.Rules
	var skipAnalysis bool
	stateReader := state.NewHistoryReaderV3()
	stateReader.SetTx(tx)
	ibs := state.New(stateReader)

	//stateReader.SetTrace(true)
	iter := txNumbers.Iterator()

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()

	addrMap := make(map[common.Address]struct{}, len(crit.Addresses))
	for _, v := range crit.Addresses {
		addrMap[v] = struct{}{}
	}

	evm := vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chainConfig, vm.Config{})
	vmConfig := vm.Config{SkipAnalysis: skipAnalysis}
	var blockCtx evmtypes.BlockContext

	var minTxNumInBlock, maxTxNumInBlock uint64 // end is an inclusive bound
	var blockNum uint64
	var ok bool
	for iter.HasNext() {
		txNum := iter.Next()

		// txNums are sorted, it means blockNum will not change until `txNum < maxTxNum`

		if maxTxNumInBlock == 0 || txNum > maxTxNumInBlock {
			// Find block number
			ok, blockNum, err = rawdb.TxNums.FindBlockNum(tx, txNum)
			if err != nil {
				return nil, err
			}
		}
		if !ok {
			return nil, nil
		}

		// if block number changed, calculate all related field
		if blockNum > lastBlockNum {
			if header, err = api._blockReader.HeaderByNumber(ctx, tx, blockNum); err != nil {
				return nil, err
			}
			lastBlockNum = blockNum
			blockHash = header.Hash()
			signer = types.MakeSigner(chainConfig, blockNum)
			if chainConfig == nil {
				log.Warn("chainConfig is nil")
			}
			if header == nil {
				log.Warn("header is nil")
			}
			rules = chainConfig.Rules(blockNum, header.Time)
			vmConfig.SkipAnalysis = core.SkipAnalysis(chainConfig, blockNum)

			minTxNumInBlock, err = rawdb.TxNums.Min(tx, blockNum)
			if err != nil {
				return nil, err
			}
			maxTxNumInBlock, err = rawdb.TxNums.Max(tx, blockNum)
			if err != nil {
				return nil, err
			}
			blockCtx = transactions.NewEVMBlockContext(engine, header, true /* requireCanonical */, tx, api._blockReader)
		}

		txIndex := int(txNum) - int(minTxNumInBlock) - 1
		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d\n", txNum, blockNum, txIndex)
		txn, err := api._txnReader.TxnByIdxInBlock(ctx, tx, blockNum, txIndex)
		if err != nil {
			return nil, err
		}
		if txn == nil {
			continue
		}
		stateReader.SetTxNum(txNum)
		txHash := txn.Hash()
		msg, err := txn.AsMessage(*signer, header.BaseFee, rules)
		if err != nil {
			return nil, err
		}

		ibs.Reset()
		ibs.Prepare(txHash, blockHash, txIndex)

		evm.ResetBetweenBlocks(blockCtx, core.NewEVMTxContext(msg), ibs, vmConfig, rules)

		gp := new(core.GasPool).AddGas(msg.Gas())
		_, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			return nil, fmt.Errorf("%w: blockNum=%d, txNum=%d, %s", err, blockNum, txNum, ibs.Error())
		}

		rawLogs := ibs.GetLogs(txHash)

		//TODO: logIndex within the block! no way to calc it now
		logIndex := uint(0)
		for _, log := range rawLogs {
			log.Index = logIndex
			logIndex++
		}
		filtered := types.Logs(rawLogs).Filter(addrMap, crit.Topics)
		for _, log := range filtered {
			log.BlockNumber = blockNum
			log.BlockHash = blockHash
			log.TxHash = txHash
		}
		logs = append(logs, filtered...)
	}

	//stats := api._agg.GetAndResetStats()
	//log.Info("Finished", "duration", time.Since(start), "history queries", stats.HistoryQueries, "ef search duration", stats.EfSearchTime)
	return logs, nil
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
func getTopicsBitmapV3(tx kv.TemporalTx, topics [][]common.Hash, from, to uint64) (*roaring64.Bitmap, error) {
	var result *roaring64.Bitmap
	for _, sub := range topics {
		var bitmapForORing roaring64.Bitmap
		for _, topic := range sub {
			it, err := tx.IndexRange(temporal.LogTopicIdx, topic.Bytes(), from, to)
			if err != nil {
				return nil, err
			}
			for it.HasNext() {
				n, err := it.NextBatch()
				if err != nil {
					return nil, err
				}
				bitmapForORing.AddMany(n)
			}
		}

		if bitmapForORing.GetCardinality() == 0 {
			continue
		}
		if result == nil {
			result = &bitmapForORing
			continue
		}
		result = roaring64.And(&bitmapForORing, result)
	}
	return result, nil
}

// GetTransactionReceipt implements eth_getTransactionReceipt. Returns the receipt of a transaction given the transaction's hash.
func (api *APIImpl) GetTransactionReceipt(ctx context.Context, txnHash common.Hash) (map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var blockNum uint64
	var ok bool

	blockNum, ok, err = api.txnLookup(ctx, tx, txnHash)
	if err != nil {
		return nil, err
	}

	cc, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	if !ok && cc.Bor == nil {
		return nil, nil
	}

	// if not ok and cc.Bor != nil then we might have a bor transaction.
	// Note that Private API returns 0 if transaction is not found.
	if !ok || blockNum == 0 {
		blockNumPtr, err := rawdb.ReadBorTxLookupEntry(tx, txnHash)
		if err != nil {
			return nil, err
		}
		if blockNumPtr == nil {
			return nil, nil
		}

		blockNum = *blockNumPtr
	}

	block, err := api.blockByNumberWithSenders(tx, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
	}

	var txnIndex uint64
	var txn types.Transaction
	for idx, transaction := range block.Transactions() {
		if transaction.Hash() == txnHash {
			txn = transaction
			txnIndex = uint64(idx)
			break
		}
	}

	var borTx types.Transaction
	if txn == nil {
		borTx, _, _, _ = rawdb.ReadBorTransactionForBlock(tx, block)
		if borTx == nil {
			return nil, nil
		}
	}

	receipts, err := api.getReceipts(ctx, tx, cc, block, block.Body().SendersFromTxs())
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %w", err)
	}
	if len(receipts) <= int(txnIndex) {
		return nil, fmt.Errorf("block has less receipts than expected: %d <= %d, block: %d", len(receipts), int(txnIndex), blockNum)
	}

	if txn == nil {
		borReceipt, err := rawdb.ReadBorReceipt(tx, block.Hash(), blockNum, receipts)
		if err != nil {
			return nil, err
		}
		if borReceipt == nil {
			return nil, nil
		}
		return marshalReceipt(borReceipt, borTx, cc, block, txnHash, false), nil
	}

	return marshalReceipt(receipts[txnIndex], block.Transactions()[txnIndex], cc, block, txnHash, true), nil
}

// GetBlockReceipts - receipts for individual block
// func (api *APIImpl) GetBlockReceipts(ctx context.Context, number rpc.BlockNumber) ([]map[string]interface{}, error) {
func (api *APIImpl) GetBlockReceipts(ctx context.Context, number rpc.BlockNumber) ([]map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, _, _, err := rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithNumber(number), tx, api.filters)
	if err != nil {
		return nil, err
	}
	block, err := api.blockByNumberWithSenders(tx, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	receipts, err := api.getReceipts(ctx, tx, chainConfig, block, block.Body().SendersFromTxs())
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %w", err)
	}
	result := make([]map[string]interface{}, 0, len(receipts))
	for _, receipt := range receipts {
		txn := block.Transactions()[receipt.TransactionIndex]
		result = append(result, marshalReceipt(receipt, txn, chainConfig, block, txn.Hash(), true))
	}

	if chainConfig.Bor != nil {
		borTx, _, _, _ := rawdb.ReadBorTransactionForBlock(tx, block)
		if borTx != nil {
			borReceipt, err := rawdb.ReadBorReceipt(tx, block.Hash(), block.NumberU64(), receipts)
			if err != nil {
				return nil, err
			}
			if borReceipt != nil {
				result = append(result, marshalReceipt(borReceipt, borTx, chainConfig, block, borReceipt.TxHash, false))
			}
		}
	}

	return result, nil
}

func marshalReceipt(receipt *types.Receipt, txn types.Transaction, chainConfig *params.ChainConfig, block *types.Block, txnHash common.Hash, signed bool) map[string]interface{} {
	var chainId *big.Int
	switch t := txn.(type) {
	case *types.LegacyTx:
		if t.Protected() {
			chainId = types.DeriveChainId(&t.V).ToBig()
		}
	case *types.AccessListTx:
		chainId = t.ChainID.ToBig()
	case *types.DynamicFeeTransaction:
		chainId = t.ChainID.ToBig()
	}

	var from common.Address
	if signed {
		signer := types.LatestSignerForChainID(chainId)
		from, _ = txn.Sender(*signer)
	}

	fields := map[string]interface{}{
		"blockHash":         receipt.BlockHash,
		"blockNumber":       hexutil.Uint64(receipt.BlockNumber.Uint64()),
		"transactionHash":   txnHash,
		"transactionIndex":  hexutil.Uint64(receipt.TransactionIndex),
		"from":              from,
		"to":                txn.GetTo(),
		"type":              hexutil.Uint(txn.Type()),
		"gasUsed":           hexutil.Uint64(receipt.GasUsed),
		"cumulativeGasUsed": hexutil.Uint64(receipt.CumulativeGasUsed),
		"contractAddress":   nil,
		"logs":              receipt.Logs,
		"logsBloom":         types.CreateBloom(types.Receipts{receipt}),
	}

	if !chainConfig.IsLondon(block.NumberU64()) {
		fields["effectiveGasPrice"] = hexutil.Uint64(txn.GetPrice().Uint64())
	} else {
		baseFee, _ := uint256.FromBig(block.BaseFee())
		gasPrice := new(big.Int).Add(block.BaseFee(), txn.GetEffectiveGasTip(baseFee).ToBig())
		fields["effectiveGasPrice"] = hexutil.Uint64(gasPrice.Uint64())
	}
	// Assign receipt status.
	fields["status"] = hexutil.Uint64(receipt.Status)
	if receipt.Logs == nil {
		fields["logs"] = [][]*types.Log{}
	}
	// If the ContractAddress is 20 0x0 bytes, assume it is not a contract creation
	if receipt.ContractAddress != (common.Address{}) {
		fields["contractAddress"] = receipt.ContractAddress
	}
	return fields
}

func includes(addresses []common.Address, a common.Address) bool {
	for _, addr := range addresses {
		if addr == a {
			return true
		}
	}
	return false
}

// filterLogs creates a slice of logs matching the given criteria.
func filterLogsOld(logs []*types.Log, addresses []common.Address, topics [][]common.Hash) []*types.Log {
	result := make(types.Logs, 0, len(logs))
Logs:
	for _, log := range logs {
		if len(addresses) > 0 && !includes(addresses, log.Address) {
			continue
		}
		// If the to filtered topics is greater than the amount of topics in logs, skip.
		if len(topics) > len(log.Topics) {
			continue Logs
		}
		for i, sub := range topics {
			match := len(sub) == 0 // empty rule set == wildcard
			for _, topic := range sub {
				if log.Topics[i] == topic {
					match = true
					break
				}
			}
			if !match {
				continue Logs
			}
		}
		result = append(result, log)
	}
	return result
}
