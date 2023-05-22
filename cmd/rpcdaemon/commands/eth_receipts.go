package commands

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/RoaringBitmap/roaring"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/transactions"
)

func (api *BaseAPI) getReceipts(ctx context.Context, tx kv.Tx, chainConfig *chain.Config, block *types.Block, senders []common.Address) (types.Receipts, error) {
	if cached := rawdb.ReadReceipts(tx, block, senders); cached != nil {
		return cached, nil
	}
	engine := api.engine()

	_, _, _, ibs, _, err := transactions.ComputeTxEnv(ctx, engine, block, chainConfig, api._blockReader, tx, 0, api.historyV3(tx))
	if err != nil {
		return nil, err
	}

	usedGas := new(uint64)
	gp := new(core.GasPool).AddGas(block.GasLimit()).AddDataGas(params.MaxDataGasPerBlock)

	noopWriter := state.NewNoopWriter()

	receipts := make(types.Receipts, len(block.Transactions()))

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, e := api._blockReader.Header(ctx, tx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h
	}
	header := block.Header()
	excessDataGas := header.ParentExcessDataGas(getHeader)
	for i, txn := range block.Transactions() {
		ibs.SetTxContext(txn.Hash(), block.Hash(), i)
		receipt, _, err := core.ApplyTransaction(chainConfig, core.GetHashFn(header, getHeader), engine, nil, gp, ibs, noopWriter, header, txn, usedGas, vm.Config{}, excessDataGas)
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
	if err := applyFilters(blockNumbers, tx, begin, end, crit); err != nil {
		return logs, err
	}
	if blockNumbers.IsEmpty() {
		return logs, nil
	}
	addrMap := make(map[common.Address]struct{}, len(crit.Addresses))
	for _, v := range crit.Addresses {
		addrMap[v] = struct{}{}
	}
	iter := blockNumbers.Iterator()
	for iter.HasNext() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		blockNumber := uint64(iter.Next())
		var logIndex uint
		var txIndex uint
		var blockLogs []*types.Log

		it, err := tx.Prefix(kv.Log, hexutility.EncodeTs(blockNumber))
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
func getTopicsBitmap(c kv.Tx, topics [][]common.Hash, from, to uint64) (*roaring.Bitmap, error) {
	var result *roaring.Bitmap
	for _, sub := range topics {
		var bitmapForORing *roaring.Bitmap
		for _, topic := range sub {
			m, err := bitmapdb.Get(c, kv.LogTopicIndex, topic[:], uint32(from), uint32(to))
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
func getAddrsBitmap(tx kv.Tx, addrs []common.Address, from, to uint64) (*roaring.Bitmap, error) {
	if len(addrs) == 0 {
		return nil, nil
	}
	rx := make([]*roaring.Bitmap, len(addrs))
	defer func() {
		for _, bm := range rx {
			bitmapdb.ReturnToPool(bm)
		}
	}()
	for idx, addr := range addrs {
		m, err := bitmapdb.Get(tx, kv.LogAddressIndex, addr[:], uint32(from), uint32(to))
		if err != nil {
			return nil, err
		}
		rx[idx] = m
	}
	return roaring.FastOr(rx...), nil
}

func applyFilters(out *roaring.Bitmap, tx kv.Tx, begin, end uint64, crit filters.FilterCriteria) error {
	out.AddRange(begin, end+1) // [from,to)
	topicsBitmap, err := getTopicsBitmap(tx, crit.Topics, begin, end)
	if err != nil {
		return err
	}
	if topicsBitmap != nil {
		out.And(topicsBitmap)
	}
	addrBitmap, err := getAddrsBitmap(tx, crit.Addresses, begin, end)
	if err != nil {
		return err
	}
	if addrBitmap != nil {
		out.And(addrBitmap)
	}
	return nil
}

/*

func applyFiltersV3(out *roaring64.Bitmap, tx kv.TemporalTx, begin, end uint64, crit filters.FilterCriteria) error {
	//[from,to)
	var fromTxNum, toTxNum uint64
	var err error
	if begin > 0 {
		fromTxNum, err = rawdbv3.TxNums.Min(tx, begin)
		if err != nil {
			return err
		}
	}
	toTxNum, err = rawdbv3.TxNums.Max(tx, end)
	if err != nil {
		return err
	}
	toTxNum++

	out.AddRange(fromTxNum, toTxNum) // [from,to)
	topicsBitmap, err := getTopicsBitmapV3(tx, crit.Topics, fromTxNum, toTxNum)
	if err != nil {
		return err
	}
	if topicsBitmap != nil {
		out.And(topicsBitmap)
	}
	addrBitmap, err := getAddrsBitmapV3(tx, crit.Addresses, fromTxNum, toTxNum)
	if err != nil {
		return err
	}
	if addrBitmap != nil {
		out.And(addrBitmap)
	}
	return nil
}
*/

func applyFiltersV3(tx kv.TemporalTx, begin, end uint64, crit filters.FilterCriteria) (out iter.U64, err error) {
	//[from,to)
	var fromTxNum, toTxNum uint64
	if begin > 0 {
		fromTxNum, err = rawdbv3.TxNums.Min(tx, begin)
		if err != nil {
			return out, err
		}
	}
	toTxNum, err = rawdbv3.TxNums.Max(tx, end)
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
			out = iter.Intersect[uint64](out, addrBitmap, -1)
		}
	}
	if out == nil {
		out = iter.Range[uint64](fromTxNum, toTxNum)
	}
	return out, nil
}

func (api *APIImpl) getLogsV3(ctx context.Context, tx kv.TemporalTx, begin, end uint64, crit filters.FilterCriteria) ([]*types.Log, error) {
	logs := []*types.Log{}

	txNumbers, err := applyFiltersV3(tx, begin, end, crit)
	if err != nil {
		return logs, err
	}

	addrMap := make(map[common.Address]struct{}, len(crit.Addresses))
	for _, v := range crit.Addresses {
		addrMap[v] = struct{}{}
	}

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	exec := txnExecutor(tx, chainConfig, api.engine(), api._blockReader, nil)

	var blockHash common.Hash
	var header *types.Header

	iter := MapTxNum2BlockNum(tx, txNumbers)
	for iter.HasNext() {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		txNum, blockNum, txIndex, isFinalTxn, blockNumChanged, err := iter.Next()
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
			blockHash = header.Hash()
			exec.changeBlock(header)
		}

		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d, maxTxNumInBlock=%d,mixTxNumInBlock=%d\n", txNum, blockNum, txIndex, maxTxNumInBlock, minTxNumInBlock)
		txn, err := api._txnReader.TxnByIdxInBlock(ctx, tx, blockNum, txIndex)
		if err != nil {
			return nil, err
		}
		if txn == nil {
			continue
		}
		rawLogs, _, err := exec.execTx(txNum, txIndex, txn)
		if err != nil {
			return nil, err
		}

		//TODO: logIndex within the block! no way to calc it now
		//logIndex := uint(0)
		//for _, log := range rawLogs {
		//	log.Index = logIndex
		//	logIndex++
		//}
		filtered := types.Logs(rawLogs).Filter(addrMap, crit.Topics)
		for _, log := range filtered {
			log.BlockNumber = blockNum
			log.BlockHash = blockHash
			log.TxHash = txn.Hash()
		}
		logs = append(logs, filtered...)
	}

	//stats := api._agg.GetAndResetStats()
	//log.Info("Finished", "duration", time.Since(start), "history queries", stats.HistoryQueries, "ef search duration", stats.EfSearchTime)
	return logs, nil
}

type intraBlockExec struct {
	ibs         *state.IntraBlockState
	stateReader *state.HistoryReaderV3
	engine      consensus.EngineReader
	tx          kv.TemporalTx
	br          services.FullBlockReader
	chainConfig *chain.Config
	evm         *vm.EVM

	tracer GenericTracer

	// calculated by .changeBlock()
	blockHash common.Hash
	blockNum  uint64
	header    *types.Header
	blockCtx  *evmtypes.BlockContext
	rules     *chain.Rules
	signer    *types.Signer
	vmConfig  *vm.Config
}

func txnExecutor(tx kv.TemporalTx, chainConfig *chain.Config, engine consensus.EngineReader, br services.FullBlockReader, tracer GenericTracer) *intraBlockExec {
	stateReader := state.NewHistoryReaderV3()
	stateReader.SetTx(tx)

	ie := &intraBlockExec{
		tx:          tx,
		engine:      engine,
		chainConfig: chainConfig,
		br:          br,
		stateReader: stateReader,
		tracer:      tracer,
		evm:         vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chainConfig, vm.Config{}),
		vmConfig:    &vm.Config{},
		ibs:         state.New(stateReader),
	}
	if tracer != nil {
		ie.vmConfig = &vm.Config{Debug: true, Tracer: tracer}
	}
	return ie
}

func (e *intraBlockExec) changeBlock(header *types.Header) {
	e.blockNum = header.Number.Uint64()
	blockCtx := transactions.NewEVMBlockContext(e.engine, header, true /* requireCanonical */, e.tx, e.br)
	e.blockCtx = &blockCtx
	e.blockHash = header.Hash()
	e.header = header
	e.rules = e.chainConfig.Rules(e.blockNum, header.Time)
	e.signer = types.MakeSigner(e.chainConfig, e.blockNum)
	e.vmConfig.SkipAnalysis = core.SkipAnalysis(e.chainConfig, e.blockNum)
}

func (e *intraBlockExec) execTx(txNum uint64, txIndex int, txn types.Transaction) ([]*types.Log, *core.ExecutionResult, error) {
	e.stateReader.SetTxNum(txNum)
	txHash := txn.Hash()
	e.ibs.Reset()
	e.ibs.SetTxContext(txHash, e.blockHash, txIndex)
	gp := new(core.GasPool).AddGas(txn.GetGas()).AddDataGas(txn.GetDataGas())
	msg, err := txn.AsMessage(*e.signer, e.header.BaseFee, e.rules)
	if err != nil {
		return nil, nil, err
	}
	e.evm.ResetBetweenBlocks(*e.blockCtx, core.NewEVMTxContext(msg), e.ibs, *e.vmConfig, e.rules)
	res, err := core.ApplyMessage(e.evm, msg, gp, true /* refunds */, false /* gasBailout */)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: blockNum=%d, txNum=%d, %s", err, e.blockNum, txNum, e.ibs.Error())
	}
	if e.vmConfig.Tracer != nil {
		if e.tracer.Found() {
			e.tracer.SetTransaction(txn)
		}
	}
	return e.ibs.GetLogs(txHash), res, nil
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
func getTopicsBitmapV3(tx kv.TemporalTx, topics [][]common.Hash, from, to uint64) (res iter.U64, err error) {
	for _, sub := range topics {

		var topicsUnion iter.U64
		for _, topic := range sub {
			it, err := tx.IndexRange(temporal.LogTopicIdx, topic.Bytes(), int(from), int(to), order.Asc, kv.Unlim)
			if err != nil {
				return nil, err
			}
			topicsUnion = iter.Union[uint64](topicsUnion, it, order.Asc, -1)
		}

		if res == nil {
			res = topicsUnion
			continue
		}
		res = iter.Intersect[uint64](res, topicsUnion, -1)
	}
	return res, nil
}

func getAddrsBitmapV3(tx kv.TemporalTx, addrs []common.Address, from, to uint64) (res iter.U64, err error) {
	for _, addr := range addrs {
		it, err := tx.IndexRange(temporal.LogAddrIdx, addr[:], int(from), int(to), true, kv.Unlim)
		if err != nil {
			return nil, err
		}
		res = iter.Union[uint64](res, it, order.Asc, -1)
	}
	return res, nil
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

	var edg *big.Int
	if n := block.Number().Uint64(); n > 0 {
		if parentHeader, err := api._blockReader.Header(ctx, tx, block.ParentHash(), n-1); err != nil {
			return nil, err
		} else {
			edg = parentHeader.ExcessDataGas
		}
	}

	if txn == nil {
		borReceipt, err := rawdb.ReadBorReceipt(tx, block.Hash(), blockNum, receipts)
		if err != nil {
			return nil, err
		}
		if borReceipt == nil {
			return nil, nil
		}
		return marshalReceipt(borReceipt, borTx, cc, block.HeaderNoCopy(), txnHash, false, edg), nil
	}

	if len(receipts) <= int(txnIndex) {
		return nil, fmt.Errorf("block has less receipts than expected: %d <= %d, block: %d", len(receipts), int(txnIndex), blockNum)
	}

	return marshalReceipt(receipts[txnIndex], block.Transactions()[txnIndex], cc, block.HeaderNoCopy(), txnHash, true, edg), nil
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
	var edg *big.Int
	if n := block.Number().Uint64(); n > 0 {
		if parentHeader, err := api._blockReader.Header(ctx, tx, block.ParentHash(), n-1); err != nil {
			return nil, err
		} else {
			edg = parentHeader.ExcessDataGas
		}
	}
	for _, receipt := range receipts {
		txn := block.Transactions()[receipt.TransactionIndex]
		result = append(result, marshalReceipt(receipt, txn, chainConfig, block.HeaderNoCopy(), txn.Hash(), true, edg))
	}

	if chainConfig.Bor != nil {
		borTx, _, _, _ := rawdb.ReadBorTransactionForBlock(tx, block)
		if borTx != nil {
			borReceipt, err := rawdb.ReadBorReceipt(tx, block.Hash(), block.NumberU64(), receipts)
			if err != nil {
				return nil, err
			}
			if borReceipt != nil {
				result = append(result, marshalReceipt(borReceipt, borTx, chainConfig, block.HeaderNoCopy(), borReceipt.TxHash, false, edg))
			}
		}
	}

	return result, nil
}

func marshalReceipt(receipt *types.Receipt, txn types.Transaction, chainConfig *chain.Config, header *types.Header, txnHash common.Hash, signed bool, excessDataGas *big.Int) map[string]interface{} {
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
		// case *types.SignedBlobTx: // TODO: needs eip-4844 signer
		// 	chainId = t.GetChainID().ToBig()
	default:
		chainId = txn.GetChainID().ToBig()
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

	if !chainConfig.IsLondon(header.Number.Uint64()) {
		fields["effectiveGasPrice"] = hexutil.Uint64(txn.GetPrice().Uint64())
	} else {
		baseFee, _ := uint256.FromBig(header.BaseFee)
		gasPrice := new(big.Int).Add(header.BaseFee, txn.GetEffectiveGasTip(baseFee).ToBig())
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
	// Set derived blob related fields
	numBlobs := len(txn.GetDataHashes())
	if numBlobs > 0 {
		if excessDataGas == nil {
			log.Warn("excess data gas not set when trying to marshal blob tx")
		} else {
			dataGasPrice, err := misc.GetDataGasPrice(excessDataGas)
			if err != nil {
				log.Error(err.Error())
			}
			fields["dataGasPrice"] = dataGasPrice
			fields["dataGasUsed"] = misc.GetDataGasUsed(numBlobs)
		}
	}
	return fields
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
	it          iter.U64
	tx          kv.Tx
	orderAscend bool

	blockNum                         uint64
	minTxNumInBlock, maxTxNumInBlock uint64
}

func MapTxNum2BlockNum(tx kv.Tx, it iter.U64) *MapTxNum2BlockNumIter {
	return &MapTxNum2BlockNumIter{tx: tx, it: it, orderAscend: true}
}
func MapDescendTxNum2BlockNum(tx kv.Tx, it iter.U64) *MapTxNum2BlockNumIter {
	return &MapTxNum2BlockNumIter{tx: tx, it: it, orderAscend: false}
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
		ok, i.blockNum, err = rawdbv3.TxNums.FindBlockNum(i.tx, txNum)
		if err != nil {
			return
		}
		if !ok {
			return txNum, i.blockNum, txIndex, isFinalTxn, blockNumChanged, fmt.Errorf("can't find blockNumber by txnID=%d", txNum)
		}
	}
	blockNum = i.blockNum

	// if block number changed, calculate all related field
	if blockNumChanged {
		i.minTxNumInBlock, err = rawdbv3.TxNums.Min(i.tx, blockNum)
		if err != nil {
			return
		}
		i.maxTxNumInBlock, err = rawdbv3.TxNums.Max(i.tx, blockNum)
		if err != nil {
			return
		}
	}

	txIndex = int(txNum) - int(i.minTxNumInBlock) - 1
	isFinalTxn = txNum == i.maxTxNumInBlock
	return
}
