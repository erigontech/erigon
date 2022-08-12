package commands

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/log/v3"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/transactions"
)

func (api *BaseAPI) getReceipts(ctx context.Context, tx kv.Tx, chainConfig *params.ChainConfig, block *types.Block, senders []common.Address) (types.Receipts, error) {
	if cached := rawdb.ReadReceipts(tx, block, senders); cached != nil {
		return cached, nil
	}

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, e := api._blockReader.Header(ctx, tx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h
	}
	contractHasTEVM := ethdb.GetHasTEVM(tx)
	_, _, _, ibs, _, err := transactions.ComputeTxEnv(ctx, block, chainConfig, getHeader, contractHasTEVM, ethash.NewFaker(), tx, block.Hash(), 0)
	if err != nil {
		return nil, err
	}

	usedGas := new(uint64)
	gp := new(core.GasPool).AddGas(block.GasLimit())

	ethashFaker := ethash.NewFaker()
	noopWriter := state.NewNoopWriter()

	receipts := make(types.Receipts, len(block.Transactions()))

	for i, txn := range block.Transactions() {
		ibs.Prepare(txn.Hash(), block.Hash(), i)
		header := block.Header()
		receipt, _, err := core.ApplyTransaction(chainConfig, core.GetHashFn(header, getHeader), ethashFaker, nil, gp, ibs, noopWriter, header, txn, usedGas, vm.Config{}, contractHasTEVM)
		if err != nil {
			return nil, err
		}
		receipt.BlockHash = block.Hash()
		receipts[i] = receipt
	}

	return receipts, nil
}

// GetLogs implements eth_getLogs. Returns an array of logs matching a given filter object.
func (api *APIImpl) GetLogs(ctx context.Context, crit filters.FilterCriteria) ([]*types.Log, error) {
	start := time.Now()
	var begin, end uint64
	logs := []*types.Log{}

	tx, beginErr := api.db.BeginRo(ctx)
	if beginErr != nil {
		return logs, beginErr
	}
	defer tx.Rollback()

	if crit.BlockHash != nil {
		number := rawdb.ReadHeaderNumber(tx, *crit.BlockHash)
		if number == nil {
			return nil, fmt.Errorf("block not found: %x", *crit.BlockHash)
		}
		begin = *number
		end = *number
	} else {
		// Convert the RPC block numbers into internal representations
		latest, err := getLatestBlockNumber(tx)
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

	var fromTxNum, toTxNum uint64
	if begin > 0 {
		fromTxNum = api._txNums[begin-1]
	}
	toTxNum = api._txNums[end] // end is an inclusive bound

	txNumbers := roaring64.New()
	txNumbers.AddRange(fromTxNum, toTxNum) // [min,max)

	ac := api._agg.MakeContext()

	topicsBitmap, err := getTopicsBitmap(ac, tx, crit.Topics, fromTxNum, toTxNum)
	if err != nil {
		return nil, err
	}

	if topicsBitmap != nil {
		txNumbers.And(topicsBitmap)
	}

	var addrBitmap *roaring64.Bitmap
	for _, addr := range crit.Addresses {
		var bitmapForORing roaring64.Bitmap
		it := ac.LogAddrIterator(addr.Bytes(), fromTxNum, toTxNum, nil)
		for it.HasNext() {
			bitmapForORing.Add(it.Next())
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
	var lastBlockHash common.Hash
	var lastHeader *types.Header
	var lastSigner *types.Signer
	var lastRules *params.Rules
	stateReader := state.NewHistoryReader22(ac, nil /* ReadIndices */)
	iter := txNumbers.Iterator()

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	for iter.HasNext() {
		txNum := iter.Next()
		// Find block number
		blockNum := uint64(sort.Search(len(api._txNums), func(i int) bool {
			return api._txNums[i] > txNum
		}))
		if blockNum > lastBlockNum {
			if lastHeader, err = api._blockReader.HeaderByNumber(ctx, nil, blockNum); err != nil {
				return nil, err
			}
			lastBlockNum = blockNum
			lastBlockHash = lastHeader.Hash()
			lastSigner = types.MakeSigner(chainConfig, blockNum)
			lastRules = chainConfig.Rules(blockNum)
		}
		var startTxNum uint64
		if blockNum > 0 {
			startTxNum = api._txNums[blockNum-1]
		}
		txIndex := txNum - startTxNum - 1
		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d\n", txNum, blockNum, txIndex)
		txn, err := api._txnReader.TxnByIdxInBlock(ctx, nil, blockNum, int(txIndex))
		if err != nil {
			return nil, err
		}
		txHash := txn.Hash()
		msg, err := txn.AsMessage(*lastSigner, lastHeader.BaseFee, lastRules)
		if err != nil {
			return nil, err
		}
		contractHasTEVM := func(contractHash common.Hash) (bool, error) { return false, nil }
		blockCtx, txCtx := transactions.GetEvmContext(msg, lastHeader, true /* requireCanonical */, tx, contractHasTEVM, api._blockReader)
		stateReader.SetTxNum(txNum)
		vmConfig := vm.Config{}
		vmConfig.SkipAnalysis = core.SkipAnalysis(chainConfig, blockNum)
		ibs := state.New(stateReader)
		evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vmConfig)

		gp := new(core.GasPool).AddGas(msg.Gas())
		ibs.Prepare(txHash, lastBlockHash, int(txIndex))
		_, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			return nil, err
		}
		filtered := filterLogs(ibs.GetLogs(txHash), crit.Addresses, crit.Topics)
		for _, log := range filtered {
			log.BlockNumber = blockNum
			log.BlockHash = lastBlockHash
			log.TxHash = txHash
			log.Index = 0
		}
		logs = append(logs, filtered...)
	}
	stats := api._agg.GetAndResetStats()
	log.Info("Finished", "duration", time.Since(start), "history queries", stats.HistoryQueries, "ef search duration", stats.EfSearchTime)
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
func getTopicsBitmap(ac *libstate.AggregatorContext, c kv.Tx, topics [][]common.Hash, from, to uint64) (*roaring64.Bitmap, error) {
	var result *roaring64.Bitmap
	for _, sub := range topics {
		var bitmapForORing roaring64.Bitmap
		for _, topic := range sub {
			it := ac.LogTopicIterator(topic.Bytes(), from, to, nil)
			for it.HasNext() {
				bitmapForORing.Add(it.Next())
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
func (api *APIImpl) GetTransactionReceipt(ctx context.Context, hash common.Hash) (map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var blockNum uint64
	var ok bool

	blockNum, ok, err = api.txnLookup(ctx, tx, hash)
	if !ok || blockNum == 0 {
		// It is not an ideal solution (ideal solution requires extending TxnLookupReply proto type to include bool flag indicating absense of result),
		// but 0 block number is used here to mean that the transaction is not found
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	block, err := api.blockByNumberWithSenders(tx, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
	}

	cc, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	var txnIndex uint64
	var txn types.Transaction
	for idx, transaction := range block.Transactions() {
		if transaction.Hash() == hash {
			txn = transaction
			txnIndex = uint64(idx)
			break
		}
	}

	if txn == nil {
		if cc.Bor == nil {
			return nil, nil
		}

		borTx, blockHash, _, _, err := rawdb.ReadBorTransactionForBlockNumber(tx, blockNum)
		if err != nil {
			return nil, err
		}
		if borTx == nil {
			return nil, nil
		}
		borReceipt := rawdb.ReadBorReceipt(tx, blockHash, blockNum)
		return marshalReceipt(borReceipt, borTx, cc, block, hash), nil
	}

	receipts, err := api.getReceipts(ctx, tx, cc, block, block.Body().SendersFromTxs())
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %w", err)
	}
	if len(receipts) <= int(txnIndex) {
		return nil, fmt.Errorf("block has less receipts than expected: %d <= %d, block: %d", len(receipts), int(txnIndex), blockNum)
	}
	return marshalReceipt(receipts[txnIndex], block.Transactions()[txnIndex], cc, block, hash), nil
}

// GetBlockReceipts - receipts for individual block
// func (api *APIImpl) GetBlockReceipts(ctx context.Context, number rpc.BlockNumber) ([]map[string]interface{}, error) {
func (api *APIImpl) GetBlockReceipts(ctx context.Context, number rpc.BlockNumber) ([]map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, err := getBlockNumber(number, tx)
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
		result = append(result, marshalReceipt(receipt, txn, chainConfig, block, txn.Hash()))
	}

	return result, nil
}

func marshalReceipt(receipt *types.Receipt, txn types.Transaction, chainConfig *params.ChainConfig, block *types.Block, hash common.Hash) map[string]interface{} {
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
	signer := types.LatestSignerForChainID(chainId)
	from, _ := txn.Sender(*signer)

	fields := map[string]interface{}{
		"blockHash":         receipt.BlockHash,
		"blockNumber":       hexutil.Uint64(receipt.BlockNumber.Uint64()),
		"transactionHash":   hash,
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
func filterLogs(logs []*types.Log, addresses []common.Address, topics [][]common.Hash) []*types.Log {
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
