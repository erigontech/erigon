package commands

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/RoaringBitmap/roaring"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/bitmapdb"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/transactions"
)

func getReceipts(ctx context.Context, tx kv.Tx, chainConfig *params.ChainConfig, block *types.Block, senders []common.Address) (types.Receipts, error) {
	if cached := rawdb.ReadReceipts(tx, block, senders); cached != nil {
		return cached, nil
	}

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		return rawdb.ReadHeader(tx, hash, number)
	}
	contractHasTEVM := ethdb.GetHasTEVM(tx)
	_, _, _, ibs, _, err := transactions.ComputeTxEnv(ctx, block, chainConfig, getHeader, contractHasTEVM, ethash.NewFaker(), tx, block.Hash(), 0)
	if err != nil {
		return nil, err
	}

	var receipts types.Receipts
	gp := new(core.GasPool).AddGas(block.GasLimit())
	var usedGas = new(uint64)
	for i, txn := range block.Transactions() {
		ibs.Prepare(txn.Hash(), block.Hash(), i)
		receipt, _, err := core.ApplyTransaction(chainConfig, getHeader, ethash.NewFaker(), nil, gp, ibs, state.NewNoopWriter(), block.Header(), txn, usedGas, vm.Config{}, contractHasTEVM)
		if err != nil {
			return nil, err
		}
		receipt.BlockHash = block.Hash()
		receipts = append(receipts, receipt)
	}

	return receipts, nil
}

// GetLogs implements eth_getLogs. Returns an array of logs matching a given filter object.
func (api *APIImpl) GetLogs(ctx context.Context, crit filters.FilterCriteria) ([]*types.Log, error) {
	var begin, end uint64
	var logs []*types.Log //nolint:prealloc

	tx, beginErr := api.db.BeginRo(ctx)
	if beginErr != nil {
		return returnLogs(logs), beginErr
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

		begin = 0
		if crit.FromBlock != nil && crit.FromBlock.Sign() > 0 {
			begin = crit.FromBlock.Uint64()
		}
		end = latest
		if crit.ToBlock != nil && crit.ToBlock.Sign() > 0 {
			end = crit.ToBlock.Uint64()
		}
	}

	blockNumbers := roaring.New()
	blockNumbers.AddRange(begin, end+1) // [min,max)

	topicsBitmap, err := getTopicsBitmap(tx, crit.Topics, uint32(begin), uint32(end))
	if err != nil {
		return nil, err
	}
	if topicsBitmap != nil {
		if blockNumbers == nil {
			blockNumbers = topicsBitmap
		} else {
			blockNumbers.And(topicsBitmap)
		}
	}

	var addrBitmap *roaring.Bitmap
	for _, addr := range crit.Addresses {
		m, err := bitmapdb.Get(tx, kv.LogAddressIndex, addr[:], uint32(begin), uint32(end))
		if err != nil {
			return nil, err
		}
		if addrBitmap == nil {
			addrBitmap = m
		} else {
			addrBitmap = roaring.Or(addrBitmap, m)
		}
	}

	if addrBitmap != nil {
		if blockNumbers == nil {
			blockNumbers = addrBitmap
		} else {
			blockNumbers.And(addrBitmap)
		}
	}

	if blockNumbers.GetCardinality() == 0 {
		return returnLogs(logs), nil
	}

	iter := blockNumbers.Iterator()
	for iter.HasNext() {
		if err = common.Stopped(ctx.Done()); err != nil {
			return nil, err
		}

		blockNToMatch := uint64(iter.Next())
		var logIndex uint
		var blockLogs types.Logs
		if err := tx.ForPrefix(kv.Log, dbutils.EncodeBlockNumber(blockNToMatch), func(k, v []byte) error {
			var logs types.Logs
			if err := cbor.Unmarshal(&logs, bytes.NewReader(v)); err != nil {
				return fmt.Errorf("receipt unmarshal failed:  %w", err)
			}
			for _, log := range logs {
				log.Index = logIndex
				logIndex++
			}
			filtered := filterLogs(logs, crit.Addresses, crit.Topics)
			if len(filtered) > 0 {
				txIndex := uint(binary.BigEndian.Uint32(k[8:]))
				for _, log := range filtered {
					log.TxIndex = txIndex
				}
				blockLogs = append(blockLogs, filtered...)
			}
			return nil
		}); err != nil {
			return returnLogs(logs), err
		}

		if len(blockLogs) > 0 {
			b, err := rawdb.ReadBlockByNumber(tx, blockNToMatch)
			if err != nil {
				return nil, err
			}
			if b == nil {
				return nil, fmt.Errorf("block not found %d", blockNToMatch)
			}
			blockHash := b.Hash()
			for _, log := range blockLogs {
				log.BlockNumber = blockNToMatch
				log.BlockHash = blockHash
				log.TxHash = b.Transactions()[log.TxIndex].Hash()
			}
			logs = append(logs, blockLogs...)
		}
	}
	return returnLogs(logs), nil
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
			} else {
				bitmapForORing.Or(m)
			}
		}

		if bitmapForORing != nil {
			if result == nil {
				result = bitmapForORing
			} else {
				result = roaring.And(bitmapForORing, result)
			}
		}
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

	blockNumber, err := rawdb.ReadTxLookupEntry(tx, hash)
	if err != nil {
		return nil, err
	}
	if blockNumber == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
	}

	// Extract transactions from block
	block, senders, bErr := rawdb.ReadBlockByNumberWithSenders(tx, *blockNumber)
	if bErr != nil {
		return nil, bErr
	}
	if block == nil {
		return nil, fmt.Errorf("could not find block  %d", *blockNumber)
	}
	var txIndex uint64
	for idx, txn := range block.Transactions() {
		if txn.Hash() == hash {
			txIndex = uint64(idx)
			break
		}
	}

	cc, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	receipts, err := getReceipts(ctx, tx, cc, block, senders)
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %v", err)
	}
	if len(receipts) <= int(txIndex) {
		return nil, fmt.Errorf("block has less receipts than expected: %d <= %d, block: %d", len(receipts), int(txIndex), blockNumber)
	}
	return marshalReceipt(receipts[txIndex], block.Transactions()[txIndex], cc, block), nil
}

// GetBlockReceipts - receipts for individual block
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
	block, senders, err := rawdb.ReadBlockByNumberWithSenders(tx, blockNum)
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
	receipts, err := getReceipts(ctx, tx, chainConfig, block, senders)
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %v", err)
	}
	result := make([]map[string]interface{}, 0, len(receipts))
	for _, receipt := range receipts {
		txn := block.Transactions()[receipt.TransactionIndex]
		result = append(result, marshalReceipt(receipt, txn, chainConfig, block))
	}

	return result, nil
}

func marshalReceipt(receipt *types.Receipt, txn types.Transaction, chainConfig *params.ChainConfig, block *types.Block) map[string]interface{} {
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
		"transactionHash":   txn.Hash(),
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
	var ret []*types.Log
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
		ret = append(ret, log)
	}
	return ret
}

func returnLogs(logs []*types.Log) []*types.Log {
	if logs == nil {
		return []*types.Log{}
	}
	return logs
}
