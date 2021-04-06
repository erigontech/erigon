package commands

import (
	"context"
	"fmt"
	"math/big"

	"github.com/RoaringBitmap/roaring"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/filters"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"
)

func getReceipts(ctx context.Context, tx ethdb.Tx, chainConfig *params.ChainConfig, number uint64, hash common.Hash) (types.Receipts, error) {
	if cached := rawdb.ReadReceipts(ethdb.NewRoTxDb(tx), hash, number); cached != nil {
		return cached, nil
	}

	block := rawdb.ReadBlock(ethdb.NewRoTxDb(tx), hash, number)

	cc := adapter.NewChainContext(tx)
	bc := adapter.NewBlockGetter(tx)
	getHeader := func(hash common.Hash, number uint64) *types.Header {
		return rawdb.ReadHeader(ethdb.NewRoTxDb(tx), hash, number)
	}
	_, _, _, ibs, dbstate, err := transactions.ComputeTxEnv(ctx, bc, chainConfig, getHeader, cc.Engine(), tx, hash, 0)
	if err != nil {
		return nil, err
	}

	var receipts types.Receipts
	gp := new(core.GasPool).AddGas(block.GasLimit())
	var usedGas = new(uint64)
	for i, txn := range block.Transactions() {
		ibs.Prepare(txn.Hash(), block.Hash(), i)

		header := rawdb.ReadHeader(ethdb.NewRoTxDb(tx), hash, number)
		receipt, err := core.ApplyTransaction(chainConfig, getHeader, cc.Engine(), nil, gp, ibs, dbstate, header, txn, usedGas, vm.Config{})
		if err != nil {
			return nil, err
		}
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
		number := rawdb.ReadHeaderNumber(ethdb.NewRoTxDb(tx), *crit.BlockHash)
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
		m, err := bitmapdb.Get(ethdb.NewRoTxDb(tx), dbutils.LogAddressIndex, addr[:], uint32(begin), uint32(end))
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

	cc, err := api.chainConfig(tx)
	if err != nil {
		return returnLogs(logs), err
	}
	for _, blockNToMatch := range blockNumbers.ToArray() {
		blockHash, err := rawdb.ReadCanonicalHash(ethdb.NewRoTxDb(tx), uint64(blockNToMatch))
		if err != nil {
			return returnLogs(logs), err
		}
		if blockHash == (common.Hash{}) {
			return returnLogs(logs), fmt.Errorf("block not found %d", uint64(blockNToMatch))
		}
		receipts, err := getReceipts(ctx, tx, cc, uint64(blockNToMatch), blockHash)
		if err != nil {
			return returnLogs(logs), err
		}
		unfiltered := make([]*types.Log, 0, len(receipts))
		for _, receipt := range receipts {
			unfiltered = append(unfiltered, receipt.Logs...)
		}
		unfiltered = filterLogs(unfiltered, nil, nil, crit.Addresses, crit.Topics)
		logs = append(logs, unfiltered...)
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
func getTopicsBitmap(c ethdb.Tx, topics [][]common.Hash, from, to uint32) (*roaring.Bitmap, error) {
	var result *roaring.Bitmap
	for _, sub := range topics {
		var bitmapForORing *roaring.Bitmap
		for _, topic := range sub {
			m, err := bitmapdb.Get(ethdb.NewRoTxDb(c), dbutils.LogTopicIndex, topic[:], from, to)
			if err != nil {
				return nil, err
			}
			if bitmapForORing == nil {
				bitmapForORing = m
			} else {
				bitmapForORing = roaring.FastOr(bitmapForORing, m)
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

	// Retrieve the transaction and assemble its EVM context
	txn, blockHash, blockNumber, txIndex := rawdb.ReadTransaction(ethdb.NewRoTxDb(tx), hash)
	if txn == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/turbo-geth/issues/1645
	}

	cc, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	receipts, err := getReceipts(ctx, tx, cc, blockNumber, blockHash)
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %v", err)
	}
	if len(receipts) <= int(txIndex) {
		return nil, fmt.Errorf("block has less receipts than expected: %d <= %d, block: %d", len(receipts), int(txIndex), blockNumber)
	}
	receipt := receipts[txIndex]

	var signer types.Signer = types.FrontierSigner{}
	if txn.Protected() {
		signer = types.NewEIP155Signer(txn.ChainId().ToBig())
	}
	from, _ := types.Sender(signer, txn)

	// Fill in the derived information in the logs
	if receipt.Logs != nil {
		for _, log := range receipt.Logs {
			log.BlockNumber = blockNumber
			log.TxHash = hash
			log.TxIndex = uint(txIndex)
			log.BlockHash = blockHash
		}
	}

	// Now reconstruct the bloom filter
	fields := map[string]interface{}{
		"blockHash":         blockHash,
		"blockNumber":       hexutil.Uint64(blockNumber),
		"transactionHash":   hash,
		"transactionIndex":  hexutil.Uint64(txIndex),
		"from":              from,
		"to":                txn.To(),
		"gasUsed":           hexutil.Uint64(receipt.GasUsed),
		"cumulativeGasUsed": hexutil.Uint64(receipt.CumulativeGasUsed),
		"contractAddress":   nil,
		"logs":              receipt.Logs,
		"logsBloom":         types.CreateBloom(types.Receipts{receipt}),
	}

	// Assign receipt status or post state.
	if len(receipt.PostState) > 0 {
		fields["root"] = hexutil.Bytes(receipt.PostState)
	} else {
		fields["status"] = hexutil.Uint(receipt.Status)
	}
	if receipt.Logs == nil {
		fields["logs"] = [][]*types.Log{}
	}
	// If the ContractAddress is 20 0x0 bytes, assume it is not a contract creation
	if receipt.ContractAddress != (common.Address{}) {
		fields["contractAddress"] = receipt.ContractAddress
	}
	return fields, nil
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
func filterLogs(logs []*types.Log, fromBlock, toBlock *big.Int, addresses []common.Address, topics [][]common.Hash) []*types.Log {
	var ret []*types.Log
Logs:
	for _, log := range logs {
		if fromBlock != nil && fromBlock.Int64() >= 0 && fromBlock.Uint64() > log.BlockNumber {
			continue
		}
		if toBlock != nil && toBlock.Int64() >= 0 && toBlock.Uint64() < log.BlockNumber {
			continue
		}

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
