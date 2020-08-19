package commands

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

func GetReceipts(ctx context.Context, db rawdb.DatabaseReader, cfg *params.ChainConfig, hash common.Hash) (types.Receipts, error) {
	number := rawdb.ReadHeaderNumber(db, hash)
	if number == nil {
		return nil, fmt.Errorf("block not found: %x", hash)
	}

	block := rawdb.ReadBlock(db, hash, *number)
	if cached := rawdb.ReadReceipts(db, block.Hash(), block.NumberU64(), cfg); cached != nil {
		return cached, nil
	}

	bc := &blockGetter{db}
	_, _, ibs, dbstate, err := transactions.ComputeTxEnv(ctx, bc, params.MainnetChainConfig, &chainContext{db: db}, db.(ethdb.HasKV).KV(), hash, 0)
	if err != nil {
		return nil, err
	}

	var receipts types.Receipts
	cc := &chainContext{db}
	gp := new(core.GasPool).AddGas(block.GasLimit())
	var usedGas = new(uint64)
	for i, tx := range block.Transactions() {
		ibs.Prepare(tx.Hash(), block.Hash(), i)

		header := rawdb.ReadHeader(db, hash, *number)
		receipt, err := core.ApplyTransaction(params.MainnetChainConfig, cc, nil, gp, ibs, dbstate, header, tx, usedGas, vm.Config{})
		if err != nil {
			return nil, err
		}
		receipts = append(receipts, receipt)
	}

	return receipts, nil
}

func (api *APIImpl) GetLogs(ctx context.Context, hash common.Hash) ([][]*types.Log, error) {
	number := rawdb.ReadHeaderNumber(api.dbReader, hash)
	if number == nil {
		return nil, fmt.Errorf("block not found: %x", hash)
	}

	receipts, err := GetReceipts(ctx, api.dbReader, params.MainnetChainConfig, hash)
	if err != nil {
		return nil, fmt.Errorf("getReceipt error: %v", err)
	}
	logs := make([][]*types.Log, len(receipts))
	for i, receipt := range receipts {
		logs[i] = receipt.Logs
	}
	return logs, nil
}

func (api *APIImpl) GetTransactionReceipt(ctx context.Context, hash common.Hash) (map[string]interface{}, error) {
	// Retrieve the transaction and assemble its EVM context
	tx, blockHash, blockNumber, txIndex := rawdb.ReadTransaction(api.dbReader, hash)
	if tx == nil {
		return nil, fmt.Errorf("transaction %#x not found", hash)
	}

	receipts, err := GetReceipts(ctx, api.dbReader, params.MainnetChainConfig, blockHash)
	if err != nil {
		return nil, fmt.Errorf("getReceipt error: %v", err)
	}
	if len(receipts) <= int(txIndex) {
		return nil, fmt.Errorf("block has less receipts than expected: %d <= %d, block: %d", len(receipts), int(txIndex), blockNumber)
	}
	receipt := receipts[txIndex]

	var signer types.Signer = types.FrontierSigner{}
	if tx.Protected() {
		signer = types.NewEIP155Signer(tx.ChainID().ToBig())
	}
	from, _ := types.Sender(signer, tx)

	// Fill in the derived information in the logs
	if receipt.Logs != nil {
		for i, log := range receipt.Logs {
			log.BlockNumber = blockNumber
			log.TxHash = hash
			log.TxIndex = uint(txIndex)
			log.BlockHash = blockHash
			log.Index = uint(i)
		}
	}

	// Now reconstruct the bloom filter
	fields := map[string]interface{}{
		"blockHash":         blockHash,
		"blockNumber":       hexutil.Uint64(blockNumber),
		"transactionHash":   hash,
		"transactionIndex":  hexutil.Uint64(txIndex),
		"from":              from,
		"to":                tx.To(),
		"gasUsed":           hexutil.Uint64(receipt.GasUsed),
		"cumulativeGasUsed": hexutil.Uint64(receipt.CumulativeGasUsed),
		"contractAddress":   nil,
		"logs":              receipt.Logs,
		"logsBloom":         receipt.Bloom,
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
