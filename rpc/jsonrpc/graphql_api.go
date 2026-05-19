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
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/types/ethutils"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc"
	ethapi "github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/rpc/transactions"
)

type GraphQLCallResult struct {
	Data    hexutil.Bytes
	GasUsed uint64
	Status  uint64
}

type GraphQLAPI interface {
	GetBlockDetails(ctx context.Context, number rpc.BlockNumber) (map[string]any, error)
	GetBlockDetailsByHash(ctx context.Context, hash common.Hash) (map[string]any, error)
	GetLatestBlockNumber(ctx context.Context) (uint64, error)
	GetChainID(ctx context.Context) (*uint256.Int, error)
	GetAccountInfo(ctx context.Context, address common.Address, blockNumber rpc.BlockNumber) (balance string, nonce uint64, code string, err error)
	GetAccountStorage(ctx context.Context, address common.Address, slot string, blockNumber rpc.BlockNumber) (string, error)
	GetBlockNumberForTx(ctx context.Context, hash common.Hash) (blockNum uint64, ok bool, err error)
	SendRawTransaction(ctx context.Context, data hexutil.Bytes) (common.Hash, error)
	Call(ctx context.Context, blockNumber rpc.BlockNumber, args ethapi.CallArgs) (*GraphQLCallResult, error)
	GetLogs(ctx context.Context, crit filters.FilterCriteria) (types.RPCLogs, error)
}

type GraphQLAPIImpl struct {
	*BaseAPI
	db              kv.TemporalRoDB
	eth             EthAPI
	gasCap          uint64
	returnDataLimit int
}

func NewGraphQLAPI(base *BaseAPI, db kv.TemporalRoDB, eth EthAPI, gasCap uint64, returnDataLimit int) *GraphQLAPIImpl {
	return &GraphQLAPIImpl{
		BaseAPI:         base,
		db:              db,
		eth:             eth,
		gasCap:          gasCap,
		returnDataLimit: returnDataLimit,
	}
}

func (api *GraphQLAPIImpl) GetLatestBlockNumber(ctx context.Context) (uint64, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	return rpchelper.GetLatestBlockNumber(api.filters.WithOverlay(tx))
}

func (api *GraphQLAPIImpl) GetBlockNumberForTx(ctx context.Context, hash common.Hash) (uint64, bool, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()

	blockNum, _, ok, err := api.txnLookup(ctx, tx, hash)
	return blockNum, ok, err
}

func (api *GraphQLAPIImpl) GetChainID(ctx context.Context) (*uint256.Int, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	response, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	return response.ChainID, nil
}

func (api *GraphQLAPIImpl) GetBlockDetails(ctx context.Context, blockNumber rpc.BlockNumber) (map[string]any, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	block, _, err := api.getBlockWithSenders(ctx, blockNumber, tx)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}

	getBlockRes, err := api.delegateGetBlockByNumber(tx, block, blockNumber, false)
	if err != nil {
		return nil, err
	}

	return api.buildBlockDetailsResponse(ctx, tx, block, getBlockRes)
}

func (api *GraphQLAPIImpl) GetBlockDetailsByHash(ctx context.Context, hash common.Hash) (map[string]any, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNrOrHash := rpc.BlockNumberOrHashWithHash(hash, false)
	blockHeight, blockHash, _, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}

	block, err := api.blockWithSenders(ctx, tx, blockHash, blockHeight)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}

	getBlockRes, err := api.delegateGetBlockByNumber(tx, block, rpc.BlockNumber(blockHeight), false)
	if err != nil {
		return nil, err
	}

	return api.buildBlockDetailsResponse(ctx, tx, block, getBlockRes)
}

func (api *GraphQLAPIImpl) buildBlockDetailsResponse(ctx context.Context, tx kv.TemporalTx, block *types.Block, getBlockRes map[string]any) (map[string]any, error) {
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	receipts, err := api.getReceipts(ctx, tx, block)
	if err != nil {
		return nil, err
	}

	result := make([]map[string]any, 0, len(receipts))
	for _, receipt := range receipts {
		txn := block.Transactions()[receipt.TransactionIndex]

		transaction := ethutils.MarshalReceipt(receipt, txn, chainConfig, block.HeaderNoCopy(), txn.Hash(), true, false)
		transaction["nonce"] = txn.GetNonce()
		transaction["value"] = txn.GetValue()
		transaction["data"] = txn.GetData()
		transaction["logs"] = receipt.Logs
		transaction["gas"] = txn.GetGasLimit()
		txType := txn.Type()
		if txType == types.DynamicFeeTxType || txType == types.SetCodeTxType || txType == types.BlobTxType {
			transaction["maxFeePerGas"] = txn.GetFeeCap()
			transaction["maxPriorityFeePerGas"] = txn.GetTipCap()
		}
		if txType == types.BlobTxType {
			if blobTx, ok := txn.(*types.BlobTx); ok {
				transaction["maxFeePerBlobGas"] = (*hexutil.Big)(blobTx.MaxFeePerBlobGas.ToBig())
			}
		}
		transaction["accessList"] = txn.GetAccessList()
		result = append(result, transaction)
	}

	td, err := rawdb.ReadTd(tx, block.Hash(), block.NumberU64())
	if err != nil {
		return nil, err
	}
	if td != nil {
		getBlockRes["totalDifficulty"] = (*hexutil.Big)(td.ToBig())
	} else {
		getBlockRes["totalDifficulty"] = (*hexutil.Big)(new(big.Int))
	}

	response := map[string]any{}
	response["block"] = getBlockRes
	response["receipts"] = result

	wresult := make([]map[string]any, 0, len(block.Withdrawals()))
	for _, withdrawal := range block.Withdrawals() {
		wmap := make(map[string]any)
		wmap["index"] = hexutil.Uint64(withdrawal.Index)
		wmap["validator"] = hexutil.Uint64(withdrawal.Validator)
		wmap["address"] = withdrawal.Address
		wmap["amount"] = withdrawal.Amount

		wresult = append(wresult, wmap)
	}

	response["withdrawals"] = wresult

	return response, nil
}

func (api *GraphQLAPIImpl) getBlockWithSenders(ctx context.Context, number rpc.BlockNumber, tx kv.Tx) (*types.Block, []common.Address, error) {
	if number == rpc.PendingBlockNumber {
		return api.pendingBlock(), nil, nil
	}

	blockHeight, blockHash, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(number), tx, api._blockReader, api.filters)
	if err != nil {
		return nil, nil, err
	}

	block, err := api.blockWithSenders(ctx, tx, blockHash, blockHeight)
	if err != nil {
		return nil, nil, err
	}
	if block == nil {
		return nil, nil, nil
	}
	return block, block.Body().SendersFromTxs(), nil
}

// zeroStorageHash is the zero-value storage slot result: 32 zero bytes hex-encoded.
const zeroStorageHash = "0x0000000000000000000000000000000000000000000000000000000000000000"

// GetAccountInfo returns the balance (hex), nonce, and bytecode for an account at the given block.
func (api *GraphQLAPIImpl) GetAccountInfo(ctx context.Context, address common.Address, blockNumber rpc.BlockNumber) (balance string, nonce uint64, code string, err error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return "", 0, "", err
	}
	defer tx.Rollback()

	blockNrOrHash := rpc.BlockNumberOrHashWithNumber(blockNumber)
	blockNum, _, latest, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return "", 0, "", err
	}

	if err = api.checkPruneHistory(ctx, tx, blockNum); err != nil {
		return "", 0, "", err
	}

	stateTx := api.filters.WithTemporalOverlay(tx)
	if err = rpchelper.CheckBlockExecuted(stateTx, blockNum); err != nil {
		return "", 0, "", err
	}

	reader, err := rpchelper.CreateStateReaderFromBlockNumber(ctx, stateTx, blockNum, latest, 0, api.stateCache, api._txNumReader)
	if err != nil {
		return "", 0, "", err
	}

	addr := accounts.InternAddress(address)
	acc, err := reader.ReadAccountData(addr)
	if err != nil {
		return "", 0, "", err
	}
	if acc == nil {
		return "0x0", 0, "0x", nil
	}

	balStr := acc.Balance.Hex()
	codeStr := "0x"
	if !acc.IsEmptyCodeHash() {
		if codeBytes, codeErr := reader.ReadAccountCode(addr); codeErr == nil && codeBytes != nil {
			codeStr = hexutil.Encode(codeBytes)
		}
	}

	return balStr, acc.Nonce, codeStr, nil
}

// GetAccountStorage returns the value of the given storage slot for an account at the given block.
func (api *GraphQLAPIImpl) GetAccountStorage(ctx context.Context, address common.Address, slot string, blockNumber rpc.BlockNumber) (string, error) {
	slotBytes, decErr := hexutil.FromHexWithValidation(slot)
	if decErr != nil {
		return zeroStorageHash, &rpc.InvalidParamsError{Message: "unable to decode storage slot: " + hexutil.ErrHexStringInvalid.Error()}
	}
	if len(slotBytes) > 32 {
		return zeroStorageHash, &rpc.InvalidParamsError{Message: hexutil.ErrTooBigHexString.Error()}
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	blockNrOrHash := rpc.BlockNumberOrHashWithNumber(blockNumber)
	blockNum, _, latest, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return zeroStorageHash, err
	}

	if err = api.checkPruneHistory(ctx, tx, blockNum); err != nil {
		return zeroStorageHash, err
	}

	stateTx := api.filters.WithTemporalOverlay(tx)
	if err = rpchelper.CheckBlockExecuted(stateTx, blockNum); err != nil {
		return zeroStorageHash, err
	}

	reader, err := rpchelper.CreateStateReaderFromBlockNumber(ctx, stateTx, blockNum, latest, 0, api.stateCache, api._txNumReader)
	if err != nil {
		return zeroStorageHash, err
	}

	addr := accounts.InternAddress(address)
	acc, err := reader.ReadAccountData(addr)
	if acc == nil || err != nil {
		return zeroStorageHash, err
	}

	location := accounts.InternKey(common.BytesToHash(slotBytes))
	res, _, err := reader.ReadAccountStorage(addr, location)
	if err != nil {
		return zeroStorageHash, err
	}
	return hexutil.Encode(res.PaddedBytes(32)), nil
}

func (api *GraphQLAPIImpl) delegateGetBlockByNumber(tx kv.Tx, b *types.Block, number rpc.BlockNumber, inclTx bool) (map[string]any, error) {
	additionalFields := make(map[string]any)
	response, err := ethapi.RPCMarshalBlock(b, inclTx, inclTx, additionalFields)
	if !inclTx {
		delete(response, "transactions") // workaround for https://github.com/erigontech/erigon/issues/4989#issuecomment-1218415666
	}
	response["transactionCount"] = hexutil.Uint64(b.Transactions().Len())

	if err == nil && number == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	return response, err
}

func (api *GraphQLAPIImpl) SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error) {
	return api.eth.SendRawTransaction(ctx, encodedTx)
}

func (api *GraphQLAPIImpl) Call(ctx context.Context, blockNumber rpc.BlockNumber, args ethapi.CallArgs) (*GraphQLCallResult, error) {
	blockNrOrHash := rpc.BlockNumberOrHashWithNumber(blockNumber)

	roTx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer roTx.Rollback()

	var tx kv.TemporalTx = roTx
	if api.filters != nil {
		if sd := api.filters.LatestSD(); sd != nil {
			if overlayTx := sd.BlockOverlayTemporalTx(roTx); overlayTx != nil {
				tx = overlayTx
			}
		}
	}

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	if args.Gas == nil || uint64(*args.Gas) == 0 {
		args.Gas = (*hexutil.Uint64)(&api.gasCap)
	}

	header, _, err := api.headerByNumberOrHash(ctx, tx, blockNrOrHash)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, fmt.Errorf("header not found")
	}

	if err = api.checkPruneHistory(ctx, tx, header.Number.Uint64()); err != nil {
		return nil, err
	}

	if err = rpchelper.CheckBlockExecuted(api.filters.WithOverlay(tx), header.Number.Uint64()); err != nil {
		return nil, err
	}

	stateReader, err := rpchelper.CreateStateReader(ctx, tx, api._blockReader, blockNrOrHash, 0, api.filters, api.stateCache, api._txNumReader)
	if err != nil {
		return nil, err
	}

	result, err := transactions.DoCall(ctx, api.engine(), args, tx, blockNrOrHash, header, nil, nil, api.gasCap, chainConfig, stateReader, api._blockReader, api.evmCallTimeout)
	if err != nil {
		return nil, err
	}

	if api.returnDataLimit > 0 && len(result.ReturnData) > api.returnDataLimit {
		return nil, fmt.Errorf("call returned result on length %d exceeding --rpc.returndata.limit %d", len(result.ReturnData), api.returnDataLimit)
	}

	if errors.Is(result.Err, vm.ErrExecutionReverted) {
		return &GraphQLCallResult{Data: result.Revert(), GasUsed: result.ReceiptGasUsed, Status: 0}, nil
	}

	return &GraphQLCallResult{Data: result.Return(), GasUsed: result.ReceiptGasUsed, Status: 1}, nil
}

func (api *GraphQLAPIImpl) GetLogs(ctx context.Context, crit filters.FilterCriteria) (types.RPCLogs, error) {
	return api.eth.GetLogs(ctx, crit)
}
