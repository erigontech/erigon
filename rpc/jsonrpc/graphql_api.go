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
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethutils"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

type GraphQLAPI interface {
	GetBlockDetails(ctx context.Context, number rpc.BlockNumber) (map[string]interface{}, error)
	GetChainID(ctx context.Context) (*big.Int, error)
}

type GraphQLAPIImpl struct {
	*BaseAPI
	db kv.TemporalRoDB
}

func NewGraphQLAPI(base *BaseAPI, db kv.TemporalRoDB) *GraphQLAPIImpl {
	return &GraphQLAPIImpl{
		BaseAPI: base,
		db:      db,
	}
}

func (api *GraphQLAPIImpl) GetChainID(ctx context.Context) (*big.Int, error) {
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

func (api *GraphQLAPIImpl) GetBlockDetails(ctx context.Context, blockNumber rpc.BlockNumber) (map[string]interface{}, error) {
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

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	getBlockRes, err := api.delegateGetBlockByNumber(tx, block, blockNumber, false, chainConfig)
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

		transaction := ethutils.MarshalReceipt(receipt, txn, chainConfig, block.HeaderNoCopy(), txn.Hash(), true)
		transaction["nonce"] = txn.GetNonce()
		transaction["value"] = txn.GetValue()
		transaction["data"] = txn.GetData()
		transaction["logs"] = receipt.Logs
		result = append(result, transaction)
	}

	response := map[string]interface{}{}
	response["block"] = getBlockRes
	response["receipts"] = result

	// Withdrawals
	wresult := make([]map[string]interface{}, 0, len(block.Withdrawals()))
	for _, withdrawal := range block.Withdrawals() {
		wmap := make(map[string]interface{})
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

func (api *GraphQLAPIImpl) delegateGetBlockByNumber(tx kv.Tx, b *types.Block, number rpc.BlockNumber, inclTx bool, chainConfig *chain.Config) (map[string]interface{}, error) {
	additionalFields := make(map[string]interface{})
	response, err := ethapi.RPCMarshalBlock(b, inclTx, inclTx, additionalFields, chainConfig)
	if !inclTx {
		delete(response, "transactions") // workaround for https://github.com/erigontech/erigon/issues/4989#issuecomment-1218415666
	}
	response["transactionCount"] = b.Transactions().Len()

	if err == nil && number == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	return response, err
}
