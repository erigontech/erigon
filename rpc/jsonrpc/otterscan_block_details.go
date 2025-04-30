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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/rpc"
)

func (api *OtterscanAPIImpl) GetBlockDetails(ctx context.Context, number rpc.BlockNumber) (map[string]interface{}, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	b, senders, err := api.getBlockWithSenders(ctx, number, tx)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}

	return api.getBlockDetailsImpl(ctx, tx, b, number, senders)
}

func (api *OtterscanAPIImpl) GetBlockDetailsByHash(ctx context.Context, hash common.Hash) (map[string]interface{}, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// b, senders, err := rawdb.ReadBlockByHashWithSenders(tx, hash)
	blockNumber, err := api._blockReader.HeaderNumber(ctx, tx, hash)
	if err != nil {
		return nil, err
	}
	if blockNumber == nil {
		return nil, fmt.Errorf("couldn't find block number for hash %v", hash.Bytes())
	}
	b, err := api.blockWithSenders(ctx, tx, hash, *blockNumber)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	number := rpc.BlockNumber(b.Number().Int64())

	return api.getBlockDetailsImpl(ctx, tx, b, number, b.Body().SendersFromTxs())
}

func (api *OtterscanAPIImpl) getBlockDetailsImpl(ctx context.Context, tx kv.TemporalTx, b *types.Block, number rpc.BlockNumber, senders []common.Address) (map[string]interface{}, error) {
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	getBlockRes, err := delegateGetBlockByNumber(tx, b, number, false)
	if err != nil {
		return nil, err
	}
	getIssuanceRes, err := delegateIssuance(tx, b, chainConfig, api.engine())
	if err != nil {
		return nil, err
	}
	receipts, err := api.getReceipts(ctx, tx, b)
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %v", err)
	}
	feesRes, err := delegateBlockFees(ctx, tx, b, senders, chainConfig, receipts)
	if err != nil {
		return nil, err
	}

	response := map[string]interface{}{}
	response["block"] = getBlockRes
	response["issuance"] = getIssuanceRes
	response["totalFees"] = (*hexutil.Big)(feesRes)
	return response, nil
}
