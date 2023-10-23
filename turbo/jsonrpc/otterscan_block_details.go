package jsonrpc

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
)

func (api *OtterscanAPIImpl) GetBlockDetails(ctx context.Context, number rpc.BlockNumber) (map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
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
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// b, senders, err := rawdb.ReadBlockByHashWithSenders(tx, hash)
	blockNumber := rawdb.ReadHeaderNumber(tx, hash)
	if blockNumber == nil {
		return nil, fmt.Errorf("couldn't find block number for hash %v", hash.Bytes())
	}
	b, senders, err := api._blockReader.BlockWithSenders(ctx, tx, hash, *blockNumber)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	number := rpc.BlockNumber(b.Number().Int64())

	return api.getBlockDetailsImpl(ctx, tx, b, number, senders)
}

func (api *OtterscanAPIImpl) getBlockDetailsImpl(ctx context.Context, tx kv.Tx, b *types.Block, number rpc.BlockNumber, senders []common.Address) (map[string]interface{}, error) {
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	getBlockRes, err := delegateGetBlockByNumber(tx, b, number, false)
	if err != nil {
		return nil, err
	}
	getIssuanceRes, err := delegateIssuance(tx, b, chainConfig)
	if err != nil {
		return nil, err
	}
	receipts, err := api.getReceipts(ctx, tx, chainConfig, b, senders)
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
	response["totalFees"] = hexutil.Uint64(feesRes)
	return response, nil
}
