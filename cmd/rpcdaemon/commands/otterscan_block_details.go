package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
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

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	getBlockRes, err := api.delegateGetBlockByNumber(tx, b, number, false)
	if err != nil {
		return nil, err
	}
	getIssuanceRes, err := api.delegateIssuance(tx, b, chainConfig)
	if err != nil {
		return nil, err
	}
	feesRes, err := api.delegateBlockFees(ctx, tx, b, senders, chainConfig)
	if err != nil {
		return nil, err
	}

	response := map[string]interface{}{}
	response["block"] = getBlockRes
	response["issuance"] = getIssuanceRes
	response["totalFees"] = hexutil.Uint64(feesRes)
	return response, nil
}

// TODO: remove duplication with GetBlockDetails
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

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	getBlockRes, err := api.delegateGetBlockByNumber(tx, b, rpc.BlockNumber(b.Number().Int64()), false)
	if err != nil {
		return nil, err
	}
	getIssuanceRes, err := api.delegateIssuance(tx, b, chainConfig)
	if err != nil {
		return nil, err
	}
	feesRes, err := api.delegateBlockFees(ctx, tx, b, senders, chainConfig)
	if err != nil {
		return nil, err
	}

	response := map[string]interface{}{}
	response["block"] = getBlockRes
	response["issuance"] = getIssuanceRes
	response["totalFees"] = hexutil.Uint64(feesRes)
	return response, nil
}
