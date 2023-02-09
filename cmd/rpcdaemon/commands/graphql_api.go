package commands

import (
	"context"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

type GraphQLAPI interface {
	GetBlockDetails(ctx context.Context, number rpc.BlockNumber) (map[string]interface{}, error)
	// GetBlockDetailsByHash(ctx context.Context, hash common.Hash) (map[string]interface{}, error)
	// GetBlockTransactions(ctx context.Context, number rpc.BlockNumber, pageNumber uint8, pageSize uint8) (map[string]interface{}, error)
}

type GraphQLAPIImpl struct {
	*BaseAPI
	db kv.RoDB
}

func NewGraphQLAPI(base *BaseAPI, db kv.RoDB) *GraphQLAPIImpl {
	return &GraphQLAPIImpl{
		BaseAPI: base,
		db:      db,
	}
}

func (api *GraphQLAPIImpl) GetBlockDetails(ctx context.Context, number rpc.BlockNumber) (map[string]interface{}, error) {
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

	receipts, err := api.getReceipts(ctx, tx, chainConfig, b, b.Body().SendersFromTxs())
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %w", err)
	}
	result := make([]map[string]interface{}, 0, len(receipts))
	for _, receipt := range receipts {
		txn := b.Transactions()[receipt.TransactionIndex]
		result = append(result, marshalReceipt(receipt, txn, chainConfig, b.HeaderNoCopy(), txn.Hash(), true))
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
	response["receipts"] = result
	return response, nil
}

func (api *GraphQLAPIImpl) getBlockWithSenders(ctx context.Context, number rpc.BlockNumber, tx kv.Tx) (*types.Block, []common.Address, error) {
	if number == rpc.PendingBlockNumber {
		return api.pendingBlock(), nil, nil
	}

	n, hash, _, err := rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithNumber(number), tx, api.filters)
	if err != nil {
		return nil, nil, err
	}

	block, senders, err := api._blockReader.BlockWithSenders(ctx, tx, hash, n)
	return block, senders, err
}

func (api *GraphQLAPIImpl) delegateGetBlockByNumber(tx kv.Tx, b *types.Block, number rpc.BlockNumber, inclTx bool) (map[string]interface{}, error) {
	td, err := rawdb.ReadTd(tx, b.Hash(), b.NumberU64())
	if err != nil {
		return nil, err
	}
	additionalFields := make(map[string]interface{})
	response, err := ethapi.RPCMarshalBlock(b, inclTx, inclTx, additionalFields)
	if !inclTx {
		delete(response, "transactions") // workaround for https://github.com/ledgerwatch/erigon/issues/4989#issuecomment-1218415666
	}
	response["totalDifficulty"] = (*hexutil.Big)(td)
	response["transactionCount"] = b.Transactions().Len()

	if err == nil && number == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	// Explicitly drop unwanted fields
	response["logsBloom"] = nil
	return response, err
}

func (api *GraphQLAPIImpl) delegateIssuance(tx kv.Tx, block *types.Block, chainConfig *chain.Config) (internalIssuance, error) {
	if chainConfig.Ethash == nil {
		// Clique for example has no issuance
		return internalIssuance{}, nil
	}

	minerReward, uncleRewards := ethash.AccumulateRewards(chainConfig, block.Header(), block.Uncles())
	issuance := minerReward
	for _, r := range uncleRewards {
		p := r // avoids warning?
		issuance.Add(&issuance, &p)
	}

	var ret internalIssuance
	ret.BlockReward = hexutil.EncodeBig(minerReward.ToBig())
	ret.Issuance = hexutil.EncodeBig(issuance.ToBig())
	issuance.Sub(&issuance, &minerReward)
	ret.UncleReward = hexutil.EncodeBig(issuance.ToBig())
	return ret, nil
}

func (api *GraphQLAPIImpl) delegateBlockFees(ctx context.Context, tx kv.Tx, block *types.Block, senders []common.Address, chainConfig *chain.Config) (uint64, error) {
	receipts, err := api.getReceipts(ctx, tx, chainConfig, block, senders)
	if err != nil {
		return 0, fmt.Errorf("getReceipts error: %v", err)
	}

	fees := uint64(0)
	for _, receipt := range receipts {
		txn := block.Transactions()[receipt.TransactionIndex]
		effectiveGasPrice := uint64(0)
		if !chainConfig.IsLondon(block.NumberU64()) {
			effectiveGasPrice = txn.GetPrice().Uint64()
		} else {
			baseFee, _ := uint256.FromBig(block.BaseFee())
			gasPrice := new(big.Int).Add(block.BaseFee(), txn.GetEffectiveGasTip(baseFee).ToBig())
			effectiveGasPrice = gasPrice.Uint64()
		}
		fees += effectiveGasPrice * receipt.GasUsed
	}

	return fees, nil
}
