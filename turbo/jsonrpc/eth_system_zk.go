package jsonrpc

import (
	"context"
	"math/big"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/ethclient"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
)

type RpcL1GasPriceTracker interface {
	GetLatestPrice() (*big.Int, error)
	GetLowestPrice() *big.Int
}

func (api *APIImpl) GasPrice(ctx context.Context) (*hexutil.Big, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	cc, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	chainId := cc.ChainID
	if !api.isZkNonSequencer(chainId) {
		latestBlockNumber, err := rpchelper.GetLatestFinishedBlockNumber(tx)
		if err != nil {
			return nil, err
		}

		block, err := api.blockByNumber(ctx, rpc.BlockNumber(latestBlockNumber), tx)
		if err != nil {
			return nil, err
		}

		price, err := api.gasTracker.GetLatestPrice()
		if err != nil {
			return nil, err
		}

		if block.BaseFee() != nil {
			price.Add(price, block.BaseFee())
		}

		return (*hexutil.Big)(price), nil
	}

	if api.BaseAPI.gasless {
		var price hexutil.Big
		return &price, nil
	}

	client, err := ethclient.DialContext(ctx, api.l2RpcUrl)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	price, err := client.SuggestGasPrice(ctx)
	if err != nil {
		return nil, err
	}

	return (*hexutil.Big)(price), nil
}
