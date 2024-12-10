package jsonrpc

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon/ethclient"
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
		price, err := api.gasTracker.GetLatestPrice()
		if err != nil {
			return nil, err
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
