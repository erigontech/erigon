package contracts

import (
	"context"
	"fmt"
	"math/big"

	ethereum "github.com/ledgerwatch/erigon"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/core/types"
)

func NewBackend(node devnet.Node) bind.ContractBackend {
	return contractBackend{node}
}

type contractBackend struct {
	node devnet.Node
}

func (cb contractBackend) CodeAt(ctx context.Context, contract libcommon.Address, blockNumber *big.Int) ([]byte, error) {
	return cb.node.GetCode(contract, requests.AsBlockNumber(blockNumber))
}

func (cb contractBackend) CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	return nil, fmt.Errorf("TODO")
}

func (cb contractBackend) PendingCodeAt(ctx context.Context, account libcommon.Address) ([]byte, error) {
	return cb.node.GetCode(account, requests.BlockNumbers.Pending)
}

func (cb contractBackend) PendingNonceAt(ctx context.Context, account libcommon.Address) (uint64, error) {
	res, err := cb.node.GetTransactionCount(account, requests.BlockNumbers.Pending)

	if err != nil {
		return 0, fmt.Errorf("failed to get transaction count for address 0x%x: %v", account, err)
	}

	return res.Uint64(), nil
}

func (cb contractBackend) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	return cb.node.GasPrice()
}

func (cb contractBackend) EstimateGas(ctx context.Context, call ethereum.CallMsg) (gas uint64, err error) {
	return 1_000_000, nil
	//return cb.node.EstimateGas(call, requests.BlockNumbers.Pending)
}

func (cb contractBackend) SendTransaction(ctx context.Context, tx types.Transaction) error {
	_, err := cb.node.SendTransaction(tx)
	return err
}

func (cb contractBackend) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	return cb.node.FilterLogs(ctx, query)
}

func (cb contractBackend) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	return cb.node.SubscribeFilterLogs(ctx, query, ch)
}
