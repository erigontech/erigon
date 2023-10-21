package contracts

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"math/big"

	ethereum "github.com/ledgerwatch/erigon"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
)

func NewBackend(node devnet.Node) bind.ContractBackend {
	return contractBackend{node}
}

type contractBackend struct {
	node devnet.Node
}

func (cb contractBackend) CodeAt(ctx context.Context, contract libcommon.Address, blockNumber *big.Int) ([]byte, error) {
	return cb.node.GetCode(contract, rpc.AsBlockReference(blockNumber))
}

func (cb contractBackend) CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	var gasPrice *hexutil.Big
	var value *hexutil.Big

	if call.Value != nil {
		value = (*hexutil.Big)(call.Value.ToBig())
	}

	if call.GasPrice != nil {
		gasPrice = (*hexutil.Big)(call.GasPrice.ToBig())
	}

	var blockRef rpc.BlockReference
	if blockNumber != nil {
		blockRef = rpc.AsBlockReference(blockNumber)
	} else {
		blockRef = rpc.LatestBlock
	}

	return cb.node.Call(ethapi.CallArgs{
		From:     &call.From,
		To:       call.To,
		Gas:      (*hexutil.Uint64)(&call.Gas),
		GasPrice: gasPrice,
		Value:    value,
		Data:     (*hexutility.Bytes)(&call.Data),
	}, blockRef, nil)
}

func (cb contractBackend) PendingCodeAt(ctx context.Context, account libcommon.Address) ([]byte, error) {
	return cb.node.GetCode(account, rpc.PendingBlock)
}

func (cb contractBackend) PendingNonceAt(ctx context.Context, account libcommon.Address) (uint64, error) {
	res, err := cb.node.GetTransactionCount(account, rpc.PendingBlock)

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
