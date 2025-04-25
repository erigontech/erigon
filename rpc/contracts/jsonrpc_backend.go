// Copyright 2025 The Erigon Authors
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

package contracts

import (
	"context"
	"math/big"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/requests"
)

func NewJsonRpcBackend(url string, logger log.Logger) JsonRpcBackend {
	return JsonRpcBackend{
		client: requests.NewRequestGenerator(url, logger),
	}
}

type JsonRpcBackend struct {
	client requests.RequestGenerator
}

func (b JsonRpcBackend) CodeAt(ctx context.Context, contract common.Address, blockNum *big.Int) ([]byte, error) {
	return b.client.GetCode(contract, rpc.BlockReference(BlockNumArg(blockNum)))
}

func (b JsonRpcBackend) CallContract(ctx context.Context, call ethereum.CallMsg, blockNum *big.Int) ([]byte, error) {
	return b.client.Call(CallArgsFromCallMsg(call), rpc.BlockReference(BlockNumArg(blockNum)), nil)
}

func (b JsonRpcBackend) PendingCodeAt(ctx context.Context, account common.Address) ([]byte, error) {
	return b.client.GetCode(account, rpc.PendingBlock)
}

func (b JsonRpcBackend) PendingNonceAt(ctx context.Context, account common.Address) (uint64, error) {
	res, err := b.client.GetTransactionCount(account, rpc.PendingBlock)
	if err != nil {
		return 0, err
	}
	return res.Uint64(), nil
}

func (b JsonRpcBackend) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	return b.client.GasPrice()
}

func (b JsonRpcBackend) EstimateGas(ctx context.Context, call ethereum.CallMsg) (uint64, error) {
	return b.client.EstimateGas(call, requests.BlockNumbers.Pending)
}

func (b JsonRpcBackend) SendTransaction(ctx context.Context, txn types.Transaction) error {
	_, err := b.client.SendTransaction(txn)
	return err
}

func (b JsonRpcBackend) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	return b.client.FilterLogs(ctx, query)
}

func (b JsonRpcBackend) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	return b.client.SubscribeFilterLogs(ctx, query, ch)
}
