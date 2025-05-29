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

package contracts

import (
	"context"
	"fmt"
	"math/big"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/cmd/devnet/devnet"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

func NewBackend(node devnet.Node) bind.ContractBackend {
	return contractBackend{node}
}

type contractBackend struct {
	node devnet.Node
}

func (cb contractBackend) CodeAt(ctx context.Context, contract common.Address, blockNumber *big.Int) ([]byte, error) {
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
		Data:     (*hexutil.Bytes)(&call.Data),
	}, blockRef, nil)
}

func (cb contractBackend) PendingCodeAt(ctx context.Context, account common.Address) ([]byte, error) {
	return cb.node.GetCode(account, rpc.PendingBlock)
}

func (cb contractBackend) PendingNonceAt(ctx context.Context, account common.Address) (uint64, error) {
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

func (cb contractBackend) SendTransaction(ctx context.Context, txn types.Transaction) error {
	_, err := cb.node.SendTransaction(txn)
	return err
}

func (cb contractBackend) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	return cb.node.FilterLogs(ctx, query)
}

func (cb contractBackend) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	return cb.node.SubscribeFilterLogs(ctx, query, ch)
}
