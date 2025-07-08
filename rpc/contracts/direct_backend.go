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
	"bytes"
	"context"
	"fmt"
	"math/big"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/eth/filters"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/p2p/event"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

var _ bind.ContractBackend = DirectBackend{}

type DirectBackend struct {
	api jsonrpc.EthAPI
}

func NewDirectBackend(api jsonrpc.EthAPI) DirectBackend {
	return DirectBackend{
		api: api,
	}
}

func (b DirectBackend) CodeAt(ctx context.Context, account common.Address, blockNum *big.Int) ([]byte, error) {
	return b.api.GetCode(ctx, account, BlockNumArg(blockNum))
}

func (b DirectBackend) CallContract(ctx context.Context, callMsg ethereum.CallMsg, blockNum *big.Int) ([]byte, error) {
	return b.api.Call(ctx, CallArgsFromCallMsg(callMsg), BlockNumArg(blockNum), nil)
}

func (b DirectBackend) PendingCodeAt(ctx context.Context, account common.Address) ([]byte, error) {
	return b.api.GetCode(ctx, account, PendingBlockNumArg())
}

func (b DirectBackend) PendingNonceAt(ctx context.Context, account common.Address) (uint64, error) {
	count, err := b.api.GetTransactionCount(ctx, account, PendingBlockNumArg())
	if err != nil {
		return 0, err
	}

	return uint64(*count), nil
}

func (b DirectBackend) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	price, err := b.api.GasPrice(ctx)
	if err != nil {
		return nil, err
	}

	return price.ToInt(), nil
}

func (b DirectBackend) EstimateGas(ctx context.Context, call ethereum.CallMsg) (uint64, error) {
	callArgs := CallArgsFromCallMsg(call)
	gas, err := b.api.EstimateGas(ctx, &callArgs, nil, nil)
	if err != nil {
		return 0, err
	}

	return gas.Uint64(), nil
}

func (b DirectBackend) SendTransaction(ctx context.Context, txn types.Transaction) error {
	var buf bytes.Buffer
	err := txn.MarshalBinary(&buf)
	if err != nil {
		return err
	}

	_, err = b.api.SendRawTransaction(ctx, buf.Bytes())
	return err
}

func (b DirectBackend) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	logs, err := b.api.GetLogs(ctx, filters.FilterCriteria(query))
	if err != nil {
		return nil, err
	}

	res := make([]types.Log, len(logs))
	for i, log := range logs {
		res[i] = *log
	}

	return res, nil
}

func (b DirectBackend) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	resc, closec := make(chan any), make(chan any)
	ctx = rpc.ContextWithNotifier(ctx, rpc.NewLocalNotifier("eth", resc, closec))
	_, err := b.api.Logs(ctx, filters.FilterCriteria(query))
	if err != nil {
		return nil, err
	}

	sub := event.NewSubscription(func(quit <-chan struct{}) error {
		for {
			select {
			case <-quit:
				close(closec)
				return nil
			case res := <-resc:
				log, ok := res.(*types.Log)
				if !ok {
					return fmt.Errorf("unexpected type %T in SubscribeFilterLogs", res)
				}

				ch <- *log
			}
		}
	})

	return sub, nil
}

func BlockNumArg(blockNum *big.Int) rpc.BlockNumberOrHash {
	var blockRef rpc.BlockReference
	if blockNum == nil {
		blockRef = rpc.LatestBlock
	} else {
		blockRef = rpc.AsBlockReference(blockNum)
	}

	return rpc.BlockNumberOrHash(blockRef)
}

func PendingBlockNumArg() rpc.BlockNumberOrHash {
	return rpc.BlockNumberOrHash(rpc.PendingBlock)
}

func CallArgsFromCallMsg(callMsg ethereum.CallMsg) ethapi.CallArgs {
	var emptyAddress common.Address
	var from *common.Address
	if callMsg.From != emptyAddress {
		from = &callMsg.From
	}

	var gas *hexutil.Uint64
	if callMsg.Gas != 0 {
		gas = (*hexutil.Uint64)(&callMsg.Gas)
	}

	var gasPrice *hexutil.Big
	if callMsg.GasPrice != nil {
		gasPrice = (*hexutil.Big)(callMsg.GasPrice.ToBig())
	}

	var feeCap *hexutil.Big
	if callMsg.FeeCap != nil {
		feeCap = (*hexutil.Big)(callMsg.FeeCap.ToBig())
	}

	var maxFeePerBlobGas *hexutil.Big
	if callMsg.MaxFeePerBlobGas != nil {
		maxFeePerBlobGas = (*hexutil.Big)(callMsg.MaxFeePerBlobGas.ToBig())
	}

	var value *hexutil.Big
	if callMsg.Value != nil {
		value = (*hexutil.Big)(callMsg.Value.ToBig())
	}

	var data *hexutil.Bytes
	if callMsg.Data != nil {
		b := hexutil.Bytes(callMsg.Data)
		data = &b
	}

	var accessList *types.AccessList
	if callMsg.AccessList != nil {
		accessList = &callMsg.AccessList
	}

	return ethapi.CallArgs{
		From:             from,
		To:               callMsg.To,
		Gas:              gas,
		GasPrice:         gasPrice,
		MaxFeePerGas:     feeCap,
		MaxFeePerBlobGas: maxFeePerBlobGas,
		Value:            value,
		Data:             data,
		AccessList:       accessList,
	}
}
