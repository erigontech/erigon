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
	"errors"
	"fmt"
	"math/big"

	ethereum "github.com/erigontech/erigon"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/accounts/abi/bind"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rpc"
)

var _ bind.ContractBackend = JsonRpcBackend{}

type JsonRpcBackend struct {
	client *rpc.Client
}

func DialJsonRpcBackendContext(ctx context.Context, url string, logger log.Logger) (JsonRpcBackend, error) {
	client, err := rpc.DialContext(ctx, url, logger)
	if err != nil {
		return JsonRpcBackend{}, err
	}

	return NewJsonRpcBackend(client), nil
}

func NewJsonRpcBackend(client *rpc.Client) JsonRpcBackend {
	return JsonRpcBackend{client: client}
}

func (b JsonRpcBackend) CodeAt(ctx context.Context, account libcommon.Address, blockNum *big.Int) ([]byte, error) {
	var result hexutil.Bytes
	err := b.client.CallContext(ctx, &result, "eth_getCode", account, toBlockNumJsonRpcArg(blockNum))
	return result, err
}

func (b JsonRpcBackend) CallContract(ctx context.Context, callMsg ethereum.CallMsg, blockNum *big.Int) ([]byte, error) {
	var result hexutil.Bytes
	err := b.client.CallContext(ctx, &result, "eth_call", CallArgsFromCallMsg(callMsg), toBlockNumJsonRpcArg(blockNum))
	return result, err
}

func (b JsonRpcBackend) PendingCodeAt(ctx context.Context, account libcommon.Address) ([]byte, error) {
	var result hexutil.Bytes
	err := b.client.CallContext(ctx, &result, "eth_getCode", account, "pending")
	return result, err
}

func (b JsonRpcBackend) PendingNonceAt(ctx context.Context, account libcommon.Address) (uint64, error) {
	var result hexutil.Uint64
	err := b.client.CallContext(ctx, &result, "eth_getTransactionCount", account, "pending")
	return uint64(result), err
}

func (b JsonRpcBackend) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	var hex hexutil.Big
	if err := b.client.CallContext(ctx, &hex, "eth_gasPrice"); err != nil {
		return nil, err
	}
	return (*big.Int)(&hex), nil
}

func (b JsonRpcBackend) EstimateGas(ctx context.Context, callMsg ethereum.CallMsg) (uint64, error) {
	var hex hexutil.Uint64
	err := b.client.CallContext(ctx, &hex, "eth_estimateGas", CallArgsFromCallMsg(callMsg))
	if err != nil {
		return 0, err
	}
	return uint64(hex), nil

}

func (b JsonRpcBackend) SendTransaction(ctx context.Context, txn types.Transaction) error {
	var buf bytes.Buffer
	err := txn.MarshalBinary(&buf)
	if err != nil {
		return err
	}
	return b.client.CallContext(ctx, nil, "eth_sendRawTransaction", hexutil.Encode(buf.Bytes()))
}

func (b JsonRpcBackend) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	var result []types.Log
	arg, err := toFilterJsonRpcArg(query)
	if err != nil {
		return nil, err
	}
	err = b.client.CallContext(ctx, &result, "eth_getLogs", arg)
	return result, err
}

func (b JsonRpcBackend) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	arg, err := toFilterJsonRpcArg(query)
	if err != nil {
		return nil, err
	}

	return b.client.EthSubscribe(ctx, ch, "logs", arg)
}

func toBlockNumJsonRpcArg(number *big.Int) string {
	if number == nil {
		return "latest"
	}
	if number.Sign() >= 0 {
		return hexutil.EncodeBig(number)
	}
	// It's negative.
	if number.IsInt64() {
		return rpc.BlockNumber(number.Int64()).String()
	}
	// It's negative and large, which is invalid.
	return fmt.Sprintf("<invalid %d>", number)
}

func toFilterJsonRpcArg(q ethereum.FilterQuery) (interface{}, error) {
	arg := map[string]interface{}{
		"address": q.Addresses,
		"topics":  q.Topics,
	}
	if q.BlockHash != nil {
		arg["blockHash"] = *q.BlockHash
		if q.FromBlock != nil || q.ToBlock != nil {
			return nil, errors.New("cannot specify both BlockHash and FromBlock/ToBlock")
		}
	} else {
		if q.FromBlock == nil {
			arg["fromBlock"] = "0x0"
		} else {
			arg["fromBlock"] = toBlockNumJsonRpcArg(q.FromBlock)
		}
		arg["toBlock"] = toBlockNumJsonRpcArg(q.ToBlock)
	}
	return arg, nil
}
