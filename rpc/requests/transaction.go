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

package requests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

type ETHEstimateGas struct {
	CommonResponse
	Number hexutil.Uint64 `json:"result"`
}

func (reqGen *requestGenerator) EstimateGas(args ethereum.CallMsg, blockRef BlockNumber) (uint64, error) {
	var b ETHEstimateGas

	gas := hexutil.Uint64(args.Gas)

	var gasPrice *hexutil.Big

	if args.GasPrice != nil {
		big := hexutil.Big(*args.GasPrice.ToBig())
		gasPrice = &big
	}

	var tipCap *hexutil.Big

	if args.TipCap != nil {
		big := hexutil.Big(*args.TipCap.ToBig())
		tipCap = &big
	}

	var feeCap *hexutil.Big

	if args.FeeCap != nil {
		big := hexutil.Big(*args.FeeCap.ToBig())
		feeCap = &big
	}

	var value *hexutil.Big

	if args.Value != nil {
		big := hexutil.Big(*args.Value.ToBig())
		value = &big
	}

	var data *hexutil.Bytes

	if args.Data != nil {
		bytes := hexutil.Bytes(args.Data)
		data = &bytes
	}

	argsVal, err := json.Marshal(ethapi.CallArgs{
		From:                 &args.From,
		To:                   args.To,
		Gas:                  &gas,
		GasPrice:             gasPrice,
		MaxPriorityFeePerGas: tipCap,
		MaxFeePerGas:         feeCap,
		Value:                value,
		Data:                 data,
		AccessList:           &args.AccessList,
	})

	if err != nil {
		return 0, err
	}

	method, body := reqGen.estimateGas(string(argsVal), blockRef)
	res := reqGen.rpcCallJSON(method, body, &b)

	if res.Err != nil {
		return 0, fmt.Errorf("EstimateGas rpc failed: %w", res.Err)
	}

	if b.Error != nil {
		return 0, fmt.Errorf("EstimateGas rpc failed: %w", b.Error)
	}

	return uint64(b.Number), nil
}

func (req *requestGenerator) estimateGas(callArgs string, blockRef BlockNumber) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":[%s,"%s"],"id":%d}`
	return Methods.ETHEstimateGas, fmt.Sprintf(template, Methods.ETHEstimateGas, callArgs, blockRef, req.reqID)
}

func (reqGen *requestGenerator) GasPrice() (*big.Int, error) {
	var result hexutil.Big

	if err := reqGen.rpcCall(context.Background(), &result, Methods.ETHGasPrice); err != nil {
		return nil, err
	}

	return result.ToInt(), nil
}

func (reqGen *requestGenerator) Call(args ethapi.CallArgs, blockRef rpc.BlockReference, overrides *ethapi.StateOverrides) ([]byte, error) {
	var result hexutil.Bytes

	if err := reqGen.rpcCall(context.Background(), &result, Methods.ETHCall, args, blockRef, overrides); err != nil {
		return nil, err
	}

	return result, nil
}

func (reqGen *requestGenerator) SendTransaction(signedTx types.Transaction) (common.Hash, error) {
	var result common.Hash

	var buf bytes.Buffer
	if err := signedTx.MarshalBinary(&buf); err != nil {
		return common.Hash{}, fmt.Errorf("failed to marshal binary: %v", err)
	}

	if err := reqGen.rpcCall(context.Background(), &result, Methods.ETHSendRawTransaction, hexutil.Bytes(buf.Bytes())); err != nil {
		return common.Hash{}, err
	}

	zeroHash := true

	for _, hb := range result {
		if hb != 0 {
			zeroHash = false
			break
		}
	}

	if zeroHash {
		return common.Hash{}, fmt.Errorf("hash: %s, nonce  %d: returned a zero transaction hash", signedTx.Hash().Hex(), signedTx.GetNonce())
	}

	return result, nil
}

func (req *requestGenerator) GetTransactionByHash(hash common.Hash) (*ethapi.RPCTransaction, error) {
	var result ethapi.RPCTransaction

	if err := req.rpcCall(context.Background(), &result, Methods.ETHGetTransactionByHash, hash); err != nil {
		return nil, err
	}

	return &result, nil
}

func (req *requestGenerator) GetTransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	var result types.Receipt

	if err := req.rpcCall(ctx, &result, Methods.ETHGetTransactionReceipt, hash); err != nil {
		return nil, err
	}

	return &result, nil
}
