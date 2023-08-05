package requests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"

	ethereum "github.com/ledgerwatch/erigon"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc"
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

	var tip *hexutil.Big

	if args.Tip != nil {
		big := hexutil.Big(*args.Tip.ToBig())
		tip = &big
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

	var data *hexutility.Bytes

	if args.Data != nil {
		bytes := hexutility.Bytes(args.Data)
		data = &bytes
	}

	argsVal, err := json.Marshal(ethapi.CallArgs{
		From:                 &args.From,
		To:                   args.To,
		Gas:                  &gas,
		GasPrice:             gasPrice,
		MaxPriorityFeePerGas: tip,
		MaxFeePerGas:         feeCap,
		Value:                value,
		Data:                 data,
		AccessList:           &args.AccessList,
	})

	if err != nil {
		return 0, err
	}

	method, body := reqGen.estimateGas(string(argsVal), blockRef)
	res := reqGen.call(method, body, &b)

	if res.Err != nil {
		return 0, fmt.Errorf("EstimateGas rpc failed: %w", res.Err)
	}

	if b.Error != nil {
		return 0, fmt.Errorf("EstimateGas rpc failed: %w", b.Error)
	}

	fmt.Println("EST GAS", b.Number)
	return uint64(b.Number), nil
}

func (req *requestGenerator) estimateGas(callArgs string, blockRef BlockNumber) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":[%s,"%s"],"id":%d}`
	return Methods.ETHEstimateGas, fmt.Sprintf(template, Methods.ETHEstimateGas, callArgs, blockRef, req.reqID)
}

func (reqGen *requestGenerator) GasPrice() (*big.Int, error) {
	var result hexutil.Big

	if err := reqGen.callCli(&result, Methods.ETHGasPrice); err != nil {
		return nil, err
	}

	return result.ToInt(), nil
}

func (reqGen *requestGenerator) Call(args ethapi.CallArgs, blockRef rpc.BlockReference, overrides *ethapi.StateOverrides) ([]byte, error) {
	var result hexutility.Bytes

	if err := reqGen.callCli(&result, Methods.ETHCall, args, blockRef, overrides); err != nil {
		return nil, err
	}

	return result, nil
}

func (reqGen *requestGenerator) SendTransaction(signedTx types.Transaction) (libcommon.Hash, error) {
	var result libcommon.Hash

	var buf bytes.Buffer
	if err := signedTx.MarshalBinary(&buf); err != nil {
		return libcommon.Hash{}, fmt.Errorf("failed to marshal binary: %v", err)
	}

	if err := reqGen.callCli(&result, Methods.ETHSendRawTransaction, hexutility.Bytes(buf.Bytes())); err != nil {
		return libcommon.Hash{}, err
	}

	zeroHash := true

	for _, hb := range result {
		if hb != 0 {
			zeroHash = false
			break
		}
	}

	if zeroHash {
		return libcommon.Hash{}, fmt.Errorf("Hash: %s, nonce  %d: returned a zero transaction hash", signedTx.Hash().Hex(), signedTx.GetNonce())
	}

	return result, nil
}

func (req *requestGenerator) GetTransactionByHash(hash libcommon.Hash) (*jsonrpc.RPCTransaction, error) {
	var result jsonrpc.RPCTransaction

	if err := req.callCli(&result, Methods.ETHGetTransactionByHash, hash); err != nil {
		return nil, err
	}

	return &result, nil
}

func (req *requestGenerator) GetTransactionReceipt(hash libcommon.Hash) (*types.Receipt, error) {
	var result types.Receipt

	if err := req.callCli(&result, Methods.ETHGetTransactionReceipt, hash); err != nil {
		return nil, err
	}

	return &result, nil
}
