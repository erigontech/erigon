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
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
)

type ETHEstimateGas struct {
	CommonResponse
	Number hexutil.Uint64 `json:"result"`
}

type ETHGasPrice struct {
	CommonResponse
	Price hexutil.Big `json:"result"`
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
	var b ETHGasPrice

	method, body := reqGen.gasPrice()
	if res := reqGen.call(method, body, &b); res.Err != nil {
		return nil, fmt.Errorf("failed to get gas price: %w", res.Err)
	}

	return b.Price.ToInt(), nil
}

func (req *requestGenerator) gasPrice() (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"id":%d}`
	return Methods.ETHGasPrice, fmt.Sprintf(template, Methods.ETHGasPrice, req.reqID)
}

func (reqGen *requestGenerator) SendTransaction(signedTx types.Transaction) (libcommon.Hash, error) {
	var b EthSendRawTransaction

	var buf bytes.Buffer
	if err := signedTx.MarshalBinary(&buf); err != nil {
		return libcommon.Hash{}, fmt.Errorf("failed to marshal binary: %v", err)
	}

	method, body := reqGen.sendRawTransaction(buf.Bytes())
	if res := reqGen.call(method, body, &b); res.Err != nil {
		return libcommon.Hash{}, fmt.Errorf("could not make to request to eth_sendRawTransaction: %v", res.Err)
	}

	if b.Error != nil {
		return libcommon.Hash{}, fmt.Errorf("SendTransaction rpc failed: %w", b.Error)
	}

	zeroHash := true

	for _, hb := range b.TxnHash {
		if hb != 0 {
			zeroHash = false
			break
		}
	}

	if zeroHash {
		return libcommon.Hash{}, fmt.Errorf("Request: %d, hash: %s, nonce  %d: returned a zero transaction hash", b.RequestId, signedTx.Hash().Hex(), signedTx.GetNonce())
	}

	return b.TxnHash, nil
}

func (req *requestGenerator) sendRawTransaction(signedTx []byte) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["0x%x"],"id":%d}`
	return Methods.ETHSendRawTransaction, fmt.Sprintf(template, Methods.ETHSendRawTransaction, signedTx, req.reqID)
}
