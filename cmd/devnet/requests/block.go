package requests

import (
	"bytes"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
)

type EthBlockNumber struct {
	CommonResponse
	Number hexutil.Uint64 `json:"result"`
}

type EthBlockByNumber struct {
	CommonResponse
	Result *EthBlockByNumberResult `json:"result"`
}

type EthBlockByNumberResult struct {
	Difficulty   hexutil.Big       `json:"difficulty"`
	Miner        libcommon.Address `json:"miner"`
	Transactions []EthTransaction  `json:"transactions"`
	TxRoot       libcommon.Hash    `json:"transactionsRoot"`
	Hash         libcommon.Hash    `json:"hash"`
}

type EthGetTransactionCount struct {
	CommonResponse
	Result hexutil.Uint64 `json:"result"`
}

type EthSendRawTransaction struct {
	CommonResponse
	TxnHash libcommon.Hash `json:"result"`
}

func (reqGen *RequestGenerator) BlockNumber() (uint64, error) {
	var b EthBlockNumber

	method, body := reqGen.blockNumber()
	res := reqGen.call(method, body, &b)
	number := uint64(b.Number)

	if res.Err != nil {
		return number, fmt.Errorf("error getting current block number: %v", res.Err)
	}

	return number, nil
}

func (req *RequestGenerator) blockNumber() (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"id":%d}`
	return Methods.ETHBlockNumber, fmt.Sprintf(template, Methods.ETHBlockNumber, req.reqID)
}

func (reqGen *RequestGenerator) GetBlockByNumber(blockNum uint64, withTxs bool) (EthBlockByNumber, error) {
	var b EthBlockByNumber

	method, body := reqGen.getBlockByNumber(blockNum, withTxs)
	res := reqGen.call(method, body, &b)
	if res.Err != nil {
		return b, fmt.Errorf("error getting block by number: %v", res.Err)
	}

	if b.Error != nil {
		return b, fmt.Errorf("error populating response object: %v", b.Error)
	}

	return b, nil
}

func (req *RequestGenerator) getBlockByNumber(blockNum uint64, withTxs bool) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["0x%x",%t],"id":%d}`
	return Methods.ETHGetBlockByNumber, fmt.Sprintf(template, Methods.ETHGetBlockByNumber, blockNum, withTxs, req.reqID)
}

func (req *RequestGenerator) getBlockByNumberI(blockNum string, withTxs bool) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["%s",%t],"id":%d}`
	return Methods.ETHGetBlockByNumber, fmt.Sprintf(template, Methods.ETHGetBlockByNumber, blockNum, withTxs, req.reqID)
}

func (reqGen *RequestGenerator) GetBlockByNumberDetails(blockNum string, withTxs bool) (map[string]interface{}, error) {
	var b struct {
		CommonResponse
		Result interface{} `json:"result"`
	}

	method, body := reqGen.getBlockByNumberI(blockNum, withTxs)
	res := reqGen.call(method, body, &b)
	if res.Err != nil {
		return nil, fmt.Errorf("error getting block by number: %v", res.Err)
	}

	if b.Error != nil {
		return nil, fmt.Errorf("error populating response object: %v", b.Error)
	}

	m, ok := b.Result.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("cannot convert type")
	}

	return m, nil
}

func (reqGen *RequestGenerator) GetTransactionCount(address libcommon.Address, blockNum BlockNumber) (EthGetTransactionCount, error) {
	var b EthGetTransactionCount

	method, body := reqGen.getTransactionCount(address, blockNum)
	if res := reqGen.call(method, body, &b); res.Err != nil {
		return b, fmt.Errorf("error getting transaction count: %v", res.Err)
	}

	if b.Error != nil {
		return b, fmt.Errorf("error populating response object: %v", b.Error)
	}

	return b, nil
}

func (req *RequestGenerator) getTransactionCount(address libcommon.Address, blockNum BlockNumber) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["0x%x","%v"],"id":%d}`
	return Methods.ETHGetTransactionCount, fmt.Sprintf(template, Methods.ETHGetTransactionCount, address, blockNum, req.reqID)
}

func (reqGen *RequestGenerator) SendTransaction(signedTx *types.Transaction) (*libcommon.Hash, error) {
	var b EthSendRawTransaction

	var buf bytes.Buffer
	if err := (*signedTx).MarshalBinary(&buf); err != nil {
		return nil, fmt.Errorf("failed to marshal binary: %v", err)
	}

	method, body := reqGen.sendRawTransaction(buf.Bytes())
	if res := reqGen.call(method, body, &b); res.Err != nil {
		return nil, fmt.Errorf("could not make to request to eth_sendRawTransaction: %v", res.Err)
	}

	return &b.TxnHash, nil
}

func (req *RequestGenerator) sendRawTransaction(signedTx []byte) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["0x%x"],"id":%d}`
	return Methods.ETHSendRawTransaction, fmt.Sprintf(template, Methods.ETHSendRawTransaction, signedTx, req.reqID)
}
