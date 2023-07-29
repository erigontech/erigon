package requests

import (
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc"
)

type EthBlockNumber struct {
	CommonResponse
	Number hexutil.Uint64 `json:"result"`
}

type BlockNumber string

func (bn BlockNumber) Uint64() uint64 {
	if b, ok := math.ParseBig256(string(bn)); ok {
		return b.Uint64()
	}

	return 0
}

func AsBlockNumber(n *big.Int) BlockNumber {
	return BlockNumber(hexutil.EncodeBig(n))
}

var BlockNumbers = struct {
	// Latest is the parameter for the latest block
	Latest BlockNumber
	// Earliest is the parameter for the earliest block
	Earliest BlockNumber
	// Pending is the parameter for the pending block
	Pending BlockNumber
}{
	Latest:   "latest",
	Earliest: "earliest",
	Pending:  "pending",
}

type EthBlockByNumber struct {
	CommonResponse
	Result Block `json:"result"`
}

type Block struct {
	*types.Header
	Hash         libcommon.Hash            `json:"hash"`
	Miner        libcommon.Address         `json:"miner"`
	Transactions []*jsonrpc.RPCTransaction `json:"transactions"`
}

type EthGetTransactionCount struct {
	CommonResponse
	Result hexutil.Uint64 `json:"result"`
}

type EthSendRawTransaction struct {
	CommonResponse
	TxnHash libcommon.Hash `json:"result"`
}

func (reqGen *requestGenerator) BlockNumber() (uint64, error) {
	var b EthBlockNumber

	method, body := reqGen.blockNumber()
	res := reqGen.call(method, body, &b)
	number := uint64(b.Number)

	if res.Err != nil {
		return number, fmt.Errorf("error getting current block number: %v", res.Err)
	}

	return number, nil
}

func (req *requestGenerator) blockNumber() (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"id":%d}`
	return Methods.ETHBlockNumber, fmt.Sprintf(template, Methods.ETHBlockNumber, req.reqID)
}

func (reqGen *requestGenerator) GetBlockByNumber(blockNum uint64, withTxs bool) (*Block, error) {
	var b EthBlockByNumber

	method, body := reqGen.getBlockByNumber(blockNum, withTxs)
	res := reqGen.call(method, body, &b)
	if res.Err != nil {
		return nil, fmt.Errorf("error getting block by number: %v", res.Err)
	}

	if b.Error != nil {
		return nil, fmt.Errorf("error populating response object: %v", b.Error)
	}

	b.Result.Number = big.NewInt(int64(blockNum))

	return &b.Result, nil
}

func (req *requestGenerator) getBlockByNumber(blockNum uint64, withTxs bool) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["0x%x",%t],"id":%d}`
	return Methods.ETHGetBlockByNumber, fmt.Sprintf(template, Methods.ETHGetBlockByNumber, blockNum, withTxs, req.reqID)
}

func (req *requestGenerator) getBlockByNumberI(blockNum string, withTxs bool) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["%s",%t],"id":%d}`
	return Methods.ETHGetBlockByNumber, fmt.Sprintf(template, Methods.ETHGetBlockByNumber, blockNum, withTxs, req.reqID)
}

func (reqGen *requestGenerator) GetBlockDetailsByNumber(blockNum string, withTxs bool) (map[string]interface{}, error) {
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

func (req *requestGenerator) GetRootHash(startBlock uint64, endBlock uint64) (libcommon.Hash, error) {
	var result libcommon.Hash

	if err := req.callCli(result, Methods.ETHGetTransactionReceipt, startBlock, endBlock); err != nil {
		return libcommon.Hash{}, err
	}

	return result, nil
}
