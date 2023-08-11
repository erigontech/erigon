package requests

import (
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
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
	Result BlockResult `json:"result"`
}

type BlockResult struct {
	BlockNumber  BlockNumber       `json:"number"`
	Difficulty   hexutil.Big       `json:"difficulty"`
	Miner        libcommon.Address `json:"miner"`
	Transactions []Transaction     `json:"transactions"`
	TxRoot       libcommon.Hash    `json:"transactionsRoot"`
	Hash         libcommon.Hash    `json:"hash"`
}

type Transaction struct {
	From     libcommon.Address  `json:"from"`
	To       *libcommon.Address `json:"to"` // Pointer because it might be missing
	Hash     string             `json:"hash"`
	Gas      hexutil.Big        `json:"gas"`
	GasPrice hexutil.Big        `json:"gasPrice"`
	Input    hexutility.Bytes   `json:"input"`
	Value    hexutil.Big        `json:"value"`
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

func (reqGen *requestGenerator) GetBlockByNumber(blockNum uint64, withTxs bool) (*BlockResult, error) {
	var b EthBlockByNumber

	method, body := reqGen.getBlockByNumber(blockNum, withTxs)
	res := reqGen.call(method, body, &b)
	if res.Err != nil {
		return nil, fmt.Errorf("error getting block by number: %v", res.Err)
	}

	if b.Error != nil {
		return nil, fmt.Errorf("error populating response object: %v", b.Error)
	}

	b.Result.BlockNumber = BlockNumber(fmt.Sprint(blockNum))

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
