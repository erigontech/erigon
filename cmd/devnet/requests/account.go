package requests

import (
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/common/hexutil"
)

type EthBalance struct {
	CommonResponse
	Balance hexutil.Big `json:"result"`
}

type EthCode struct {
	CommonResponse
	Code hexutility.Bytes `json:"result"`
}

type DebugAccountAt struct {
	CommonResponse
	Result AccountResult `json:"result"`
}

// AccountResult is the result struct for GetProof
type AccountResult struct {
	Address      libcommon.Address `json:"address"`
	AccountProof []string          `json:"accountProof"`
	Balance      *hexutil.Big      `json:"balance"`
	CodeHash     libcommon.Hash    `json:"codeHash"`
	Code         hexutility.Bytes  `json:"code"`
	Nonce        hexutil.Uint64    `json:"nonce"`
	StorageHash  libcommon.Hash    `json:"storageHash"`
	StorageProof []StorageResult   `json:"storageProof"`
}

type StorageResult struct {
	Key   string       `json:"key"`
	Value *hexutil.Big `json:"value"`
	Proof []string     `json:"proof"`
}

func (reqGen *requestGenerator) GetCode(address libcommon.Address, blockRef BlockNumber) (hexutility.Bytes, error) {
	var b EthCode

	method, body := reqGen.getCode(address, blockRef)
	if res := reqGen.call(method, body, &b); res.Err != nil {
		return hexutility.Bytes{}, fmt.Errorf("failed to get code: %w", res.Err)
	}

	if b.Error != nil {
		return hexutility.Bytes{}, fmt.Errorf("Failed to get code: rpc failed: %w", b.Error)
	}

	return b.Code, nil
}

func (req *requestGenerator) getCode(address libcommon.Address, blockRef BlockNumber) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["0x%x","%s"],"id":%d}`
	return Methods.ETHGetCode, fmt.Sprintf(template, Methods.ETHGetCode, address, blockRef, req.reqID)
}

func (reqGen *requestGenerator) GetBalance(address libcommon.Address, blockNum BlockNumber) (*big.Int, error) {
	var b EthBalance

	method, body := reqGen.getBalance(address, blockNum)
	if res := reqGen.call(method, body, &b); res.Err != nil {
		return &big.Int{}, fmt.Errorf("failed to get balance: %w", res.Err)
	}

	if b.Error != nil {
		return &big.Int{}, fmt.Errorf("Failed to get balance: rpc failed: %w", b.Error)
	}

	return b.Balance.ToInt(), nil
}

func (req *requestGenerator) getBalance(address libcommon.Address, blockNum BlockNumber) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["0x%x","%v"],"id":%d}`
	return Methods.ETHGetBalance, fmt.Sprintf(template, Methods.ETHGetBalance, address, blockNum, req.reqID)
}

func (reqGen *requestGenerator) GetTransactionCount(address libcommon.Address, blockNum BlockNumber) (*big.Int, error) {
	var b EthGetTransactionCount

	method, body := reqGen.getTransactionCount(address, blockNum)
	if res := reqGen.call(method, body, &b); res.Err != nil {
		return nil, fmt.Errorf("error getting transaction count: %w", res.Err)
	}

	if b.Error != nil {
		return nil, fmt.Errorf("error populating response object: %w", b.Error)
	}

	return big.NewInt(int64(b.Result)), nil
}

func (req *requestGenerator) getTransactionCount(address libcommon.Address, blockNum BlockNumber) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["0x%x","%v"],"id":%d}`
	return Methods.ETHGetTransactionCount, fmt.Sprintf(template, Methods.ETHGetTransactionCount, address, blockNum, req.reqID)
}

func (reqGen *requestGenerator) DebugAccountAt(blockHash libcommon.Hash, txIndex uint64, account libcommon.Address) (*AccountResult, error) {
	var b DebugAccountAt

	method, body := reqGen.debugAccountAt(blockHash, txIndex, account)
	if res := reqGen.call(method, body, &b); res.Err != nil {
		return nil, fmt.Errorf("failed to get account: %v", res.Err)
	}

	if b.Error != nil {
		return nil, fmt.Errorf("failed to get account: rpc failed: %w", b.Error)
	}

	return &b.Result, nil
}

func (req *requestGenerator) debugAccountAt(blockHash libcommon.Hash, txIndex uint64, account libcommon.Address) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["0x%x",%d, "0x%x"],"id":%d}`
	return Methods.DebugAccountAt, fmt.Sprintf(template, Methods.DebugAccountAt, blockHash, txIndex, account, req.reqID)
}
