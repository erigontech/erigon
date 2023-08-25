package requests

import (
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/rpc"
)

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

func (reqGen *requestGenerator) GetCode(address libcommon.Address, blockRef rpc.BlockReference) (hexutility.Bytes, error) {
	var result hexutility.Bytes

	if err := reqGen.callCli(&result, Methods.ETHGetCode, address, blockRef); err != nil {
		return nil, err
	}

	return result, nil
}

func (reqGen *requestGenerator) GetBalance(address libcommon.Address, blockRef rpc.BlockReference) (*big.Int, error) {
	var result hexutil.Big

	if err := reqGen.callCli(&result, Methods.ETHGetBalance, address, blockRef); err != nil {
		return nil, err
	}

	return result.ToInt(), nil
}

func (reqGen *requestGenerator) GetTransactionCount(address libcommon.Address, blockRef rpc.BlockReference) (*big.Int, error) {
	var result hexutil.Big

	if err := reqGen.callCli(&result, Methods.ETHGetTransactionCount, address, blockRef); err != nil {
		return nil, err
	}

	return result.ToInt(), nil
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
