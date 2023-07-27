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

type EthTransaction struct {
	From     libcommon.Address  `json:"from"`
	To       *libcommon.Address `json:"to"` // Pointer because it might be missing
	Hash     string             `json:"hash"`
	Gas      hexutil.Big        `json:"gas"`
	GasPrice hexutil.Big        `json:"gasPrice"`
	Input    hexutility.Bytes   `json:"input"`
	Value    hexutil.Big        `json:"value"`
}

func (reqGen *requestGenerator) GetBalance(address libcommon.Address, blockNum BlockNumber) (*big.Int, error) {
	var b EthBalance

	method, body := reqGen.getBalance(address, blockNum)
	if res := reqGen.call(method, body, &b); res.Err != nil {
		return &big.Int{}, fmt.Errorf("failed to get balance: %v", res.Err)
	}

	return b.Balance.ToInt(), nil
}

func (req *requestGenerator) getBalance(address libcommon.Address, blockNum BlockNumber) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["0x%x","%v"],"id":%d}`
	return Methods.ETHGetBalance, fmt.Sprintf(template, Methods.ETHGetBalance, address, blockNum, req.reqID)
}
