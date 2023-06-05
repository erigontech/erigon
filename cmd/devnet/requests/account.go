package requests

import (
	"fmt"

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

func (reqGen *RequestGenerator) GetBalance(address libcommon.Address, blockNum BlockNumber) (uint64, error) {
	var b EthBalance

	method, body := reqGen.getBalance(address, blockNum)
	if res := reqGen.call(method, body, &b); res.Err != nil {
		return 0, fmt.Errorf("failed to get balance: %v", res.Err)
	}

	if !b.Balance.ToInt().IsUint64() {
		return 0, fmt.Errorf("balance is not uint64")
	}

	return b.Balance.ToInt().Uint64(), nil
}

func (req *RequestGenerator) getBalance(address libcommon.Address, blockNum BlockNumber) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["0x%x","%v"],"id":%d}`
	return Methods.ETHGetBalance, fmt.Sprintf(template, Methods.ETHGetBalance, address, blockNum, req.reqID)
}
