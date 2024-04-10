package commands

import (
	"fmt"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/client"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"math/big"
	"encoding/json"
)

func (api *APIImpl) gasPriceZk(rpcUrl string) (*hexutil.Big, error) {
	res, err := client.JSONRPCCall(rpcUrl, "eth_gasPrice")
	if err != nil {
		return &hexutil.Big{}, err
	}

	if res.Error != nil {
		return &hexutil.Big{}, fmt.Errorf("RPC error response: %s", res.Error.Message)
	}
	if res.Error != nil {
		return &hexutil.Big{}, fmt.Errorf("RPC error response: %s", res.Error.Message)
	}

	var resultString string
	if err := json.Unmarshal(res.Result, &resultString); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %v", err)
	}

	price, ok := big.NewInt(0).SetString(resultString[2:], 16)
	if !ok {
		return nil, fmt.Errorf("failed to convert result to big.Int")
	}

	return (*hexutil.Big)(price), nil
}
