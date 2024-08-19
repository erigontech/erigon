package commands

import (
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/client"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"fmt"
	"encoding/json"
)

func (api *APIImpl) forwardGetTransactionByHash(rpcUrl string, txnHash common.Hash) (json.RawMessage, error) {
	asString := txnHash.String()
	res, err := client.JSONRPCCall(rpcUrl, "eth_getTransactionByHash", asString)
	if err != nil {
		return nil, err
	}

	if res.Error != nil {
		return nil, fmt.Errorf("RPC error response is: %s", res.Error.Message)
	}

	return res.Result, nil
}
