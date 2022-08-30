package services

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/ledgerwatch/erigon/common"
)

// GetNonce fetches the latest nonce of the developer account by making an JSONRPC request
func GetNonce(reqId int) (uint64, error) {
	blockNum := "latest"
	address := common.HexToAddress(DevAddress)

	res, err := requests.GetTransactionCount(reqId, address, blockNum)
	if err != nil {
		return 0, fmt.Errorf("failed to get transaction count for address 0x%x: %v", address, err)
	}

	return uint64(res.Result), nil
}
