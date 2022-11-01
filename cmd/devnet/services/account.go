package services

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/common"
)

func GetNonce(reqId int, address common.Address) (uint64, error) {
	res, err := requests.GetTransactionCount(reqId, address, models.Latest)
	if err != nil {
		return 0, fmt.Errorf("failed to get transaction count for address 0x%x: %v", address, err)
	}

	return uint64(res.Result), nil
}
