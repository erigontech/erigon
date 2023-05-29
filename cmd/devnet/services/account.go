package services

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
)

func GetNonce(reqGen *requests.RequestGenerator, address libcommon.Address, logger log.Logger) (uint64, error) {
	res, err := requests.GetTransactionCount(reqGen, address, models.Latest, logger)
	if err != nil {
		return 0, fmt.Errorf("failed to get transaction count for address 0x%x: %v", address, err)
	}

	return uint64(res.Result), nil
}
