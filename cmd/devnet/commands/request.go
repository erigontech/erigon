package commands

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/log/v3"
)

func pingErigonRpc(logger log.Logger) error {
	err := requests.PingErigonRpc(models.ReqId, logger)
	if err != nil {
		fmt.Printf("FAILURE => %v\n", err)
	}
	return err
}
