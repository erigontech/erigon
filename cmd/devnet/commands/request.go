package commands

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
)

func pingErigonRpc() error {
	err := requests.PingErigonRpc(models.ReqId)
	if err != nil {
		fmt.Printf("FAILURE => %v\n", err)
	}
	return err
}
