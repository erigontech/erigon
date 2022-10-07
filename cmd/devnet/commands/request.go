package commands

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
)

func pingErigonRpc() {
	if err := requests.PingErigonRpc(models.ReqId); err != nil {
		fmt.Printf("error mocking get request: %v\n", err)
	}
}
