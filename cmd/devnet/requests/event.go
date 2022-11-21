package requests

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/common"
)

func GetLogs(reqId int, fromBlock uint64, toBlock uint64, address common.Address, printLogs bool) error {
	reqGen := initialiseRequestGenerator(reqId)
	var b rpctest.EthGetLogs

	if res := reqGen.Erigon(models.ETHGetLogs, reqGen.GetLogs(fromBlock, toBlock, address), &b); res.Err != nil {
		return fmt.Errorf("failed to fetch logs: %v", res.Err)
	}

	s, err := devnetutils.ParseResponse(b)
	if err != nil {
		return fmt.Errorf("error parsing response: %v", err)
	}

	if printLogs {
		fmt.Printf("Logs ======================> %v\n", s)
	}

	return nil
}
