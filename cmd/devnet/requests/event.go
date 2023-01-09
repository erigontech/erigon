package requests

import (
	"fmt"
	"strconv"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
)

func GetAndCompareLogs(reqId int, fromBlock uint64, toBlock uint64, expected rpctest.Log) error {
	fmt.Printf("\nGETTING AND COMPARING LOGS\n")
	reqGen := initialiseRequestGenerator(reqId)
	var b rpctest.EthGetLogs

	if res := reqGen.Erigon(models.ETHGetLogs, reqGen.GetLogs(fromBlock, toBlock, expected.Address), &b); res.Err != nil {
		return fmt.Errorf("failed to fetch logs: %v", res.Err)
	}

	if len(b.Result) == 0 {
		return fmt.Errorf("logs result should not be empty")
	}

	eventLog := b.Result[0]

	actual := devnetutils.BuildLog(eventLog.TxHash, strconv.FormatUint(uint64(eventLog.BlockNumber), 10),
		eventLog.Address, eventLog.Topics, eventLog.Data, eventLog.TxIndex, eventLog.BlockHash, eventLog.Index,
		eventLog.Removed)

	// compare the log events
	errs, ok := devnetutils.CompareLogEvents(expected, actual)
	if !ok {
		fmt.Printf("FAILURE => log result is incorrect: %v\n", errs)
		return fmt.Errorf("incorrect logs: %v", errs)
	}

	_, err := devnetutils.ParseResponse(b)
	if err != nil {
		return fmt.Errorf("error parsing response: %v", err)
	}

	fmt.Println("SUCCESS => Logs compared successfully, no discrepancies")

	return nil
}
