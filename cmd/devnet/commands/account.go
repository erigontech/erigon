package commands

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/log/v3"
)

const (
	addr = "0x71562b71999873DB5b286dF957af199Ec94617F7"
)

func callGetBalance(addr string, blockNum models.BlockNumber, checkBal uint64, logger log.Logger) {
	fmt.Printf("Getting balance for address: %q...\n", addr)
	address := libcommon.HexToAddress(addr)
	bal, err := requests.GetBalance(models.ReqId, address, blockNum, logger)
	if err != nil {
		fmt.Printf("FAILURE => %v\n", err)
		return
	}

	if checkBal > 0 && checkBal != bal {
		fmt.Printf("FAILURE => Balance should be %d, got %d\n", checkBal, bal)
		return
	}

	fmt.Printf("SUCCESS => Balance: %d\n", bal)
}
