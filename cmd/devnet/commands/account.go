package commands

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/log/v3"
)

const (
	addr = "0x71562b71999873DB5b286dF957af199Ec94617F7"
)

func callGetBalance(reqGen *requests.RequestGenerator, addr string, blockNum models.BlockNumber, checkBal uint64, logger log.Logger) {
	logger.Info("Getting balance", "addeess", addr)
	address := libcommon.HexToAddress(addr)
	bal, err := requests.GetBalance(reqGen, address, blockNum, logger)
	if err != nil {
		logger.Error("FAILURE", "error", err)
		return
	}

	if checkBal > 0 && checkBal != bal {
		logger.Error("FAILURE => Balance mismatch", "expected", checkBal, "got", bal)
		return
	}

	logger.Info("SUCCESS", "balance", bal)
}
