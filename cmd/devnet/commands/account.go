package jsonrpc

import (
	"context"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/scenarios"
)

func init() {
	scenarios.MustRegisterStepHandlers(
		scenarios.StepHandler(GetBalance),
	)
}

//const (
//	addr = "0x71562b71999873DB5b286dF957af199Ec94617F7"
//)

func GetBalance(ctx context.Context, addr string, blockNum requests.BlockNumber, checkBal uint64) {
	logger := devnet.Logger(ctx)

	logger.Info("Getting balance", "addeess", addr)

	address := libcommon.HexToAddress(addr)
	bal, err := devnet.SelectNode(ctx).GetBalance(address, blockNum)

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
