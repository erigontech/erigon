package accounts_steps

import (
	"context"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/scenarios"
)

func init() {
	scenarios.MustRegisterStepHandlers(
		scenarios.StepHandler(GetBalance),
		scenarios.StepHandler(GetNonce),
	)
}

//const (
//	addr = "0x71562b71999873DB5b286dF957af199Ec94617F7"
//)

func GetBalance(ctx context.Context, addr string, blockNum requests.BlockNumber) (uint64, error) {
	logger := devnet.Logger(ctx)

	node := devnet.CurrentNode(ctx)

	if node == nil {
		node = devnet.SelectBlockProducer(ctx)
	}

	logger.Info("Getting balance", "addess", addr)
	address := libcommon.HexToAddress(addr)

	bal, err := node.GetBalance(address, blockNum)

	if err != nil {
		logger.Error("FAILURE", "error", err)
		return 0, err
	}

	logger.Info("SUCCESS", "balance", bal)

	return bal, nil
}

func GetNonce(ctx context.Context, address libcommon.Address) (uint64, error) {
	node := devnet.CurrentNode(ctx)

	if node == nil {
		node = devnet.SelectBlockProducer(ctx)
	}

	res, err := node.GetTransactionCount(address, requests.BlockNumbers.Latest)

	if err != nil {
		return 0, fmt.Errorf("failed to get transaction count for address 0x%x: %v", address, err)
	}

	return uint64(res.Result), nil
}
