package requests

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
)

func GetBalance(reqGen *RequestGenerator, address libcommon.Address, blockNum models.BlockNumber, logger log.Logger) (uint64, error) {
	var b rpctest.EthBalance

	if res := reqGen.Erigon(models.ETHGetBalance, reqGen.GetBalance(address, blockNum), &b); res.Err != nil {
		return 0, fmt.Errorf("failed to get balance: %v", res.Err)
	}
	if !b.Balance.ToInt().IsUint64() {
		return 0, fmt.Errorf("balance is not uint64")
	}
	return b.Balance.ToInt().Uint64(), nil
}
