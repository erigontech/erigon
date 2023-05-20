package requests

import (
	"encoding/json"
	"fmt"
	"strconv"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
)

func GetBalance(reqId int, address libcommon.Address, blockNum models.BlockNumber, logger log.Logger) (uint64, error) {
	reqGen := initialiseRequestGenerator(reqId, logger)
	var b rpctest.EthBalance

	if res := reqGen.Erigon(models.ETHGetBalance, reqGen.GetBalance(address, blockNum), &b); res.Err != nil {
		return 0, fmt.Errorf("failed to get balance: %v", res.Err)
	}

	bal, err := json.Marshal(b.Balance)
	if err != nil {
		fmt.Println(err)
	}

	balStr := string(bal)[3 : len(bal)-1]
	balance, err := strconv.ParseInt(balStr, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("cannot convert balance to decimal: %v", err)
	}

	return uint64(balance), nil
}
