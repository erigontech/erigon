package requests

import (
	"encoding/json"
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/common"
	"strconv"
)

func GetBalance(reqId int, address common.Address, blockNum string) (uint64, error) {
	reqGen := initialiseRequestGenerator(reqId)
	var b rpctest.EthBalance

	if res := reqGen.Erigon("eth_getBalance", reqGen.getBalance(address, blockNum), &b); res.Err != nil {
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
