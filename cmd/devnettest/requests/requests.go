package requests

import (
	"crypto/ecdsa"
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/common"
)

func GetBalance(address common.Address, blockNum string) {
	reqGen := initialiseRequestGenerator()
	var b rpctest.EthBalance

	res := reqGen.Erigon("eth_getBalance", reqGen.getBalance(address, blockNum), &b)
	if res.Err != nil {
		fmt.Printf("Error getting balance: %v\n", res.Err)
		return
	}

	fmt.Printf("Balance is: %v\n", b.Balance.ToInt())
}

func SendTx(from *ecdsa.PrivateKey, to common.Address, value uint64) {

}

func TxpoolContent() string {
	return ""
}
