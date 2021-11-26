package services

import (
	"fmt"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
)

var devnetSignPrivateKey, _ = crypto.HexToECDSA("26e86e45f6fc45ec6e2ecd128cec80fa1d1505e5507dcd2ae58c3130a7a97b48")

func ValidateInputs(getBalance bool, sendTx bool, txpoolContent bool, blockNum string, value uint64, to string) {
	if !(getBalance) && !(sendTx) && !(txpoolContent) {
		panic("At least one function flag (get-balance, send-tx, txpool-content) should be true")
	}

	seen := false
	for _, val := range []bool{getBalance, sendTx, txpoolContent} {
		if val {
			if seen {
				panic("Only function flag (get-balance, send-tx, txpool-content) can be true at a time")
			}
			seen = true
		}
	}

	if value <= 0 {
		panic("Value must be greater than zero")
	}

	if getBalance {
		if to == "" {
			panic("Cannot check balance of empty address")
		}
		if blockNum != "pending" && blockNum != "latest" && blockNum != "earliest" {
			panic("Block number must be 'pending', 'latest' or 'earliest'")
		}
	}

	if sendTx && to == "" {
		panic("Cannot send to empty address")
	}

}

func ParseRequests(getBalance bool, sendTx bool, txpoolContent bool, clearDev bool, blockNum string, value uint64, to string) {
	if getBalance {
		toAddress := common.HexToAddress(to)
		requests.GetBalance(toAddress, blockNum)
	}

	if sendTx {
		toAddress := common.HexToAddress(to)
		signer := types.LatestSigner(params.AllCliqueProtocolChanges)
		signedTx, _ := types.SignTx(types.NewTransaction(0, toAddress, uint256.NewInt(value),
			params.TxGas, uint256.NewInt(50000), nil), *signer, devnetSignPrivateKey)
		requests.SendTx(&signedTx)
	}

	if txpoolContent {
		requests.TxpoolContent()
	}

	if clearDev {
		clearDevDB()
	}
}

func clearDevDB() {
	fmt.Printf("Clearing ~/dev\n")
	//
	//_, err := exec.Command("rm", "-rf", "~/dev", "~/dev2").Output()
	//if err != nil {
	//	fmt.Println(err)
	//}
}
