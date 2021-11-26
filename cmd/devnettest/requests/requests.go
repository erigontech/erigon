package requests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
)

func parseResponse(resp interface{}) string {
	result, err := json.Marshal(resp)
	if err != nil {
		panic("invalid response")
	}

	return string(result)
}

func GetBalance(address common.Address, blockNum string) {
	reqGen := initialiseRequestGenerator()
	var b rpctest.EthBalance

	res := reqGen.Erigon("eth_getBalance", reqGen.getBalance(address, blockNum), &b)
	if res.Err != nil {
		fmt.Printf("Error getting balance: %v\n", res.Err)
		return
	}

	fmt.Printf("Balance retrieved: %v\n", parseResponse(b))
}

func SendTx(signedTx *types.Transaction) {
	reqGen := initialiseRequestGenerator()
	var b rpctest.EthSendRawTransaction

	var buf bytes.Buffer
	err := (*signedTx).MarshalBinary(&buf)
	if err != nil {
		fmt.Println(err)
		return
	}

	res := reqGen.Erigon("eth_sendRawTransaction", reqGen.sendRawTransaction(buf.Bytes()), &b)
	if res.Err != nil {
		fmt.Printf("Error sending transaction: %v\n", res.Err)
		return
	}

	fmt.Printf("Submitted transaction successfully: %v\n", parseResponse(b))
}

func TxpoolContent() {
	reqGen := initialiseRequestGenerator()
	var b rpctest.EthTxPool

	res := reqGen.Erigon("txpool_content", reqGen.txpoolContent(), &b)
	if res.Err != nil {
		fmt.Printf("Error fetching txpool: %v\n", res.Err)
		return
	}

	fmt.Printf("Txpool content: %v\n", parseResponse(b))
}
