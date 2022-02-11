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
		panic(err)
	}

	return string(result)
}

func GetBalance(reqId int, address common.Address, blockNum string) {
	reqGen := initialiseRequestGenerator(reqId)
	var b rpctest.EthBalance

	res := reqGen.Erigon("eth_getBalance", reqGen.getBalance(address, blockNum), &b)
	if res.Err != nil {
		fmt.Printf("Error getting balance: %v\n", res.Err)
		return
	}

	fmt.Printf("Balance retrieved: %v\n", parseResponse(b))
}

func SendTx(reqId int, signedTx *types.Transaction) (*common.Hash, error) {
	reqGen := initialiseRequestGenerator(reqId)
	var b rpctest.EthSendRawTransaction

	var buf bytes.Buffer
	err := (*signedTx).MarshalBinary(&buf)
	if err != nil {
		return nil, err
	}

	res := reqGen.Erigon("eth_sendRawTransaction", reqGen.sendRawTransaction(buf.Bytes()), &b)
	if res.Err != nil {
		return nil, err
	}

	fmt.Printf("Submitted transaction successfully: %v\n", parseResponse(b))
	return &b.TxnHash, nil
}

func TxpoolContent(reqId int) {
	reqGen := initialiseRequestGenerator(reqId)
	var b rpctest.EthTxPool

	res := reqGen.Erigon("txpool_content", reqGen.txpoolContent(), &b)
	if res.Err != nil {
		fmt.Printf("Error fetching txpool: %v\n", res.Err)
		return
	}

	fmt.Printf("Txpool content: %v\n", parseResponse(b))
}

func ParityList(reqId int, account common.Address, quantity int, offset []byte, blockNum string) {
	reqGen := initialiseRequestGenerator(reqId)
	var b rpctest.ParityListStorageKeysResult

	res := reqGen.Erigon("parity_listStorageKeys", reqGen.parityStorageKeyListContent(account, quantity, offset, blockNum), &b)
	if res.Err != nil {
		fmt.Printf("Error fetching storage keys: %v\n", res.Err)
		return
	}

	fmt.Printf("Storage keys: %v\n", parseResponse(b))

}
