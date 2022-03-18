package requests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
)

func post(client *http.Client, url, request string, response interface{}) error {
	start := time.Now()
	r, err := client.Post(url, "application/json", strings.NewReader(request))
	if err != nil {
		return fmt.Errorf("client failed to make post request: %v", err)
	}
	defer r.Body.Close()

	if r.StatusCode != 200 {
		return fmt.Errorf("status %s", r.Status)
	}

	decoder := json.NewDecoder(r.Body)
	err = decoder.Decode(response)
	if err != nil {
		return fmt.Errorf("failed to decode response: %v", err)
	}

	log.Info("Got in", "time", time.Since(start).Seconds())
	return nil
}

func GetBalance(reqId int, address common.Address, blockNum string) error {
	reqGen := initialiseRequestGenerator(reqId)
	var b rpctest.EthBalance

	if res := reqGen.Erigon("eth_getBalance", reqGen.getBalance(address, blockNum), &b); res.Err != nil {
		return fmt.Errorf("failed to get balance: %v", res.Err)
	}

	s, err := parseResponse(b)
	if err != nil {
		return fmt.Errorf("error parsing resonse: %v", err)
	}

	fmt.Printf("Balance retrieved: %v\n", s)
	return nil
}

func SendTx(reqId int, signedTx *types.Transaction) (*common.Hash, error) {
	reqGen := initialiseRequestGenerator(reqId)
	var b rpctest.EthSendRawTransaction

	var buf bytes.Buffer
	if err := (*signedTx).MarshalBinary(&buf); err != nil {
		return nil, fmt.Errorf("failed to marshal binary: %v", err)
	}

	if res := reqGen.Erigon("eth_sendRawTransaction", reqGen.sendRawTransaction(buf.Bytes()), &b); res.Err != nil {
		return nil, fmt.Errorf("could not make request to eth_sendRawTransaction: %v", res.Err)
	}

	s, err := parseResponse(b)
	if err != nil {
		return nil, fmt.Errorf("error parsing resonse: %v", err)
	}

	fmt.Printf("Submitted transaction successfully: %v\n", s)
	return &b.TxnHash, nil
}

func TxpoolContent(reqId int) error {
	reqGen := initialiseRequestGenerator(reqId)
	var b rpctest.EthTxPool

	if res := reqGen.Erigon("txpool_content", reqGen.txpoolContent(), &b); res.Err != nil {
		return fmt.Errorf("failed to fetch txpool content: %v", res.Err)
	}

	s, err := parseResponse(b)
	if err != nil {
		return fmt.Errorf("error parsing resonse: %v", err)
	}

	fmt.Printf("Txpool content: %v\n", s)
	return nil
}

func ParityList(reqId int, account common.Address, quantity int, offset []byte, blockNum string) error {
	reqGen := initialiseRequestGenerator(reqId)
	var b rpctest.ParityListStorageKeysResult

	if res := reqGen.Erigon("parity_listStorageKeys", reqGen.parityStorageKeyListContent(account, quantity, offset, blockNum), &b); res.Err != nil {
		return fmt.Errorf("failed to fetch storage keys: %v", res.Err)
	}

	s, err := parseResponse(b)
	if err != nil {
		return fmt.Errorf("error parsing resonse: %v", err)
	}

	fmt.Printf("Storage keys: %v\n", s)
	return nil
}

func GetLogs(reqId int, fromBlock, toBlock uint64, address common.Address) error {
	reqGen := initialiseRequestGenerator(reqId)
	var b rpctest.EthGetLogs

	if res := reqGen.Erigon("eth_getLogs", reqGen.getLogs(fromBlock, toBlock, address), &b); res.Err != nil {
		return fmt.Errorf("Error fetching logs: %v\n", res.Err)
	}

	s, err := parseResponse(b)
	if err != nil {
		return fmt.Errorf("error parsing resonse: %v", err)
	}

	fmt.Printf("Logs: %v\n", s)
	return nil
}
