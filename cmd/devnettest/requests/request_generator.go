package requests

import (
	"fmt"
	"net/http"
	"time"

	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/common"
)

var (
	erigonUrl = "http://localhost:8545"
)

type RequestGenerator struct {
	reqID  int
	client *http.Client
}

func initialiseRequestGenerator(reqId int) *RequestGenerator {
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	reqGen := RequestGenerator{
		client: client,
		reqID:  reqId,
	}
	if reqGen.reqID == 0 {
		reqGen.reqID++
	}

	return &reqGen
}

func (req *RequestGenerator) Erigon(method, body string, response interface{}) rpctest.CallResult {
	return req.call(erigonUrl, method, body, response)
}

func (req *RequestGenerator) call(target string, method, body string, response interface{}) rpctest.CallResult {
	start := time.Now()
	err := post(req.client, erigonUrl, body, response)
	return rpctest.CallResult{
		RequestBody: body,
		Target:      target,
		Took:        time.Since(start),
		RequestID:   req.reqID,
		Method:      method,
		Err:         err,
	}
}

func (req *RequestGenerator) getBalance(address common.Address, blockNum string) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x%x","%v"],"id":%d}`
	return fmt.Sprintf(template, address, blockNum, req.reqID)
}

func (req *RequestGenerator) sendRawTransaction(signedTx []byte) string {
	const template = `{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x%x"],"id":%d}`
	return fmt.Sprintf(template, signedTx, req.reqID)
}

func (req *RequestGenerator) txpoolContent() string {
	const template = `{"jsonrpc":"2.0","method":"txpool_content","params":[],"id":%d}`
	return fmt.Sprintf(template, req.reqID)
}

func (req *RequestGenerator) parityStorageKeyListContent(address common.Address, quantity int, offset []byte) string {
	const template = `{"jsonrpc":"2.0","method":"parity_listStorageKeys","params":["0x%x", %d, %v],"id":%d}`
	var offsetString string
	if len(offset) != 0 {
		offsetString = fmt.Sprintf(`"0x%x"`, offset)
	} else {
		offsetString = "null"
	}
	return fmt.Sprintf(template, address, quantity, offsetString, req.reqID)
}
