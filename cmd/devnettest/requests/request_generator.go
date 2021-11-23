package requests

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/common"
	"net/http"
	"time"
)

var (
	erigonUrl = "http://localhost:8545"
)

type RequestGenerator struct {
	reqID  int
	client *http.Client
}

func initialiseRequestGenerator() *RequestGenerator {
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	reqGen := RequestGenerator{
		client: client,
	}
	reqGen.reqID++

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
