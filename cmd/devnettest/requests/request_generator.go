package requests

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
)

const erigonUrl string = "http://localhost:8545"

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

func (req *RequestGenerator) Get() rpctest.CallResult {
	start := time.Now()
	res := rpctest.CallResult{
		RequestID: req.reqID,
	}

	resp, err := http.Get(erigonUrl) //nolint
	if err != nil {
		res.Took = time.Since(start)
		res.Err = err
		return res
	}
	defer func(Body io.ReadCloser) {
		closeErr := Body.Close()
		if closeErr != nil {
			log.Warn("Failed to close readCloser", "err", closeErr)
		}
	}(resp.Body)

	if resp.StatusCode != 200 {
		res.Took = time.Since(start)
		res.Err = errors.New("bad request")
		return res
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		res.Took = time.Since(start)
		res.Err = err
		return res
	}

	res.Response = body
	res.Took = time.Since(start)
	res.Err = err
	return res
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

func (req *RequestGenerator) parityStorageKeyListContent(address common.Address, quantity int, offset []byte, blockNum string) string {
	const template = `{"jsonrpc":"2.0","method":"parity_listStorageKeys","params":["0x%x", %d, %v, "%s"],"id":%d}`
	var offsetString string
	if len(offset) != 0 {
		offsetString = fmt.Sprintf(`"0x%x"`, offset)
	} else {
		offsetString = "null"
	}

	return fmt.Sprintf(template, address, quantity, offsetString, blockNum, req.reqID)
}

func (req *RequestGenerator) getLogs(fromBlock, toBlock uint64, address common.Address) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x%x", "toBlock": "0x%x", "address": "0x%x"}],"id":%d}`
	return fmt.Sprintf(template, fromBlock, toBlock, address, req.reqID)
}

func (req *RequestGenerator) getTransactionCount(address common.Address, blockNum string) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getTransactionCount","params":["0x%x","%v"],"id":%d}`
	return fmt.Sprintf(template, address, blockNum, req.reqID)
}
