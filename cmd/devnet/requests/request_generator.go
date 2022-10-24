package requests

import (
	"encoding/json"
	"fmt"
	"github.com/ledgerwatch/erigon/common"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/log/v3"
)

func post(client *http.Client, url, request string, response interface{}) error {
	start := time.Now()
	r, err := client.Post(url, "application/json", strings.NewReader(request)) // nolint:bodyclose
	if err != nil {
		return fmt.Errorf("client failed to make post request: %v", err)
	}
	defer func(Body io.ReadCloser) {
		closeErr := Body.Close()
		if closeErr != nil {
			log.Warn("body close", "err", closeErr)
		}
	}(r.Body)

	if r.StatusCode != 200 {
		return fmt.Errorf("status %s", r.Status)
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("failed to readAll from body: %s", err)
	}

	err = json.Unmarshal(b, &response)
	if err != nil {
		return fmt.Errorf("failed to unmarshal response: %s", err)
	}

	log.Info("Got in", "time", time.Since(start).Seconds())
	return nil
}

type RequestGenerator struct {
	reqID  int
	client *http.Client
}

func (req *RequestGenerator) call(target string, method, body string, response interface{}) rpctest.CallResult {
	start := time.Now()
	err := post(req.client, models.ErigonUrl, body, response)
	return rpctest.CallResult{
		RequestBody: body,
		Target:      target,
		Took:        time.Since(start),
		RequestID:   req.reqID,
		Method:      method,
		Err:         err,
	}
}

func (req *RequestGenerator) Erigon(method, body string, response interface{}) rpctest.CallResult {
	return req.call(models.ErigonUrl, method, body, response)
}

func (req *RequestGenerator) getAdminNodeInfo() string {
	const template = `{"jsonrpc":"2.0","method":"admin_nodeInfo","id":%d}`
	return fmt.Sprintf(template, req.reqID)
}

func (req *RequestGenerator) getBalance(address common.Address, blockNum string) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x%x","%v"],"id":%d}`
	return fmt.Sprintf(template, address, blockNum, req.reqID)
}

func (req *RequestGenerator) txpoolContent() string {
	const template = `{"jsonrpc":"2.0","method":"txpool_content","params":[],"id":%d}`
	return fmt.Sprintf(template, req.reqID)
}

func (req *RequestGenerator) PingErigonRpc() rpctest.CallResult {
	start := time.Now()
	res := rpctest.CallResult{
		RequestID: req.reqID,
	}

	// return early if the http module has issue fetching the url
	resp, err := http.Get(models.ErigonUrl) //nolint
	if err != nil {
		res.Took = time.Since(start)
		res.Err = err
		return res
	}

	// close the response body after reading its content at the end of the function
	defer func(body io.ReadCloser) {
		closeErr := body.Close()
		if closeErr != nil {
			log.Warn("failed to close readCloser", "err", closeErr)
		}
	}(resp.Body)

	// return a bad request if the status code is not 200
	if resp.StatusCode != 200 {
		res.Took = time.Since(start)
		res.Err = models.ErrBadRequest
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
