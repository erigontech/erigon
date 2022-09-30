package requests

import (
	"encoding/json"
	"fmt"
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
