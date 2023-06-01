package requests

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"
	"github.com/valyala/fastjson"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
)

type CallResult struct {
	Target      string
	Took        time.Duration
	RequestID   int
	Method      string
	RequestBody string
	Response    []byte
	Result      *fastjson.Value
	Err         error
}

func post(client *http.Client, url, request string, response interface{}, logger log.Logger) error {
	start := time.Now()
	r, err := client.Post(url, "application/json", strings.NewReader(request)) // nolint:bodyclose
	if err != nil {
		return fmt.Errorf("client failed to make post request: %v", err)
	}
	defer func(Body io.ReadCloser) {
		closeErr := Body.Close()
		if closeErr != nil {
			logger.Warn("body close", "err", closeErr)
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

	logger.Info("Got in", "time", time.Since(start).Seconds())
	return nil
}

type RequestGenerator struct {
	reqID  int
	client *http.Client
	logger log.Logger
}

func (req *RequestGenerator) call(target string, method, body string, response interface{}) CallResult {
	start := time.Now()
	err := post(req.client, models.ErigonUrl, body, response, req.logger)
	r := CallResult{
		RequestBody: body,
		Target:      target,
		Took:        time.Since(start),
		RequestID:   req.reqID,
		Method:      method,
		Err:         err,
	}
	req.reqID++
	return r
}

func (req *RequestGenerator) Erigon(method models.RPCMethod, body string, response interface{}) CallResult {
	return req.call(models.ErigonUrl, string(method), body, response)
}

func (req *RequestGenerator) BlockNumber() string {
	const template = `{"jsonrpc":"2.0","method":%q,"id":%d}`
	return fmt.Sprintf(template, models.ETHBlockNumber, req.reqID)
}

func (req *RequestGenerator) GetAdminNodeInfo() string {
	const template = `{"jsonrpc":"2.0","method":%q,"id":%d}`
	return fmt.Sprintf(template, models.AdminNodeInfo, req.reqID)
}

func (req *RequestGenerator) GetBalance(address libcommon.Address, blockNum models.BlockNumber) string {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["0x%x","%v"],"id":%d}`
	return fmt.Sprintf(template, models.ETHGetBalance, address, blockNum, req.reqID)
}

func (req *RequestGenerator) GetBlockByNumber(blockNum uint64, withTxs bool) string {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["0x%x",%t],"id":%d}`
	return fmt.Sprintf(template, models.ETHGetBlockByNumber, blockNum, withTxs, req.reqID)
}

func (req *RequestGenerator) GetBlockByNumberI(blockNum string, withTxs bool) string {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["%s",%t],"id":%d}`
	return fmt.Sprintf(template, models.ETHGetBlockByNumber, blockNum, withTxs, req.reqID)
}

func (req *RequestGenerator) GetLogs(fromBlock, toBlock uint64, address libcommon.Address) string {
	const template = `{"jsonrpc":"2.0","method":%q,"params":[{"fromBlock":"0x%x","toBlock":"0x%x","address":"0x%x"}],"id":%d}`
	return fmt.Sprintf(template, models.ETHGetLogs, fromBlock, toBlock, address, req.reqID)
}

func (req *RequestGenerator) GetTransactionCount(address libcommon.Address, blockNum models.BlockNumber) string {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["0x%x","%v"],"id":%d}`
	return fmt.Sprintf(template, models.ETHGetTransactionCount, address, blockNum, req.reqID)
}

func (req *RequestGenerator) SendRawTransaction(signedTx []byte) string {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["0x%x"],"id":%d}`
	return fmt.Sprintf(template, models.ETHSendRawTransaction, signedTx, req.reqID)
}

func (req *RequestGenerator) TxpoolContent() string {
	const template = `{"jsonrpc":"2.0","method":%q,"params":[],"id":%d}`
	return fmt.Sprintf(template, models.TxpoolContent, req.reqID)
}

func (req *RequestGenerator) GetBlockDetails(blockNum string) string {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["%s"],"id":%d}`
	return fmt.Sprintf(template, models.OTSGetBlockDetails, blockNum, req.reqID)
}

func (req *RequestGenerator) PingErigonRpc() CallResult {
	start := time.Now()
	res := CallResult{
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
			req.logger.Warn("failed to close readCloser", "err", closeErr)
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

func NewRequestGenerator(logger log.Logger) *RequestGenerator {
	var client = &http.Client{
		Timeout: time.Second * 600,
	}
	reqGen := RequestGenerator{
		client: client,
		reqID:  1,
		logger: logger,
	}
	return &reqGen
}
