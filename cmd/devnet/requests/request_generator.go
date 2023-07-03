package requests

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/log/v3"
	"github.com/valyala/fastjson"
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

type CommonResponse struct {
	Version   string    `json:"jsonrpc"`
	RequestId int       `json:"id"`
	Error     *EthError `json:"error"`
}

type EthError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type RequestGenerator interface {
	PingErigonRpc() CallResult
	GetBalance(address libcommon.Address, blockNum BlockNumber) (uint64, error)
	AdminNodeInfo() (p2p.NodeInfo, error)
	GetBlockByNumberDetails(blockNum string, withTxs bool) (map[string]interface{}, error)
	GetBlockByNumber(blockNum uint64, withTxs bool) (EthBlockByNumber, error)
	BlockNumber() (uint64, error)
	GetTransactionCount(address libcommon.Address, blockNum BlockNumber) (EthGetTransactionCount, error)
	SendTransaction(signedTx types.Transaction) (*libcommon.Hash, error)
	GetAndCompareLogs(fromBlock uint64, toBlock uint64, expected Log) error
	TxpoolContent() (int, int, int, error)
}

type requestGenerator struct {
	reqID  int
	client *http.Client
	logger log.Logger
	target string
}

type (
	// RPCMethod is the type for rpc methods used
	RPCMethod string
	// SubMethod is the type for sub methods used in subscriptions
	SubMethod string
	// BlockNumber represents the block number type
	BlockNumber string
)

var BlockNumbers = struct {
	// Latest is the parameter for the latest block
	Latest BlockNumber
	// Earliest is the parameter for the earliest block
	Earliest BlockNumber
	// Pending is the parameter for the pending block
	Pending BlockNumber
}{
	Latest:   "latest",
	Earliest: "earliest",
	Pending:  "pending",
}

var Methods = struct {
	// ETHGetTransactionCount represents the eth_getTransactionCount method
	ETHGetTransactionCount RPCMethod
	// ETHGetBalance represents the eth_getBalance method
	ETHGetBalance RPCMethod
	// ETHSendRawTransaction represents the eth_sendRawTransaction method
	ETHSendRawTransaction RPCMethod
	// ETHGetBlockByNumber represents the eth_getBlockByNumber method
	ETHGetBlockByNumber RPCMethod
	// ETHGetBlock represents the eth_getBlock method
	ETHGetBlock RPCMethod
	// ETHGetLogs represents the eth_getLogs method
	ETHGetLogs RPCMethod
	// ETHBlockNumber represents the eth_blockNumber method
	ETHBlockNumber RPCMethod
	// AdminNodeInfo represents the admin_nodeInfo method
	AdminNodeInfo RPCMethod
	// TxpoolContent represents the txpool_content method
	TxpoolContent RPCMethod
	// OTSGetBlockDetails represents the ots_getBlockDetails method
	OTSGetBlockDetails RPCMethod
	// ETHNewHeads represents the eth_newHeads sub method
	ETHNewHeads SubMethod
}{
	ETHGetTransactionCount: "eth_getTransactionCount",
	ETHGetBalance:          "eth_getBalance",
	ETHSendRawTransaction:  "eth_sendRawTransaction",
	ETHGetBlockByNumber:    "eth_getBlockByNumber",
	ETHGetBlock:            "eth_getBlock",
	ETHGetLogs:             "eth_getLogs",
	ETHBlockNumber:         "eth_blockNumber",
	AdminNodeInfo:          "admin_nodeInfo",
	TxpoolContent:          "txpool_content",
	OTSGetBlockDetails:     "ots_getBlockDetails",
	ETHNewHeads:            "eth_newHeads",
}

func (req *requestGenerator) call(method RPCMethod, body string, response interface{}) CallResult {
	start := time.Now()
	err := post(req.client, req.target, string(method), body, response, req.logger)
	req.reqID++
	return CallResult{
		RequestBody: body,
		Target:      req.target,
		Took:        time.Since(start),
		RequestID:   req.reqID,
		Method:      string(method),
		Err:         err,
	}
}

func (req *requestGenerator) PingErigonRpc() CallResult {
	start := time.Now()
	res := CallResult{
		RequestID: req.reqID,
	}

	// return early if the http module has issue fetching the url
	resp, err := http.Get(req.target) //nolint
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
		res.Err = ErrBadRequest
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

func NewRequestGenerator(target string, logger log.Logger) RequestGenerator {
	return &requestGenerator{
		client: &http.Client{
			Timeout: time.Second * 10,
		},
		reqID:  1,
		logger: logger,
		target: target,
	}
}

func post(client *http.Client, url, method, request string, response interface{}, logger log.Logger) error {
	start := time.Now()
	r, err := client.Post(url, "application/json", strings.NewReader(request)) // nolint:bodyclose
	if err != nil {
		return fmt.Errorf("client failed to make post request: %w", err)
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
		return fmt.Errorf("failed to readAll from body: %w", err)
	}

	err = json.Unmarshal(b, &response)
	if err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if len(method) > 0 {
		method = "#" + method
	}

	logger.Info(fmt.Sprintf("%s%s", url, method), "time", time.Since(start).Seconds())

	return nil
}
