package requests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	ethereum "github.com/ledgerwatch/erigon"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc"
	"github.com/ledgerwatch/log/v3"
	"github.com/valyala/fastjson"
)

type callResult struct {
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

func (e EthError) Error() string {
	return fmt.Sprintf("%d: %s", e.Code, e.Message)
}

type RequestGenerator interface {
	PingErigonRpc() PingResult
	GetBalance(address libcommon.Address, blockRef rpc.BlockReference) (*big.Int, error)
	AdminNodeInfo() (p2p.NodeInfo, error)
	GetBlockByNumber(blockNum rpc.BlockNumber, withTxs bool) (*Block, error)
	GetTransactionByHash(hash libcommon.Hash) (*jsonrpc.RPCTransaction, error)
	GetTransactionReceipt(hash libcommon.Hash) (*types.Receipt, error)
	TraceTransaction(hash libcommon.Hash) ([]TransactionTrace, error)
	GetTransactionCount(address libcommon.Address, blockRef rpc.BlockReference) (*big.Int, error)
	BlockNumber() (uint64, error)
	SendTransaction(signedTx types.Transaction) (libcommon.Hash, error)
	FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error)
	SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error)
	Subscribe(ctx context.Context, method SubMethod, subChan interface{}, args ...interface{}) (ethereum.Subscription, error)
	TxpoolContent() (int, int, int, error)
	Call(args ethapi.CallArgs, blockRef rpc.BlockReference, overrides *ethapi.StateOverrides) ([]byte, error)
	TraceCall(blockRef rpc.BlockReference, args ethapi.CallArgs, traceOpts ...TraceOpt) (*TraceCallResult, error)
	DebugAccountAt(blockHash libcommon.Hash, txIndex uint64, account libcommon.Address) (*AccountResult, error)
	GetCode(address libcommon.Address, blockRef rpc.BlockReference) (hexutility.Bytes, error)
	EstimateGas(args ethereum.CallMsg, blockNum BlockNumber) (uint64, error)
	GasPrice() (*big.Int, error)

	GetRootHash(startBlock uint64, endBlock uint64) (libcommon.Hash, error)
}

type requestGenerator struct {
	sync.Mutex
	reqID              int
	client             *http.Client
	subscriptionClient *rpc.Client
	requestClient      *rpc.Client
	logger             log.Logger
	target             string
}

type (
	// RPCMethod is the type for rpc methods used
	RPCMethod string
	// SubMethod is the type for sub methods used in subscriptions
	SubMethod string
)

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
	ETHNewHeads              SubMethod
	ETHLogs                  SubMethod
	TraceCall                RPCMethod
	TraceTransaction         RPCMethod
	DebugAccountAt           RPCMethod
	ETHGetCode               RPCMethod
	ETHEstimateGas           RPCMethod
	ETHGasPrice              RPCMethod
	ETHGetTransactionByHash  RPCMethod
	ETHGetTransactionReceipt RPCMethod
	BorGetRootHash           RPCMethod
	ETHCall                  RPCMethod
}{
	ETHGetTransactionCount:   "eth_getTransactionCount",
	ETHGetBalance:            "eth_getBalance",
	ETHSendRawTransaction:    "eth_sendRawTransaction",
	ETHGetBlockByNumber:      "eth_getBlockByNumber",
	ETHGetBlock:              "eth_getBlock",
	ETHGetLogs:               "eth_getLogs",
	ETHBlockNumber:           "eth_blockNumber",
	AdminNodeInfo:            "admin_nodeInfo",
	TxpoolContent:            "txpool_content",
	OTSGetBlockDetails:       "ots_getBlockDetails",
	ETHNewHeads:              "eth_newHeads",
	ETHLogs:                  "eth_logs",
	TraceCall:                "trace_call",
	TraceTransaction:         "trace_transaction",
	DebugAccountAt:           "debug_accountAt",
	ETHGetCode:               "eth_getCode",
	ETHEstimateGas:           "eth_estimateGas",
	ETHGasPrice:              "eth_gasPrice",
	ETHGetTransactionByHash:  "eth_getTransactionByHash",
	ETHGetTransactionReceipt: "eth_getTransactionReceipt",
	BorGetRootHash:           "bor_getRootHash",
	ETHCall:                  "eth_call",
}

func (req *requestGenerator) call(method RPCMethod, body string, response interface{}) callResult {
	start := time.Now()
	targetUrl := "http://" + req.target
	err := post(req.client, targetUrl, string(method), body, response, req.logger)
	req.reqID++

	return callResult{
		RequestBody: body,
		Target:      targetUrl,
		Took:        time.Since(start),
		RequestID:   req.reqID,
		Method:      string(method),
		Err:         err,
	}
}

func (req *requestGenerator) callCli(result interface{}, method RPCMethod, args ...interface{}) error {
	cli, err := req.cli(context.Background())

	if err != nil {
		return err
	}

	return cli.Call(result, string(method), args...)
}

type PingResult callResult

func (req *requestGenerator) PingErigonRpc() PingResult {
	start := time.Now()
	res := callResult{
		RequestID: req.reqID,
	}

	// return early if the http module has issue fetching the url
	resp, err := http.Get("http://" + req.target) //nolint
	if err != nil {
		res.Took = time.Since(start)
		res.Err = err
		return PingResult(res)
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
		return PingResult(res)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		res.Took = time.Since(start)
		res.Err = err
		return PingResult(res)
	}

	res.Response = body
	res.Took = time.Since(start)
	res.Err = err
	return PingResult(res)
}

func NewRequestGenerator(target string, logger log.Logger) RequestGenerator {
	// TODO
	//rpc.DialHTTPWithClient(target, &http.Client{
	//		Timeout: time.Second * 10,
	//	}, logger)

	return &requestGenerator{
		client: &http.Client{
			Timeout: time.Second * 10,
		},
		reqID:  1,
		logger: logger,
		target: target,
	}
}

func (req *requestGenerator) cli(ctx context.Context) (*rpc.Client, error) {
	if req.requestClient == nil {
		var err error
		req.requestClient, err = rpc.DialContext(ctx, "http://"+req.target, req.logger)

		if err != nil {
			return nil, err
		}
	}

	return req.requestClient, nil
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

// subscribe connects to a websocket client and returns the subscription handler and a channel buffer
func (req *requestGenerator) Subscribe(ctx context.Context, method SubMethod, subChan interface{}, args ...interface{}) (ethereum.Subscription, error) {
	var err error

	if req.subscriptionClient == nil {
		req.subscriptionClient, err = rpc.DialWebsocket(ctx, "ws://"+req.target, "", req.logger)

		if err != nil {
			return nil, fmt.Errorf("failed to dial websocket: %v", err)
		}
	}

	namespace, subMethod, err := devnetutils.NamespaceAndSubMethodFromMethod(string(method))

	if err != nil {
		return nil, fmt.Errorf("cannot get namespace and submethod from method: %v", err)
	}

	args = append([]interface{}{subMethod}, args...)

	return req.subscriptionClient.Subscribe(ctx, namespace, subChan, args...)
}

// UnsubscribeAll closes all the client subscriptions and empties their global subscription channel
func (req *requestGenerator) UnsubscribeAll() {
	if req.subscriptionClient == nil {
		return
	}
	subscriptionClient := req.subscriptionClient
	req.subscriptionClient = nil
	subscriptionClient.Close()
}
