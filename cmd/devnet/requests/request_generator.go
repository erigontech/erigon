package requests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
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
	GetBlockByNumber(ctx context.Context, blockNum rpc.BlockNumber, withTxs bool) (*Block, error)
	GetTransactionByHash(hash libcommon.Hash) (*jsonrpc.RPCTransaction, error)
	GetTransactionReceipt(ctx context.Context, hash libcommon.Hash) (*types.Receipt, error)
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

	GetRootHash(ctx context.Context, startBlock uint64, endBlock uint64) (libcommon.Hash, error)
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

func (req *requestGenerator) rpcCallJSON(method RPCMethod, body string, response interface{}) callResult {
	ctx := context.Background()
	req.reqID++
	start := time.Now()
	targetUrl := "http://" + req.target

	err := retryConnects(ctx, func(ctx context.Context) error {
		return post(ctx, req.client, targetUrl, string(method), body, response, req.logger)
	})

	return callResult{
		RequestBody: body,
		Target:      targetUrl,
		Took:        time.Since(start),
		RequestID:   req.reqID,
		Method:      string(method),
		Err:         err,
	}
}

func (req *requestGenerator) rpcCall(ctx context.Context, result interface{}, method RPCMethod, args ...interface{}) error {
	client, err := req.rpcClient(ctx)
	if err != nil {
		return err
	}

	return retryConnects(ctx, func(ctx context.Context) error {
		return client.CallContext(ctx, result, string(method), args...)
	})
}

const requestTimeout = time.Second * 20
const connectionTimeout = time.Millisecond * 500

func isConnectionError(err error) bool {
	var opErr *net.OpError
	switch {
	case errors.As(err, &opErr):
		return opErr.Op == "dial"

	case errors.Is(err, context.DeadlineExceeded):
		return true
	}

	return false
}

func retryConnects(ctx context.Context, op func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return retry(ctx, op, isConnectionError, time.Second*1, nil)
}

func retry(ctx context.Context, op func(context.Context) error, isRecoverableError func(error) bool, delay time.Duration, lastErr error) error {
	opctx, cancel := context.WithTimeout(ctx, connectionTimeout)
	defer cancel()

	err := op(opctx)

	if err == nil {
		return nil
	}

	if !isRecoverableError(err) {
		return err
	}

	if errors.Is(err, context.DeadlineExceeded) {
		if lastErr != nil {
			return lastErr
		}

		err = nil
	}

	delayTimer := time.NewTimer(delay)
	select {
	case <-delayTimer.C:
		return retry(ctx, op, isRecoverableError, delay, err)
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return err
		}
		return ctx.Err()
	}
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
		logger: logger,
		target: target,
	}
}

func (req *requestGenerator) rpcClient(ctx context.Context) (*rpc.Client, error) {
	if req.requestClient == nil {
		var err error
		req.requestClient, err = rpc.DialContext(ctx, "http://"+req.target, req.logger)
		if err != nil {
			return nil, err
		}
	}

	return req.requestClient, nil
}

func post(ctx context.Context, client *http.Client, url, method, request string, response interface{}, logger log.Logger) error {
	start := time.Now()

	req, err := http.NewRequest("POST", url, strings.NewReader(request))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(ctx)

	r, err := client.Do(req) // nolint:bodyclose
	if err != nil {
		return fmt.Errorf("client failed to make post request: %w", err)
	}

	defer func(body io.ReadCloser) {
		closeErr := body.Close()
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
	if req.subscriptionClient == nil {
		err := retryConnects(ctx, func(ctx context.Context) error {
			var err error
			req.subscriptionClient, err = rpc.DialWebsocket(ctx, "ws://"+req.target, "", req.logger)
			return err
		})
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
