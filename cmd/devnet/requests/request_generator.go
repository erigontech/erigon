package requests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	ethereum "github.com/ledgerwatch/erigon"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/rpc"
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
	GetBalance(address libcommon.Address, blockNum BlockNumber) (*big.Int, error)
	AdminNodeInfo() (p2p.NodeInfo, error)
	GetBlockByNumberDetails(blockNum string, withTxs bool) (map[string]interface{}, error)
	GetBlockByNumber(blockNum uint64, withTxs bool) (EthBlockByNumber, error)
	BlockNumber() (uint64, error)
	GetTransactionCount(address libcommon.Address, blockNum BlockNumber) (*big.Int, error)
	SendTransaction(signedTx types.Transaction) (*libcommon.Hash, error)
	FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error)
	SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error)
	TxpoolContent() (int, int, int, error)
}

type SubscriptionHandler func(interface{})

// Subscription houses the client subscription, name and channel for its delivery
type Subscription struct {
	Name      SubMethod
	Handler   SubscriptionHandler
	clientSub *rpc.ClientSubscription
	reqGen    *requestGenerator
	subChan   chan interface{}
}

func (s *Subscription) Unsubscribe() {
	s.clientSub.Unsubscribe()
	close(s.subChan)
	s.subChan = nil
}

func (s *Subscription) Err() <-chan error {
	return s.clientSub.Err()
}

type requestGenerator struct {
	reqID              int
	client             *http.Client
	subscriptionClient *rpc.Client
	logger             log.Logger
	target             string
	subscriptions      map[SubMethod]*Subscription
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
	ETHLogs     SubMethod
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
	ETHLogs:                "eth_logs",
}

func (req *requestGenerator) call(method RPCMethod, body string, response interface{}) CallResult {
	start := time.Now()
	targetUrl := "http://" + req.target
	err := post(req.client, targetUrl, string(method), body, response, req.logger)
	req.reqID++

	return CallResult{
		RequestBody: body,
		Target:      targetUrl,
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
	resp, err := http.Get("http://" + req.target) //nolint
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

// NewSubscription returns a new Subscription instance
func newSubscription(req *requestGenerator, name SubMethod, handler SubscriptionHandler) *Subscription {
	sub := &Subscription{
		Name:    name,
		Handler: handler,
		reqGen:  req,
		subChan: make(chan interface{}),
	}

	go func() {
		for value := range sub.subChan {
			func() {
				defer func() {
					if r := recover(); r != nil {
						sub.reqGen.logger.Error("Subscription Handler Crashed", "err", r)
					}
				}()

				sub.Handler(value)
			}()
		}
	}()

	return sub
}

// subscribe connects to a websocket client and returns the subscription handler and a channel buffer
func (req *requestGenerator) subscribe(method SubMethod, handler SubscriptionHandler, args ...interface{}) (*Subscription, error) {
	var err error

	if req.subscriptionClient == nil {
		req.subscriptionClient, err = rpc.DialWebsocket(context.Background(), "ws://"+req.target, "", req.logger)

		if err != nil {
			return nil, fmt.Errorf("failed to dial websocket: %v", err)
		}
	}

	methodSub := newSubscription(req, method, handler)

	namespace, subMethod, err := devnetutils.NamespaceAndSubMethodFromMethod(string(method))

	if err != nil {
		return nil, fmt.Errorf("cannot get namespace and submethod from method: %v", err)
	}

	arr := append([]interface{}{subMethod}, args...)

	sub, err := req.subscriptionClient.Subscribe(context.Background(), namespace, methodSub.subChan, arr...)

	if err != nil {
		return nil, fmt.Errorf("client failed to subscribe: %v", err)
	}

	methodSub.clientSub = sub

	if req.subscriptions == nil {
		req.subscriptions = map[SubMethod]*Subscription{}
	}

	req.subscriptions[method] = methodSub

	return methodSub, nil
}

// UnsubscribeAll closes all the client subscriptions and empties their global subscription channel
func (req *requestGenerator) UnsubscribeAll() {
	if req.subscriptions == nil {
		return
	}
	for _, methodSub := range req.subscriptions {
		if methodSub != nil {
			methodSub.Unsubscribe()
		}
	}
}
