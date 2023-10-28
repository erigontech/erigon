package requests

import (
	"encoding/json"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
)

type TraceCall struct {
	CommonResponse
	Result TraceCallResult `json:"result"`
}

type TraceCallResult struct {
	Output    hexutility.Bytes                         `json:"output"`
	Trace     []CallTrace                              `json:"trace"`
	StateDiff map[libcommon.Address]TraceCallStateDiff `json:"stateDiff"`
}

type CallTrace struct {
	Type         string          `json:"type"`
	Action       TraceCallAction `json:"action"`
	Result       *CallResult     `json:"result"`
	Subtraces    int             `json:"subtraces"`
	TraceAddress []int           `json:"traceAddress"`
	Error        string          `json:"error"`
}

// TraceCallAction is superset of all possible action types
type TraceCallAction struct {
	From          libcommon.Address `json:"from"`
	To            libcommon.Address `json:"to"`
	Address       libcommon.Address `json:"address"`
	RefundAddress libcommon.Address `json:"refundAddress"`
	Gas           hexutil.Big       `json:"gas"`
	Value         hexutil.Big       `json:"value"`
	Balance       hexutil.Big       `json:"balance"`
	Init          hexutility.Bytes  `json:"init"`
	Input         hexutility.Bytes  `json:"input"`
	CallType      string            `json:"callType"`
}

type CallResult struct {
	GasUsed hexutil.Big       `json:"gasUsed"`
	Output  hexutility.Bytes  `json:"output"`
	Address libcommon.Address `json:"address"`
	Code    hexutility.Bytes  `json:"code"`
}

type TraceCallStateDiff struct {
	Balance interface{}                                             `json:"balance"`
	Nonce   interface{}                                             `json:"nonce"`
	Code    interface{}                                             `json:"code"`
	Storage map[libcommon.Hash]map[string]TraceCallStateDiffStorage `json:"storage"`
}

type TraceCallStateDiffStorage struct {
	From libcommon.Hash `json:"from"`
	To   libcommon.Hash `json:"to"`
}

type TransactionTrace struct {
	Type                string          `json:"type"`
	Action              TraceCallAction `json:"action"`
	Result              *CallResult     `json:"result"`
	Error               string          `json:"error"`
	BlockHash           libcommon.Hash  `json:"blockHash"`
	BlockNumber         uint64          `json:"blockNumber"`
	TransactionHash     libcommon.Hash  `json:"transactionHash"`
	TransactionPosition uint64          `json:"transactionPosition"`
	Subtraces           int             `json:"subtraces"`
	TraceAddress        []int           `json:"traceAddress"`
}

type TraceOpt string

var TraceOpts = struct {
	VmTrace   TraceOpt
	Trace     TraceOpt
	StateDiff TraceOpt
}{
	VmTrace:   "vmTrace",
	Trace:     "trace",
	StateDiff: "stateDiff",
}

func (reqGen *requestGenerator) TraceCall(blockRef rpc.BlockReference, args ethapi.CallArgs, traceOpts ...TraceOpt) (*TraceCallResult, error) {
	var b TraceCall

	if args.Data == nil {
		args.Data = &hexutility.Bytes{}
	}

	argsVal, err := json.Marshal(args)

	if err != nil {
		return nil, err
	}

	if len(traceOpts) == 0 {
		traceOpts = []TraceOpt{TraceOpts.Trace, TraceOpts.StateDiff}
	}

	optsVal, err := json.Marshal(traceOpts)

	if err != nil {
		return nil, err
	}

	method, body := reqGen.traceCall(blockRef, string(argsVal), string(optsVal))
	res := reqGen.call(method, body, &b)

	if res.Err != nil {
		return nil, fmt.Errorf("TraceCall rpc failed: %w", res.Err)
	}

	if b.Error != nil {
		return nil, fmt.Errorf("TraceCall rpc failed: %w", b.Error)
	}

	return &b.Result, nil
}

func (req *requestGenerator) traceCall(blockRef rpc.BlockReference, callArgs string, traceOpts string) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":[%s,%s,"%s"],"id":%d}`
	return Methods.TraceCall, fmt.Sprintf(template, Methods.TraceCall, callArgs, traceOpts, blockRef.String(), req.reqID)
}

func (reqGen *requestGenerator) TraceTransaction(hash libcommon.Hash) ([]TransactionTrace, error) {
	var result []TransactionTrace

	if err := reqGen.callCli(&result, Methods.TraceTransaction, hash); err != nil {
		return nil, err
	}

	return result, nil
}
