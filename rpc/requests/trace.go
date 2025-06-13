// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package requests

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/fastjson"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

type TraceCall struct {
	CommonResponse
	Result TraceCallResult `json:"result"`
}

type TraceCallResult struct {
	Output    hexutil.Bytes                         `json:"output"`
	Trace     []CallTrace                           `json:"trace"`
	StateDiff map[common.Address]TraceCallStateDiff `json:"stateDiff"`
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
	From          common.Address `json:"from"`
	To            common.Address `json:"to"`
	Address       common.Address `json:"address"`
	RefundAddress common.Address `json:"refundAddress"`
	Gas           hexutil.Big    `json:"gas"`
	Value         hexutil.Big    `json:"value"`
	Balance       hexutil.Big    `json:"balance"`
	Init          hexutil.Bytes  `json:"init"`
	Input         hexutil.Bytes  `json:"input"`
	CallType      string         `json:"callType"`
}

type CallResult struct {
	GasUsed hexutil.Big    `json:"gasUsed"`
	Output  hexutil.Bytes  `json:"output"`
	Address common.Address `json:"address"`
	Code    hexutil.Bytes  `json:"code"`
}

type TraceCallStateDiff struct {
	Balance interface{}                                          `json:"balance"`
	Nonce   interface{}                                          `json:"nonce"`
	Code    interface{}                                          `json:"code"`
	Storage map[common.Hash]map[string]TraceCallStateDiffStorage `json:"storage"`
}

type TraceCallStateDiffStorage struct {
	From common.Hash `json:"from"`
	To   common.Hash `json:"to"`
}

type TransactionTrace struct {
	Type                string          `json:"type"`
	Action              TraceCallAction `json:"action"`
	Result              *CallResult     `json:"result"`
	Error               string          `json:"error"`
	BlockHash           common.Hash     `json:"blockHash"`
	BlockNumber         uint64          `json:"blockNumber"`
	TransactionHash     common.Hash     `json:"transactionHash"`
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
		args.Data = &hexutil.Bytes{}
	}

	argsVal, err := fastjson.Marshal(args)

	if err != nil {
		return nil, err
	}

	if len(traceOpts) == 0 {
		traceOpts = []TraceOpt{TraceOpts.Trace, TraceOpts.StateDiff}
	}

	optsVal, err := fastjson.Marshal(traceOpts)

	if err != nil {
		return nil, err
	}

	method, body := reqGen.traceCall(blockRef, string(argsVal), string(optsVal))
	res := reqGen.rpcCallJSON(method, body, &b)

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

func (reqGen *requestGenerator) TraceTransaction(hash common.Hash) ([]TransactionTrace, error) {
	var result []TransactionTrace

	if err := reqGen.rpcCall(context.Background(), &result, Methods.TraceTransaction, hash); err != nil {
		return nil, err
	}

	return result, nil
}
