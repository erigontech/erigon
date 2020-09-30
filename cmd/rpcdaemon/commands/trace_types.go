package commands

import (
	"fmt"
	"strings"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
)

// TODO:(tjayrush)
// Implementation Notes:
// -- Many of these fields are of string type. I chose to do this for ease of debugging / clarity of code (less
//    conversions, etc.).Once we start optimizing this code, many of these fields will be made into their native
//    types (Addresses, uint64, etc.)
// -- The ordering of the fields in the Parity types should not be changed. This allows us to compare output
//    directly with existing Parity tests

// GethTrace The trace as received from the existing Geth javascript tracer 'callTracer'
type GethTrace struct {
	Type    string     `json:"type"`
	Error   string     `json:"error"`
	From    string     `json:"from"`
	To      string     `json:"to"`
	Value   string     `json:"value"`
	Gas     string     `json:"gas"`
	GasUsed string     `json:"gasUsed"`
	Input   string     `json:"input"`
	Output  string     `json:"output"`
	Time    string     `json:"time"`
	Calls   GethTraces `json:"calls"`
}

// GethTraces an array of GethTraces
type GethTraces []*GethTrace

// ParityTrace A trace in the desired format (Parity/OpenEtherum) See: https://openethereum.github.io/wiki/JSONRPC-trace-module
type ParityTrace struct {
	// Do not change the ordering of these fields -- allows for easier comparison with other clients
	Action              TraceAction `json:"action"`
	BlockHash           common.Hash `json:"blockHash"`
	BlockNumber         uint64      `json:"blockNumber"`
	Error               string      `json:"error,omitempty"`
	Result              TraceResult `json:"result"`
	Subtraces           int         `json:"subtraces"`
	TraceAddress        []int       `json:"traceAddress"`
	TransactionHash     common.Hash `json:"transactionHash"`
	TransactionPosition uint64      `json:"transactionPosition"`
	Type                string      `json:"type"`
}

// ParityTraces An array of parity traces
type ParityTraces []ParityTrace

// TraceAction A parity formatted trace action
type TraceAction struct {
	// Do not change the ordering of these fields -- allows for easier comparison with other clients
	Author         string `json:"author,omitempty"`
	RewardType     string `json:"rewardType,omitempty"`
	SelfDestructed string `json:"address,omitempty"`
	Balance        string `json:"balance,omitempty"`
	CallType       string `json:"callType,omitempty"`
	From           string `json:"from,omitempty"`
	Gas            string `json:"gas,omitempty"`
	Init           string `json:"init,omitempty"`
	Input          string `json:"input,omitempty"`
	RefundAddress  string `json:"refundAddress,omitempty"`
	To             string `json:"to,omitempty"`
	Value          string `json:"value,omitempty"`
}

// TraceResult A parity formatted trace result
type TraceResult struct {
	// Do not change the ordering of these fields -- allows for easier comparison with other clients
	Address string `json:"address,omitempty"`
	Code    string `json:"code,omitempty"`
	GasUsed string `json:"gasUsed,omitempty"`
	Output  string `json:"output,omitempty"`
}

// Allows for easy printing of a geth trace for debugging
func (p GethTrace) String() string {
	var ret string
	ret += fmt.Sprintf("Type: %s\n", p.Type)
	ret += fmt.Sprintf("From: %s\n", p.From)
	ret += fmt.Sprintf("To: %s\n", p.To)
	ret += fmt.Sprintf("Value: %s\n", p.Value)
	ret += fmt.Sprintf("Gas: %s\n", p.Gas)
	ret += fmt.Sprintf("GasUsed: %s\n", p.GasUsed)
	ret += fmt.Sprintf("Input: %s\n", p.Input)
	ret += fmt.Sprintf("Output: %s\n", p.Output)
	return ret
}

// Allows for easy printing of a parity trace for debugging
func (t ParityTrace) String() string {
	var ret string
	ret += fmt.Sprintf("Action.SelfDestructed: %s\n", t.Action.SelfDestructed)
	ret += fmt.Sprintf("Action.Balance: %s\n", t.Action.Balance)
	ret += fmt.Sprintf("Action.CallType: %s\n", t.Action.CallType)
	ret += fmt.Sprintf("Action.From: %s\n", t.Action.From)
	ret += fmt.Sprintf("Action.Gas: %s\n", t.Action.Gas)
	ret += fmt.Sprintf("Action.Init: %s\n", t.Action.Init)
	ret += fmt.Sprintf("Action.Input: %s\n", t.Action.Input)
	ret += fmt.Sprintf("Action.RefundAddress: %s\n", t.Action.RefundAddress)
	ret += fmt.Sprintf("Action.To: %s\n", t.Action.To)
	ret += fmt.Sprintf("Action.Value: %s\n", t.Action.Value)
	ret += fmt.Sprintf("BlockHash: %v\n", t.BlockHash)
	ret += fmt.Sprintf("BlockNumber: %d\n", t.BlockNumber)
	ret += fmt.Sprintf("Result.Address: %s\n", t.Result.Address)
	ret += fmt.Sprintf("Result.Code: %s\n", t.Result.Code)
	ret += fmt.Sprintf("Result.GasUsed: %s\n", t.Result.GasUsed)
	ret += fmt.Sprintf("Result.Output: %s\n", t.Result.Output)
	ret += fmt.Sprintf("Subtraces: %d\n", t.Subtraces)
	//ret += fmt.Sprintf("TraceAddress: %s\n", t.TraceAddress)
	ret += fmt.Sprintf("TransactionHash: %v\n", t.TransactionHash)
	ret += fmt.Sprintf("TransactionPosition: %d\n", t.TransactionPosition)
	ret += fmt.Sprintf("Type: %s\n", t.Type)
	return ret
}

// Takes a hierarchical Geth trace with fields of different meaning stored in the same named fields depending on 'type'. Parity traces
// are flattened depth first and each field is put in its proper place
func (api *TraceAPIImpl) convertToParityTrace(gethTrace GethTrace, blockHash common.Hash, blockNumber uint64, tx *types.Transaction, txIndex uint64, depth []int) ParityTraces {
	var traces ParityTraces // nolint prealloc
	var pt ParityTrace

	callType := strings.ToLower(gethTrace.Type)
	if callType == "create" {
		pt.Action.CallType = ""
		pt.Action.From = gethTrace.From
		pt.Action.Init = gethTrace.Input
		pt.Result.Address = gethTrace.To
		pt.Result.Output = ""
		pt.Action.Value = gethTrace.Value
		pt.Result.Code = gethTrace.Output
		pt.Action.Gas = gethTrace.Gas
		pt.Result.GasUsed = gethTrace.GasUsed

	} else if callType == "selfdestruct" {
		pt.Action.CallType = ""
		pt.Action.Input = gethTrace.Input
		pt.Result.Output = gethTrace.Output
		pt.Action.Balance = gethTrace.Value
		pt.Action.Gas = gethTrace.Gas
		pt.Result.GasUsed = gethTrace.GasUsed
		pt.Action.SelfDestructed = gethTrace.From
		pt.Action.RefundAddress = gethTrace.To

	} else {
		pt.Action.CallType = callType
		pt.Action.Input = gethTrace.Input
		pt.Action.From = gethTrace.From
		pt.Action.To = gethTrace.To
		pt.Result.Output = gethTrace.Output
		pt.Action.Value = gethTrace.Value
		pt.Result.Code = "" // gethTrace.XXX
		pt.Action.Gas = gethTrace.Gas
		pt.Result.GasUsed = gethTrace.GasUsed

		// TODO(tjayrush): This extreme hack will be removed. It is here because it's the only way I could
		// TODO(tjayrush): get test cases to pass. Search (gasUsedHack) for related code
		if pt.Action.Gas == "0xdeadbeef" {
			a, _ := hexutil.DecodeUint64(pt.Action.Gas)     // 0xdeadbeef
			b, _ := hexutil.DecodeUint64(pt.Result.GasUsed) // the tracer returns a value too big by 0xdeadbeef - trueValue
			c := a - b                                      // trueValue
			pt.Action.Gas = hexutil.EncodeUint64(c)
			pt.Result.GasUsed = hexutil.EncodeUint64(0)
		}
	}

	// This ugly code is here to convert Geth error messages to Parity error message. One day, when
	// we figure out what we want to do, it will be removed
	var (
		ErrInvalidJumpParity       = "Bad jump destination"
		ErrExecutionRevertedParity = "Reverted"
	)
	gethError := gethTrace.Error
	if gethError == vm.ErrInvalidJump.Error() {
		pt.Error = ErrInvalidJumpParity
	} else if gethError == vm.ErrExecutionReverted.Error() {
		pt.Error = ErrExecutionRevertedParity
	} else {
		pt.Error = gethTrace.Error
	}
	if pt.Error != "" {
		pt.Result.GasUsed = "0"
	}
	// This ugly code is here to convert Geth error messages to Parity error message. One day, when
	// we figure out what we want to do, it will be removed

	pt.BlockHash = blockHash
	pt.BlockNumber = blockNumber
	pt.Subtraces = len(gethTrace.Calls)
	pt.TraceAddress = depth
	pt.TransactionHash = tx.Hash()
	pt.TransactionPosition = txIndex
	pt.Type = callType
	if pt.Type == "delegatecall" || pt.Type == "staticcall" {
		pt.Type = "call"
	}

	traces = append(traces, pt)

	for i, item := range gethTrace.Calls {
		newDepth := append(depth, i)
		subTraces := api.convertToParityTrace(*item, blockHash, blockNumber, tx, txIndex, newDepth)
		traces = append(traces, subTraces...)
	}

	return traces
}
