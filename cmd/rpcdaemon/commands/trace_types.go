package commands

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
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
	Action              interface{}  `json:"action"` // Can be either CallTraceAction or CreateTraceAction
	BlockHash           *common.Hash `json:"blockHash,omitempty"`
	BlockNumber         *uint64      `json:"blockNumber,omitempty"`
	Error               string       `json:"error,omitempty"`
	Result              interface{}  `json:"result"`
	Subtraces           int          `json:"subtraces"`
	TraceAddress        []int        `json:"traceAddress"`
	TransactionHash     *common.Hash `json:"transactionHash,omitempty"`
	TransactionPosition *uint64      `json:"transactionPosition,omitempty"`
	Type                string       `json:"type"`
}

// ParityTraces An array of parity traces
type ParityTraces []ParityTrace

// TraceAction A parity formatted trace action
type TraceAction struct {
	// Do not change the ordering of these fields -- allows for easier comparison with other clients
	Author         string         `json:"author,omitempty"`
	RewardType     string         `json:"rewardType,omitempty"`
	SelfDestructed string         `json:"address,omitempty"`
	Balance        string         `json:"balance,omitempty"`
	CallType       string         `json:"callType,omitempty"`
	From           common.Address `json:"from"`
	Gas            hexutil.Big    `json:"gas"`
	Init           hexutil.Bytes  `json:"init,omitempty"`
	Input          hexutil.Bytes  `json:"input,omitempty"`
	RefundAddress  string         `json:"refundAddress,omitempty"`
	To             string         `json:"to,omitempty"`
	Value          string         `json:"value,omitempty"`
}

type CallTraceAction struct {
	From     common.Address `json:"from"`
	CallType string         `json:"callType"`
	Gas      hexutil.Big    `json:"gas"`
	Input    hexutil.Bytes  `json:"input"`
	To       common.Address `json:"to"`
	Value    hexutil.Big    `json:"value"`
}

type CreateTraceAction struct {
	From  common.Address `json:"from"`
	Gas   hexutil.Big    `json:"gas"`
	Init  hexutil.Bytes  `json:"init"`
	Value hexutil.Big    `json:"value"`
}

type SuicideTraceAction struct {
	Address       common.Address `json:"address"`
	RefundAddress common.Address `json:"refundAddress"`
	Balance       hexutil.Big    `json:"balance"`
}

type RewardTraceAction struct {
	Author     common.Address `json:"author"`
	RewardType string         `json:"rewardType"`
	Value      hexutil.Big    `json:"value,omitempty"`
}

type CreateTraceResult struct {
	// Do not change the ordering of these fields -- allows for easier comparison with other clients
	Address *common.Address `json:"address,omitempty"`
	Code    hexutil.Bytes   `json:"code"`
	GasUsed *hexutil.Big    `json:"gasUsed"`
}

// TraceResult A parity formatted trace result
type TraceResult struct {
	// Do not change the ordering of these fields -- allows for easier comparison with other clients
	GasUsed *hexutil.Big  `json:"gasUsed"`
	Output  hexutil.Bytes `json:"output"`
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
	//ret += fmt.Sprintf("Action.SelfDestructed: %s\n", t.Action.SelfDestructed)
	//ret += fmt.Sprintf("Action.Balance: %s\n", t.Action.Balance)
	//ret += fmt.Sprintf("Action.CallType: %s\n", t.Action.CallType)
	//ret += fmt.Sprintf("Action.From: %s\n", t.Action.From)
	//ret += fmt.Sprintf("Action.Gas: %d\n", t.Action.Gas.ToInt())
	//ret += fmt.Sprintf("Action.Init: %s\n", t.Action.Init)
	//ret += fmt.Sprintf("Action.Input: %s\n", t.Action.Input)
	//ret += fmt.Sprintf("Action.RefundAddress: %s\n", t.Action.RefundAddress)
	//ret += fmt.Sprintf("Action.To: %s\n", t.Action.To)
	//ret += fmt.Sprintf("Action.Value: %s\n", t.Action.Value)
	ret += fmt.Sprintf("BlockHash: %v\n", t.BlockHash)
	ret += fmt.Sprintf("BlockNumber: %d\n", t.BlockNumber)
	//ret += fmt.Sprintf("Result.Address: %s\n", t.Result.Address)
	//ret += fmt.Sprintf("Result.Code: %s\n", t.Result.Code)
	//ret += fmt.Sprintf("Result.GasUsed: %s\n", t.Result.GasUsed)
	//ret += fmt.Sprintf("Result.Output: %s\n", t.Result.Output)
	ret += fmt.Sprintf("Subtraces: %d\n", t.Subtraces)
	ret += fmt.Sprintf("TraceAddress: %v\n", t.TraceAddress)
	ret += fmt.Sprintf("TransactionHash: %v\n", t.TransactionHash)
	ret += fmt.Sprintf("TransactionPosition: %d\n", t.TransactionPosition)
	ret += fmt.Sprintf("Type: %s\n", t.Type)
	return ret
}

// Takes a hierarchical Geth trace with fields of different meaning stored in the same named fields depending on 'type'. Parity traces
// are flattened depth first and each field is put in its proper place
func (api *TraceAPIImpl) convertToParityTrace(gethTrace GethTrace, blockHash common.Hash, blockNumber uint64, tx types.Transaction, txIndex uint64, depth []int) ParityTraces { //nolint: unused
	var traces ParityTraces // nolint prealloc
	return traces
}
