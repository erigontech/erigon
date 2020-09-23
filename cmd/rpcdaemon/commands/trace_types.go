package commands

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
)

// TODO:(tjayrush)
// Implementation Notes:
// -- Many of these fields are of string type. I chose to do this for ease of debugging / clarity of code (less conversions).
//    Once we start optimizing this code, many of these fields will be made into their native types (Addresses, uint64, etc.)
// -- The ordering of the fields in the Parity types should not be changed. This allows us to compare output directly with existing Parity tests

// GethTrace The trace as received from the existing Geth javascript tracer 'callTracer'
type GethTrace struct {
	Type    string     `json:"type"`
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

// ParityTrace A trace in the desired format (Parity/OpenEtherum) See: https://openethereum.github.io/wiki/JSONRPC-trace-module
type ParityTrace struct {
	// Do not change the ordering of these fields -- allows for easier comparison with other clients
	Action              TraceAction `json:"action"`
	BlockHash           common.Hash `json:"blockHash"`
	BlockNumber         uint64      `json:"blockNumber"`
	Result              TraceResult `json:"result"`
	Subtraces           int         `json:"subtraces"`
	TraceAddress        []int       `json:"traceAddress"`
	TransactionHash     common.Hash `json:"transactionHash"`
	TransactionPosition uint64      `json:"transactionPosition"`
	Type                string      `json:"type"`
}

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

// ParityTraces what
type ParityTraces []ParityTrace

// TraceFilterRequest represents the arguments for trace_filter.
type TraceFilterRequest struct {
	FromBlock   *hexutil.Uint64   `json:"fromBlock"`
	ToBlock     *hexutil.Uint64   `json:"toBlock"`
	FromAddress []*common.Address `json:"fromAddress"`
	ToAddress   []*common.Address `json:"toAddress"`
	After       *uint64           `json:"after"`
	Count       *uint64           `json:"count"`
}
