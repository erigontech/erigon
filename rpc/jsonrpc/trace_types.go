// Copyright 2024 The Erigon Authors
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

package jsonrpc

import (
	"fmt"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

// ParityTrace A trace in the desired format (Parity/OpenEthereum) See: https://openethereum.github.io/JSONRPC-trace-module
type ParityTrace struct {
	// Do not change the ordering of these fields -- allows for easier comparison with other clients
	Action              any          `json:"action"` // Can be either CallTraceAction or CreateTraceAction
	BlockHash           *common.Hash `json:"blockHash,omitempty"`
	BlockNumber         *uint64      `json:"blockNumber,omitempty"`
	Error               string       `json:"error,omitempty"`
	Result              any          `json:"result"`
	Subtraces           int          `json:"subtraces"`
	TraceAddress        []int        `json:"traceAddress"`
	TransactionHash     *common.Hash `json:"transactionHash,omitempty"`
	TransactionPosition *uint64      `json:"transactionPosition,omitempty"`
	Type                string       `json:"type"`
}

// ParityTraces An array of parity traces
type ParityTraces []ParityTrace

type CallTraceAction struct {
	From     common.Address `json:"from"`
	CallType string         `json:"callType"`
	Gas      hexutil.U256   `json:"gas"`
	Input    hexutil.Bytes  `json:"input"`
	To       common.Address `json:"to"`
	Value    hexutil.U256   `json:"value"`
}

type CreateTraceAction struct {
	From           common.Address  `json:"from"`
	CreationMethod string          `json:"creationMethod"`
	Gas            hexutil.U256    `json:"gas"`
	StateGas       *hexutil.Uint64 `json:"stateGasReservoir,omitempty"`
	Init           hexutil.Bytes   `json:"init"`
	Value          hexutil.U256    `json:"value"`
}

type SuicideTraceAction struct {
	Address       common.Address `json:"address"`
	RefundAddress common.Address `json:"refundAddress"`
	Balance       hexutil.U256   `json:"balance"`
}

type RewardTraceAction struct {
	Author     common.Address `json:"author"`
	RewardType string         `json:"rewardType"`
	Value      hexutil.U256   `json:"value"`
}

type CreateTraceResult struct {
	// Do not change the ordering of these fields -- allows for easier comparison with other clients
	Address      *common.Address `json:"address,omitempty"`
	Code         hexutil.Bytes   `json:"code"`
	GasUsed      *hexutil.U256   `json:"gasUsed"`                // execution gas for the root frame and each child frame.
	StateGasUsed *hexutil.Int64  `json:"stateGasUsed,omitempty"` // amsterdam: signed net state usage for the root frame and each child frame.
}

// TraceResult A parity formatted trace result
type TraceResult struct {
	// Do not change the ordering of these fields -- allows for easier comparison with other clients
	GasUsed      *hexutil.U256  `json:"gasUsed"` // execution gas for the root frame and each child frame.
	Output       hexutil.Bytes  `json:"output"`
	StateGasUsed *hexutil.Int64 `json:"stateGasUsed,omitempty"` // amsterdam: signed net state usage for the root frame and each child frame.
}

// Allows for easy printing of a parity trace for debugging
func (t ParityTrace) String() string {
	var ret strings.Builder
	//ret.WriteString(fmt.Sprintf("Action.SelfDestructed: %s\n", t.Action.SelfDestructed))
	//ret.WriteString(fmt.Sprintf("Action.Balance: %s\n", t.Action.Balance))
	//ret.WriteString(fmt.Sprintf("Action.CallType: %s\n", t.Action.CallType))
	//ret.WriteString(fmt.Sprintf("Action.From: %s\n", t.Action.From))
	//ret.WriteString(fmt.Sprintf("Action.Gas: %d\n", t.Action.Gas.ToInt()))
	//ret.WriteString(fmt.Sprintf("Action.Init: %s\n", t.Action.Init))
	//ret.WriteString(fmt.Sprintf("Action.Input: %s\n", t.Action.Input))
	//ret.WriteString(fmt.Sprintf("Action.RefundAddress: %s\n", t.Action.RefundAddress))
	//ret.WriteString(fmt.Sprintf("Action.To: %s\n", t.Action.To))
	//ret.WriteString(fmt.Sprintf("Action.Value: %s\n", t.Action.Value))
	ret.WriteString(fmt.Sprintf("BlockHash: %v\n", t.BlockHash))
	ret.WriteString(fmt.Sprintf("BlockNumber: %d\n", t.BlockNumber))
	//ret.WriteString(fmt.Sprintf("Result.Address: %s\n", t.Result.Address))
	//ret.WriteString(fmt.Sprintf("Result.Code: %s\n", t.Result.Code))
	//ret.WriteString(fmt.Sprintf("Result.GasUsed: %s\n", t.Result.GasUsed))
	//ret.WriteString(fmt.Sprintf("Result.Output: %s\n", t.Result.Output))
	ret.WriteString(fmt.Sprintf("Subtraces: %d\n", t.Subtraces))
	ret.WriteString(fmt.Sprintf("TraceAddress: %v\n", t.TraceAddress))
	ret.WriteString(fmt.Sprintf("TransactionHash: %v\n", t.TransactionHash))
	ret.WriteString(fmt.Sprintf("TransactionPosition: %d\n", t.TransactionPosition))
	ret.WriteString(fmt.Sprintf("Type: %s\n", t.Type))
	return ret.String()
}
