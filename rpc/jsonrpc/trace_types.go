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
	"github.com/erigontech/erigon/rpc/jsonstream"
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

// MarshalFastJSONTo streams trace_block and trace_transaction results in encoding/json's field
// order and forms. An action or result of a type it does not know fails before the first write.
func (ts ParityTraces) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	if ts == nil {
		s.WriteNil()
		return nil
	}
	for i := range ts {
		if err := ts[i].checkKinds(); err != nil {
			return err
		}
	}
	s.WriteArrayStart()
	for i := range ts {
		ts[i].writeTo(s)
	}
	s.WriteArrayEnd()
	return nil
}

func (t *ParityTrace) checkKinds() error {
	switch t.Action.(type) {
	case nil, *CallTraceAction, *CreateTraceAction, *SuicideTraceAction, *RewardTraceAction:
	default:
		return fmt.Errorf("trace action of type %T has no JSON writer", t.Action)
	}
	switch t.Result.(type) {
	case nil, *TraceResult, *CreateTraceResult:
	default:
		return fmt.Errorf("trace result of type %T has no JSON writer", t.Result)
	}
	return nil
}

func (t *ParityTrace) writeTo(s *jsonstream.StackStream) {
	s.WriteObjectStart()
	s.Field("action")
	switch a := t.Action.(type) {
	case *CallTraceAction:
		a.writeTo(s)
	case *CreateTraceAction:
		a.writeTo(s)
	case *SuicideTraceAction:
		a.writeTo(s)
	case *RewardTraceAction:
		a.writeTo(s)
	default:
		s.WriteNil()
	}
	jsonstream.HexPtrOmitempty(s, "blockHash", t.BlockHash)
	if t.BlockNumber != nil {
		s.Field("blockNumber").Uint(*t.BlockNumber)
	}
	if t.Error != "" {
		s.Field("error").WriteString(t.Error)
	}
	s.Field("result")
	switch r := t.Result.(type) {
	case *TraceResult:
		r.writeTo(s)
	case *CreateTraceResult:
		r.writeTo(s)
	default:
		s.WriteNil()
	}
	s.Field("subtraces").Int(int64(t.Subtraces))
	s.Field("traceAddress")
	jsonstream.ArrayValue(s, t.TraceAddress, writeIntElem)
	jsonstream.HexPtrOmitempty(s, "transactionHash", t.TransactionHash)
	if t.TransactionPosition != nil {
		s.Field("transactionPosition").Uint(*t.TransactionPosition)
	}
	s.Field("type").WriteString(t.Type)
	s.WriteObjectEnd()
}

func writeIntElem(s *jsonstream.StackStream, v *int) { s.Int(int64(*v)) }

type CallTraceAction struct {
	From     common.Address `json:"from"`
	CallType string         `json:"callType"`
	Gas      hexutil.U256   `json:"gas"`
	Input    hexutil.Bytes  `json:"input"`
	To       common.Address `json:"to"`
	Value    hexutil.U256   `json:"value"`
}

func (a *CallTraceAction) writeTo(s *jsonstream.StackStream) {
	if a == nil {
		s.WriteNil()
		return
	}
	s.WriteObjectStart()
	jsonstream.Hex(s, "from", a.From)
	s.Field("callType").WriteString(a.CallType)
	jsonstream.Hex(s, "gas", a.Gas)
	jsonstream.Hex(s, "input", a.Input)
	jsonstream.Hex(s, "to", a.To)
	jsonstream.Hex(s, "value", a.Value)
	s.WriteObjectEnd()
}

type CreateTraceAction struct {
	From           common.Address  `json:"from"`
	CreationMethod string          `json:"creationMethod"`
	Gas            hexutil.U256    `json:"gas"`
	StateGas       *hexutil.Uint64 `json:"stateGasReservoir,omitempty"`
	Init           hexutil.Bytes   `json:"init"`
	Value          hexutil.U256    `json:"value"`
}

func (a *CreateTraceAction) writeTo(s *jsonstream.StackStream) {
	if a == nil {
		s.WriteNil()
		return
	}
	s.WriteObjectStart()
	jsonstream.Hex(s, "from", a.From)
	s.Field("creationMethod").WriteString(a.CreationMethod)
	jsonstream.Hex(s, "gas", a.Gas)
	jsonstream.HexPtrOmitempty(s, "stateGasReservoir", a.StateGas)
	jsonstream.Hex(s, "init", a.Init)
	jsonstream.Hex(s, "value", a.Value)
	s.WriteObjectEnd()
}

type SuicideTraceAction struct {
	Address       common.Address `json:"address"`
	RefundAddress common.Address `json:"refundAddress"`
	Balance       hexutil.U256   `json:"balance"`
}

func (a *SuicideTraceAction) writeTo(s *jsonstream.StackStream) {
	if a == nil {
		s.WriteNil()
		return
	}
	s.WriteObjectStart()
	jsonstream.Hex(s, "address", a.Address)
	jsonstream.Hex(s, "refundAddress", a.RefundAddress)
	jsonstream.Hex(s, "balance", a.Balance)
	s.WriteObjectEnd()
}

type RewardTraceAction struct {
	Author     common.Address `json:"author"`
	RewardType string         `json:"rewardType"`
	Value      hexutil.U256   `json:"value"`
}

func (a *RewardTraceAction) writeTo(s *jsonstream.StackStream) {
	if a == nil {
		s.WriteNil()
		return
	}
	s.WriteObjectStart()
	jsonstream.Hex(s, "author", a.Author)
	s.Field("rewardType").WriteString(a.RewardType)
	jsonstream.Hex(s, "value", a.Value)
	s.WriteObjectEnd()
}

type CreateTraceResult struct {
	// Do not change the ordering of these fields -- allows for easier comparison with other clients
	Address      *common.Address `json:"address,omitempty"`
	Code         hexutil.Bytes   `json:"code"`
	GasUsed      *hexutil.U256   `json:"gasUsed"`                // execution gas for the root frame and each child frame.
	StateGasUsed *hexutil.Int64  `json:"stateGasUsed,omitempty"` // amsterdam: signed net state usage for the root frame and each child frame.
}

func (r *CreateTraceResult) writeTo(s *jsonstream.StackStream) {
	if r == nil {
		s.WriteNil()
		return
	}
	s.WriteObjectStart()
	jsonstream.HexPtrOmitempty(s, "address", r.Address)
	jsonstream.Hex(s, "code", r.Code)
	jsonstream.HexPtr(s, "gasUsed", r.GasUsed)
	jsonstream.HexPtrOmitempty(s, "stateGasUsed", r.StateGasUsed)
	s.WriteObjectEnd()
}

// TraceResult A parity formatted trace result
type TraceResult struct {
	// Do not change the ordering of these fields -- allows for easier comparison with other clients
	GasUsed      *hexutil.U256  `json:"gasUsed"` // execution gas for the root frame and each child frame.
	Output       hexutil.Bytes  `json:"output"`
	StateGasUsed *hexutil.Int64 `json:"stateGasUsed,omitempty"` // amsterdam: signed net state usage for the root frame and each child frame.
}

func (r *TraceResult) writeTo(s *jsonstream.StackStream) {
	if r == nil {
		s.WriteNil()
		return
	}
	s.WriteObjectStart()
	jsonstream.HexPtr(s, "gasUsed", r.GasUsed)
	jsonstream.Hex(s, "output", r.Output)
	jsonstream.HexPtrOmitempty(s, "stateGasUsed", r.StateGasUsed)
	s.WriteObjectEnd()
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
