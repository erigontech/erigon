// Copyright 2026 The Erigon Authors
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
package protocol

import (
	"bytes"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon/common"
)

// IncomingCall is a successful call an EEZL2 entry delivers to its rollup.
type IncomingCall struct {
	SourceAddress  common.Address
	SourceRollupID uint64
	TargetAddress  common.Address
	// RollupID is the destination L2.
	RollupID   uint64
	Value      *big.Int // uint256
	Data       []byte
	ReturnData []byte
}

// OutgoingCall is a successful call an L2 sends to L1.
type OutgoingCall struct {
	SourceAddress common.Address
	// RollupID is the source L2.
	RollupID      uint64
	TargetAddress common.Address
	Value         *big.Int // uint256
	Data          []byte
	ReturnData    []byte
}

// BuildIncomingEntry builds the EEZL2 entry delivering c: one incoming call
// with zero gas, its call hash as proxy entry hash, and the rolling hash
// seed → CALL_BEGIN → CALL_END(true, returnData).
func BuildIncomingEntry(c *IncomingCall) (*L2ExecutionEntry, error) {
	callHash, err := CrossChainCallHash(&CallHashInput{
		SourceAddress:  c.SourceAddress,
		SourceRollupID: c.SourceRollupID,
		TargetAddress:  c.TargetAddress,
		TargetRollupID: c.RollupID,
		Value:          c.Value,
		Data:           c.Data,
	})
	if err != nil {
		return nil, err
	}
	r := L2EntrySeed(callHash)
	r.CallBegin(callHash)
	r.CallEnd(true, c.ReturnData)
	return &L2ExecutionEntry{
		ProxyEntryHash: callHash,
		IncomingCalls: []CrossChainCall{{
			SourceAddress:  c.SourceAddress,
			SourceRollupID: c.SourceRollupID,
			TargetAddress:  c.TargetAddress,
			Value:          c.Value,
			Data:           c.Data,
		}},
		ExpectedOutgoingCalls: []ExpectedCall{},
		RollingHash:           r.Sum(),
		Success:               true,
		ReturnData:            c.ReturnData,
	}, nil
}

// BuildOutgoingEntry builds the EEZL2 entry an outgoing call consumes: the
// call's hash with zero call gas as proxy entry hash and the bare seed as
// rolling hash. It has no incoming call, as the target runs on L1.
func BuildOutgoingEntry(c *OutgoingCall) (*L2ExecutionEntry, error) {
	callHash, err := CrossChainCallHash(&CallHashInput{
		SourceAddress:  c.SourceAddress,
		SourceRollupID: c.RollupID,
		TargetAddress:  c.TargetAddress,
		TargetRollupID: MainnetRollupID,
		Value:          c.Value,
		Data:           c.Data,
	})
	if err != nil {
		return nil, err
	}
	r := L2EntrySeed(callHash)
	return &L2ExecutionEntry{
		ProxyEntryHash:        callHash,
		IncomingCalls:         []CrossChainCall{},
		ExpectedOutgoingCalls: []ExpectedCall{},
		RollingHash:           r.Sum(),
		Success:               true,
		ReturnData:            c.ReturnData,
	}, nil
}

// NewInboundSidecar returns the canonical inbound sidecar of c: the L1 entry
// layout carrying the call, hashes and return data of c's EEZL2 entry, with no
// rollup updates. c must come from another rollup and target an L2.
func NewInboundSidecar(c *IncomingCall) (*ExecutionEntry, error) {
	if c.RollupID == MainnetRollupID || c.SourceRollupID == c.RollupID {
		return nil, fmt.Errorf("%w: delivery from rollup %d to %d", ErrEntryShape, c.SourceRollupID, c.RollupID)
	}
	l2, err := BuildIncomingEntry(c)
	if err != nil {
		return nil, err
	}
	return &ExecutionEntry{
		RollupUpdates:       []RollupUpdate{},
		ProxyEntryHash:      l2.ProxyEntryHash,
		L2ToL1Calls:         l2.IncomingCalls,
		ExpectedL1ToL2Calls: []ExpectedCall{},
		RollingHash:         l2.RollingHash,
		DestinationRollupID: c.RollupID,
		Success:             true,
		ReturnData:          l2.ReturnData,
	}, nil
}

// IncomingCallFromSidecar returns the call an inbound sidecar delivers. The
// sidecar must be exactly the canonical one for that call.
func IncomingCallFromSidecar(e *ExecutionEntry) (*IncomingCall, error) {
	if len(e.L2ToL1Calls) != 1 {
		return nil, fmt.Errorf("%w: inbound sidecar with %d calls", ErrEntryShape, len(e.L2ToL1Calls))
	}
	call := &e.L2ToL1Calls[0]
	c := &IncomingCall{
		SourceAddress:  call.SourceAddress,
		SourceRollupID: call.SourceRollupID,
		TargetAddress:  call.TargetAddress,
		RollupID:       e.DestinationRollupID,
		Value:          call.Value,
		Data:           call.Data,
		ReturnData:     e.ReturnData,
	}
	canonical, err := NewInboundSidecar(c)
	if err != nil {
		return nil, err
	}
	want, err := EncodeExecutionEntry(canonical)
	if err != nil {
		return nil, err
	}
	got, err := EncodeExecutionEntry(e)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(want, got) {
		return nil, fmt.Errorf("%w: non-canonical inbound sidecar", ErrEntryShape)
	}
	return c, nil
}
