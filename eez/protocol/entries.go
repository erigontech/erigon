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
	"errors"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/eez/protocol/da"
)

// ErrEntryShape reports an entry outside the shapes derivation lowers to
// system transactions.
var ErrEntryShape = errors.New("unsupported entry shape")

// EntryFromAction rebuilds the entry an action bracket of the DA payload
// describes for rollup local. An action from local to L1 is outbound: its
// entry leads the batch with zero proxy and rolling hashes, which settlement
// fills in. An action targeting local is inbound: its entry carries the call's
// hash (zero call gas) as proxy entry hash and the EEZL2 rolling hash of the
// delivery, seed → CALL_BEGIN → CALL_END.
func EntryFromAction(a *da.Action, local uint64) (*ExecutionEntry, error) {
	outbound := a.SourceRollupID == local && a.TargetRollupID == MainnetRollupID
	if !outbound && a.TargetRollupID != local {
		return nil, fmt.Errorf("action from rollup %d to %d involves neither direction of rollup %d", a.SourceRollupID, a.TargetRollupID, local)
	}
	call := CrossChainCall{
		Gas:            a.Gas,
		SourceAddress:  a.SourceAddress,
		SourceRollupID: a.SourceRollupID,
		TargetAddress:  a.TargetAddress,
		Value:          a.Value.ToBig(),
		Data:           a.Data,
	}
	e := &ExecutionEntry{
		RollupUpdates:       []RollupUpdate{},
		L2ToL1Calls:         []CrossChainCall{call},
		ExpectedL1ToL2Calls: []ExpectedCall{},
		DestinationRollupID: local,
		Success:             a.Success,
		ReturnData:          a.ReturnData,
	}
	if outbound {
		return e, nil
	}
	callHash, err := CrossChainCallHash(&CallHashInput{
		SourceAddress:  call.SourceAddress,
		SourceRollupID: call.SourceRollupID,
		TargetAddress:  call.TargetAddress,
		TargetRollupID: a.TargetRollupID,
		Value:          call.Value,
		Data:           call.Data,
	})
	if err != nil {
		return nil, err
	}
	r := L2EntrySeed(callHash)
	r.CallBegin(callHash)
	r.CallEnd(a.Success, a.ReturnData)
	e.ProxyEntryHash, e.RollingHash = callHash, r.Sum()
	return e, nil
}

// ActionFromEntry describes e, an entry of rollup local, as the action
// bracket it was built from. A call leaving local targets L1; any other call
// must be a delivery to local.
func ActionFromEntry(e *ExecutionEntry, local uint64) (*da.Action, error) {
	if len(e.L2ToL1Calls) != 1 || len(e.ExpectedL1ToL2Calls) != 0 {
		return nil, fmt.Errorf("%w: %d calls and %d expected calls, want one call", ErrEntryShape, len(e.L2ToL1Calls), len(e.ExpectedL1ToL2Calls))
	}
	c := &e.L2ToL1Calls[0]
	if c.IsStatic || c.RevertNextNCalls != 0 {
		return nil, fmt.Errorf("%w: static call or revert span", ErrEntryShape)
	}
	target := local
	if c.SourceRollupID == local {
		target = MainnetRollupID
	} else if e.DestinationRollupID != local {
		return nil, fmt.Errorf("%w: delivery to rollup %d, not %d", ErrEntryShape, e.DestinationRollupID, local)
	}
	value, err := toUint256(c.Value)
	if err != nil {
		return nil, fmt.Errorf("call value: %w", err)
	}
	return &da.Action{
		SourceRollupID: c.SourceRollupID,
		TargetRollupID: target,
		SourceAddress:  c.SourceAddress,
		TargetAddress:  c.TargetAddress,
		Value:          value,
		Gas:            c.Gas,
		Data:           c.Data,
		Success:        e.Success,
		ReturnData:     e.ReturnData,
	}, nil
}

// CheckEntryShape rejects what the Sync-block lowering cannot represent: more
// than one call, nested expected calls, a failed entry, and calls that are
// static, open a revert span or set gas.
func CheckEntryShape(e *ExecutionEntry) error {
	switch {
	case len(e.L2ToL1Calls) > 1:
		return fmt.Errorf("%w: %d calls", ErrEntryShape, len(e.L2ToL1Calls))
	case len(e.ExpectedL1ToL2Calls) != 0:
		return fmt.Errorf("%w: nested calls", ErrEntryShape)
	case !e.Success:
		return fmt.Errorf("%w: failed entry", ErrEntryShape)
	}
	for i := range e.L2ToL1Calls {
		if c := &e.L2ToL1Calls[i]; c.IsStatic || c.RevertNextNCalls != 0 || c.Gas != 0 {
			return fmt.Errorf("%w: static call, revert span or explicit gas", ErrEntryShape)
		}
	}
	return nil
}

func toUint256(v *big.Int) (uint256.Int, error) {
	if err := checkUint256(v); err != nil {
		return uint256.Int{}, err
	}
	var u uint256.Int
	u.SetFromBig(v)
	return u, nil
}
