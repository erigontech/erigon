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
package da

import (
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
)

// ErrProfile reports a valid stream that the client DA profile does not admit.
var ErrProfile = errors.New("stream outside the DA profile")

// Action is one action bracket: InitiateCrossRollupTransaction, Call,
// ReturnSuccess or ReturnFail, FinishCrossRollupTransaction.
type Action struct {
	// SourceRollupID is the rollup the transaction starts on, which the Call
	// leaves.
	SourceRollupID uint64
	TargetRollupID uint64
	SourceAddress  common.Address
	TargetAddress  common.Address
	Value          uint256.Int
	Gas            uint64
	Data           []byte
	Success        bool
	ReturnData     []byte
}

// Payload is a batch's DA payload under the client profile.
type Payload struct {
	// RollupID is the rollup whose operations the stream carries.
	RollupID uint64
	// Operations is the rollup-defined content of the RollupOperation.
	Operations []byte
	// Actions are the brackets in stream order; bracket i is manifest index i.
	Actions []Action
}

// ParsePayload applies the client profile to a decoded stream: one
// RollupOperation first, then action brackets of exactly one Call each.
// tx_data is opaque to the protocol and skipped, as the reference client
// does.
func ParsePayload(msgs []Message) (*Payload, error) {
	if len(msgs) == 0 || msgs[0].Type != RollupOperation {
		return nil, fmt.Errorf("%w: no leading RollupOperation", ErrProfile)
	}
	p := &Payload{RollupID: msgs[0].RollupID, Operations: msgs[0].Data}
	shape := [...]MessageType{InitiateCrossRollupTransaction, Call, ReturnSuccess, FinishCrossRollupTransaction}
	rest := msgs[1:]
	for len(rest) > 0 {
		if len(rest) < len(shape) {
			return nil, fmt.Errorf("%w: incomplete bracket %d", ErrProfile, len(p.Actions))
		}
		for i, want := range shape {
			if got := rest[i].Type; got != want && (want != ReturnSuccess || got != ReturnFail) {
				return nil, fmt.Errorf("%w: bracket %d has message %d of type %d", ErrProfile, len(p.Actions), i, got)
			}
		}
		call, ret := &rest[1], &rest[2]
		p.Actions = append(p.Actions, Action{
			SourceRollupID: call.FromRollup,
			TargetRollupID: call.RollupID,
			SourceAddress:  call.FromAddress,
			TargetAddress:  call.ToAddress,
			Value:          call.Value,
			Gas:            call.Gas,
			Data:           call.Data,
			Success:        ret.Type == ReturnSuccess,
			ReturnData:     ret.Data,
		})
		rest = rest[len(shape):]
	}
	return p, nil
}
