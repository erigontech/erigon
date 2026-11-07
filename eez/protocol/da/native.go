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

// MaxDepth bounds the calls and Snapshots a rollupX transaction keeps open.
const MaxDepth = 64

var (
	ErrPadding  = errors.New("non-zero padding after CloseBlobStream")
	ErrSemantic = errors.New("invalid cross-chain transaction")
)

// NativePayload is the payload of a rollupX batch: one rollup's operations
// and its cross-chain transactions.
type NativePayload struct {
	Operations []byte
	Txs        []SemanticTx
}

// SemanticTx is a cross-chain transaction: where it starts, its signed
// transaction when it starts on L2, and its calls in preorder.
type SemanticTx struct {
	Origin uint64
	// TxData is the originating signed transaction, empty for an L1 origin.
	TxData  []byte
	Actions []SemanticAction
}

// SemanticAction is a call of a cross-chain transaction with its outcome.
type SemanticAction struct {
	// Parent is the index of the open call it is made from, -1 for a root.
	Parent         int
	Static         bool
	SourceRollupID uint64
	SourceAddress  common.Address
	TargetRollupID uint64
	TargetAddress  common.Address
	Value          uint256.Int
	Data           []byte
	Success        bool
	ReturnData     []byte
	// RevertSpan is the number of actions from this one that a Snapshot
	// region reverts, zero outside one.
	RevertSpan int
}

// DecodeNative decodes the native stream of a rollupX batch, carried in its
// blobs alone: the version byte, rollupID's RollupOperation, complete
// cross-chain transactions, CloseBlobStream, then zero padding.
func DecodeNative(blobs [][]byte, rollupID uint64) (*NativePayload, error) {
	if len(blobs) == 0 {
		return nil, fmt.Errorf("%w: no blobs", ErrProfile)
	}
	data := make([]byte, 0, len(blobs)*BlobDataSize)
	for i, blob := range blobs {
		b, err := BlobData(blob)
		if err != nil {
			return nil, fmt.Errorf("blob %d: %w", i, err)
		}
		data = append(data, b...)
	}
	rest, err := version(data)
	if err != nil {
		return nil, err
	}
	var d decoder
	if err := d.run(rest, true); err != nil {
		return nil, err
	}
	if !d.closed {
		return nil, fmt.Errorf("%w: blob portion ends without it", ErrBlobClose)
	}
	for i := d.pos; i < len(rest); i++ {
		if rest[i] != 0 {
			return nil, fmt.Errorf("%w: byte %d", ErrPadding, i+1)
		}
	}
	msgs, err := d.finish()
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 || msgs[0].Type != RollupOperation {
		return nil, fmt.Errorf("%w: no leading RollupOperation", ErrProfile)
	}
	if msgs[0].RollupID != rollupID {
		return nil, fmt.Errorf("%w: operations of rollup %d, not %d", ErrProfile, msgs[0].RollupID, rollupID)
	}
	txs, err := SemanticTxs(msgs[1:])
	if err != nil {
		return nil, err
	}
	return &NativePayload{Operations: msgs[0].Data, Txs: txs}, nil
}

// SemanticTxs reads the cross-chain transactions of a decoded stream. Calls
// carry no gas, a Revert closes a non-empty region at the depth its Snapshot
// opened it, regions do not overlap, and no transaction keeps more than
// MaxDepth calls or Snapshots open.
func SemanticTxs(msgs []Message) ([]SemanticTx, error) {
	var txs []SemanticTx
	var cur *SemanticTx
	var open []int
	type snapshot struct{ depth, first int }
	var snapshots []snapshot
	fail := func(format string, args ...any) ([]SemanticTx, error) {
		return nil, fmt.Errorf("%w: transaction %d: %s", ErrSemantic, len(txs), fmt.Sprintf(format, args...))
	}
	for i := range msgs {
		m := &msgs[i]
		if cur == nil && m.Type != InitiateCrossRollupTransaction {
			return fail("message type %d outside a transaction", m.Type)
		}
		switch m.Type {
		case InitiateCrossRollupTransaction:
			if cur != nil {
				return fail("nested transaction")
			}
			cur = &SemanticTx{Origin: m.RollupID, TxData: m.Data}
		case Call, StaticCall:
			if m.Gas != 0 {
				return fail("call gas %d", m.Gas)
			}
			if len(open) == MaxDepth {
				return fail("deeper than %d calls", MaxDepth)
			}
			parent := -1
			if len(open) > 0 {
				parent = open[len(open)-1]
			}
			cur.Actions = append(cur.Actions, SemanticAction{
				Parent:         parent,
				Static:         m.Type == StaticCall,
				SourceRollupID: m.FromRollup,
				SourceAddress:  m.FromAddress,
				TargetRollupID: m.RollupID,
				TargetAddress:  m.ToAddress,
				Value:          m.Value,
				Data:           m.Data,
			})
			open = append(open, len(cur.Actions)-1)
		case ReturnSuccess, ReturnFail:
			if len(open) == 0 {
				return fail("return without a call")
			}
			a := &cur.Actions[open[len(open)-1]]
			a.Success, a.ReturnData = m.Type == ReturnSuccess, m.Data
			open = open[:len(open)-1]
		case Snapshot:
			if len(snapshots) == MaxDepth {
				return fail("deeper than %d Snapshots", MaxDepth)
			}
			snapshots = append(snapshots, snapshot{depth: len(open), first: len(cur.Actions)})
		case Revert:
			if len(snapshots) == 0 {
				return fail("Revert without a Snapshot")
			}
			s := snapshots[len(snapshots)-1]
			snapshots = snapshots[:len(snapshots)-1]
			switch {
			case len(open) != s.depth:
				return fail("Revert crosses an open call")
			case len(cur.Actions) == s.first:
				return fail("empty Snapshot region")
			case cur.Actions[s.first].RevertSpan != 0:
				return fail("overlapping Snapshot regions")
			}
			cur.Actions[s.first].RevertSpan = len(cur.Actions) - s.first
		case FinishCrossRollupTransaction:
			if len(open) != 0 || len(snapshots) != 0 {
				return fail("finished with open calls or Snapshots")
			}
			txs = append(txs, *cur)
			cur = nil
		default:
			return fail("message type %d", m.Type)
		}
	}
	if cur != nil {
		return fail("stream ends inside it")
	}
	return txs, nil
}
