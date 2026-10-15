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
package deriver

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/protocol/da"
)

// ErrBlobPayload reports a batch whose DA payload is in blobs, which this
// follower does not read.
var ErrBlobPayload = errors.New("DA payload in blobs")

// Payload is the part of a batch's DA payload the deriver replays.
type Payload struct {
	Span    *da.Span
	Actions []da.Action
}

// Blocks is the number of L2 blocks the payload carries.
func (p *Payload) Blocks() uint64 { return uint64(len(p.Span.TxCounts)) }

// DecodePayload decodes the DA payload a batch carries in its callData. It
// returns nil for another rollup's operations, which a multi-rollup batch
// carries and which are not ours to judge.
func DecodePayload(b *protocol.ProofSystemBatch, rollupID uint64) (*Payload, error) {
	if len(b.BlobIndices) > 0 {
		return nil, fmt.Errorf("%w: %d blobs", ErrBlobPayload, len(b.BlobIndices))
	}
	msgs, err := da.Decode(nil, b.CallData)
	if err != nil {
		return nil, err
	}
	p, err := da.ParsePayload(msgs)
	if err != nil {
		return nil, err
	}
	if p.RollupID != rollupID {
		return nil, nil
	}
	span, err := da.DecodeSpan(p.Operations)
	if err != nil {
		return nil, err
	}
	return &Payload{Span: span, Actions: p.Actions}, nil
}
