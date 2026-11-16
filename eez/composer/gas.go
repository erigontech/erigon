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

package composer

import (
	"bytes"

	"github.com/erigontech/erigon/eez/protocol"
)

const (
	// MaxPostBatchGas is the EIP-7825 cap on a transaction's gas, the
	// default ceiling of a posting.
	MaxPostBatchGas = 16_777_216
	// A batch's execution, calldata aside, is projected at postBatchBaseGas
	// and postBatchEntryGas an entry: postAndVerifyBatch's measured cost with
	// 10% over.
	postBatchBaseGas  = 160_000
	postBatchEntryGas = 370_000
	// drainMargin leaves room for the framing a drain of held transactions
	// cannot price.
	drainMargin = 600_000
	// MinPostBatchGas is the lowest ceiling taken: a batch of two entries
	// and the drain margin.
	MinPostBatchGas = txGas + postBatchBaseGas + 2*postBatchEntryGas + drainMargin

	txGas = 21_000
)

// proofPlaceholder stands for the proof of a batch not yet proven: longer
// than a 65-byte ECDSA attestation and all non-zero, so it prices above it.
var proofPlaceholder = bytes.Repeat([]byte{0xff}, 128)

// PostBatchGas projects the gas of posting b with a proof of
// proofPlaceholder: the larger of its execution and calldata at the standard
// rate and its EIP-7623 floor.
func PostBatchGas(b *protocol.ProofSystemBatch) (uint64, error) {
	sized := *b
	sized.Proofs = [][]byte{proofPlaceholder}
	input, err := protocol.EncodePostAndVerifyBatch(&sized)
	if err != nil {
		return 0, err
	}
	standard := txGas + postBatchBaseGas + postBatchEntryGas*uint64(len(b.Entries)) + calldataGas(input)
	return max(standard, FloorGas(input)), nil
}

// FloorGas is the EIP-7623 floor of a transaction with input.
func FloorGas(input []byte) uint64 {
	zero := uint64(bytes.Count(input, []byte{0}))
	return txGas + 10*(zero+4*(uint64(len(input))-zero))
}

// calldataGas is the standard price of input: 4 a zero byte, 16 any other.
func calldataGas(input []byte) uint64 {
	zero := uint64(bytes.Count(input, []byte{0}))
	return 4*zero + 16*(uint64(len(input))-zero)
}

// PostBatchGasCeiling returns requested as the ceiling of a posting's gas
// when it lies between MinPostBatchGas and MaxPostBatchGas, else
// MaxPostBatchGas and false.
func PostBatchGasCeiling(requested uint64) (uint64, bool) {
	if requested < MinPostBatchGas || requested > MaxPostBatchGas {
		return MaxPostBatchGas, false
	}
	return requested, true
}

// l1Cost is what a held transaction adds to its batch's posting: entries,
// calldata at the standard rate, and the gas of the L1 calls the posting
// makes for it.
type l1Cost struct{ entries, calldata, target uint64 }

// budget projects the gas of a batch's posting as held transactions join
// it, from the anchor entry every batch leads with, against a cap leaving
// the drain margin under the ceiling.
type budget struct{ cap, projected uint64 }

func newBudget(maxGas uint64) *budget {
	b := &budget{projected: txGas + postBatchBaseGas + postBatchEntryGas}
	if maxGas > drainMargin {
		b.cap = maxGas - drainMargin
	}
	return b
}

func (b *budget) after(c l1Cost) uint64 {
	return b.projected + postBatchEntryGas*c.entries + c.calldata + c.target
}

// fits reports whether c keeps the projection within the cap.
func (b *budget) fits(c l1Cost) bool { return b.after(c) <= b.cap }

// take charges c.
func (b *budget) take(c l1Cost) { b.projected = b.after(c) }

// inboundCost is what deliveries add: each a deferred entry in the batch
// and a sidecar in its DA.
func inboundCost(ds []*Delivery) (l1Cost, error) {
	c := l1Cost{entries: uint64(len(ds))}
	for _, d := range ds {
		deferred := inboundEntry(d.Sidecar)
		for _, e := range []*protocol.ExecutionEntry{d.Sidecar, &deferred} {
			enc, err := protocol.EncodeExecutionEntry(e)
			if err != nil {
				return l1Cost{}, err
			}
			c.calldata += calldataGas(enc)
		}
	}
	return c, nil
}
