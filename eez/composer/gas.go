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
	"github.com/erigontech/erigon/execution/types"
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
	standard, tokens, err := postBatchGas(b)
	if err != nil {
		return 0, err
	}
	return max(standard, txGas+10*tokens), nil
}

// postBatchGas projects the gas of posting b with a proof of
// proofPlaceholder at the standard rate, and returns its calldata's EIP-7623
// tokens.
func postBatchGas(b *protocol.ProofSystemBatch) (standard, tokens uint64, err error) {
	sized := *b
	sized.Proofs = [][]byte{proofPlaceholder}
	input, err := protocol.EncodePostAndVerifyBatch(&sized)
	if err != nil {
		return 0, 0, err
	}
	return txGas + postBatchBaseGas + postBatchEntryGas*uint64(len(b.Entries)) + calldataGas(input), floorTokens(input), nil
}

// FloorGas is the EIP-7623 floor of a transaction with input.
func FloorGas(input []byte) uint64 { return txGas + 10*floorTokens(input) }

// floorTokens are the EIP-7623 tokens of input: 1 a zero byte, 4 any other.
func floorTokens(input []byte) uint64 {
	zero := uint64(bytes.Count(input, []byte{0}))
	return zero + 4*(uint64(len(input))-zero)
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
// calldata at the standard rate and in EIP-7623 tokens, and the gas of the
// L1 calls the posting makes for it.
type l1Cost struct{ entries, calldata, tokens, target uint64 }

// budget projects the gas of a batch's posting as held transactions join
// it, from the anchor entry every batch leads with, at the standard rate
// and on the EIP-7623 floor of the calldata it prices, against a cap
// leaving the drain margin under the ceiling.
type budget struct{ cap, projected, tokens uint64 }

func newBudget(maxGas uint64) *budget {
	b := &budget{projected: txGas + postBatchBaseGas + postBatchEntryGas}
	if maxGas > drainMargin {
		b.cap = maxGas - drainMargin
	}
	return b
}

// windowBudget returns the budget of a batch settling blocks, the chain from
// the cursor's child to an empty Sync block, from its anchor-only batch.
func windowBudget(batcher *Batcher, maxGas uint64, blocks []*types.Block) (*budget, error) {
	batch, err := batcher.AnchorBatch(blocks[0].ParentHash(), blocks)
	if err != nil {
		return nil, err
	}
	b := newBudget(maxGas)
	if b.projected, b.tokens, err = postBatchGas(batch); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *budget) after(c l1Cost) uint64 {
	return b.projected + postBatchEntryGas*c.entries + c.calldata + c.target
}

// floor is the EIP-7623 floor of the calldata priced so far.
func (b *budget) floor() uint64 { return txGas + 10*b.tokens }

// fits reports whether c keeps the projection and the floor within the cap.
func (b *budget) fits(c l1Cost) bool {
	return b.after(c) <= b.cap && txGas+10*(b.tokens+c.tokens) <= b.cap
}

// take charges c.
func (b *budget) take(c l1Cost) {
	b.projected, b.tokens = b.after(c), b.tokens+c.tokens
}

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
			c.tokens += floorTokens(enc)
		}
	}
	return c, nil
}
