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
	"math/big"
	"math/rand/v2"

	"github.com/erigontech/erigon/common"
)

var maxUint256 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))

// valueGen builds values with every field set from a seeded source, including
// integer bounds and empty, word-aligned and unaligned byte strings. Slices
// are never nil, matching what the decoder returns.
type valueGen struct{ r *rand.Rand }

func newValueGen(seed uint64) valueGen {
	return valueGen{rand.New(rand.NewPCG(seed, ^seed))}
}

func (g valueGen) count() int { return g.r.IntN(3) }
func (g valueGen) bool() bool { return g.r.IntN(2) == 1 }

func (g valueGen) bytes() []byte {
	lengths := [...]int{0, 1, 31, 32, 33, 64, 70}
	b := make([]byte, lengths[g.r.IntN(len(lengths))])
	for i := range b {
		b[i] = byte(g.r.Uint32())
	}
	return b
}

func (g valueGen) hash() (h common.Hash) {
	for i := range h {
		h[i] = byte(g.r.Uint32())
	}
	return h
}

func (g valueGen) address() (a common.Address) {
	for i := range a {
		a[i] = byte(g.r.Uint32())
	}
	return a
}

func (g valueGen) uint256() *big.Int {
	switch g.r.IntN(4) {
	case 0:
		return new(big.Int)
	case 1:
		return new(big.Int).Set(maxUint256)
	}
	h := g.hash()
	return new(big.Int).SetBytes(h[:])
}

func (g valueGen) int192() *big.Int {
	switch g.r.IntN(5) {
	case 0:
		return new(big.Int).Set(minInt192)
	case 1:
		return new(big.Int).Set(maxInt192)
	case 2:
		return big.NewInt(-1)
	}
	h := g.hash()
	v := new(big.Int).SetBytes(h[:24])
	return v.Add(v, minInt192)
}

func (g valueGen) call() CrossChainCall {
	return CrossChainCall{
		RevertNextNCalls: uint16(g.r.Uint32()),
		IsStatic:         g.bool(),
		Gas:              g.r.Uint64(),
		SourceAddress:    g.address(),
		SourceRollupID:   g.r.Uint64(),
		TargetAddress:    g.address(),
		Value:            g.uint256(),
		Data:             g.bytes(),
	}
}

func (g valueGen) calls() []CrossChainCall {
	cs := make([]CrossChainCall, g.count())
	for i := range cs {
		cs[i] = g.call()
	}
	return cs
}

func (g valueGen) expectedCalls() []ExpectedCall {
	es := make([]ExpectedCall, g.count())
	for i := range es {
		es[i] = ExpectedCall{
			Hash:                        g.hash(),
			Calls:                       g.calls(),
			RevertedOrStaticRollingHash: g.hash(),
			Success:                     g.bool(),
			ReturnData:                  g.bytes(),
		}
	}
	return es
}

func (g valueGen) expectedRoots() []ExpectedRootPerRollup {
	rs := make([]ExpectedRootPerRollup, g.count())
	for i := range rs {
		rs[i] = ExpectedRootPerRollup{RollupID: g.r.Uint64(), Root: g.hash()}
	}
	return rs
}

func (g valueGen) entry() ExecutionEntry {
	updates := make([]RollupUpdate, g.count())
	for i := range updates {
		updates[i] = RollupUpdate{RollupID: g.r.Uint64(), EtherDelta: g.int192(), CurrentRoot: g.hash(), NewRoot: g.hash()}
	}
	return ExecutionEntry{
		RollupUpdates:       updates,
		ProxyEntryHash:      g.hash(),
		L2ToL1Calls:         g.calls(),
		ExpectedL1ToL2Calls: g.expectedCalls(),
		RollingHash:         g.hash(),
		DestinationRollupID: g.r.Uint64(),
		Success:             g.bool(),
		ReturnData:          g.bytes(),
	}
}

func (g valueGen) staticEntry() StaticExecutionEntry {
	return StaticExecutionEntry{
		ExpectedRoots:       g.expectedRoots(),
		ProxyEntryHash:      g.hash(),
		L2ToL1Calls:         g.calls(),
		RollingHash:         g.hash(),
		DestinationRollupID: g.r.Uint64(),
		Success:             g.bool(),
		ReturnData:          g.bytes(),
	}
}

func (g valueGen) batch() ProofSystemBatch {
	b := ProofSystemBatch{
		ExpectedRootPerRollup:      g.expectedRoots(),
		Entries:                    make([]ExecutionEntry, g.count()),
		StaticEntries:              make([]StaticExecutionEntry, g.count()),
		ImmediateEntryCount:        g.uint256(),
		ImmediateStaticEntryCount:  g.uint256(),
		ProofSystems:               make([]common.Address, g.count()),
		RollupIDsWithProofSystems:  make([]RollupIDWithProofSystems, g.count()),
		BlobIndices:                make([]*big.Int, g.count()),
		CallData:                   g.bytes(),
		Proofs:                     make([][]byte, g.count()),
		BlockNumber:                g.r.Uint64(),
		BindMsgSenderInPublicInput: g.bool(),
	}
	for i := range b.Entries {
		b.Entries[i] = g.entry()
	}
	for i := range b.StaticEntries {
		b.StaticEntries[i] = g.staticEntry()
	}
	for i := range b.ProofSystems {
		b.ProofSystems[i] = g.address()
	}
	for i := range b.RollupIDsWithProofSystems {
		indexes := make([]uint64, g.count())
		for j := range indexes {
			indexes[j] = g.r.Uint64()
		}
		b.RollupIDsWithProofSystems[i] = RollupIDWithProofSystems{RollupID: g.r.Uint64(), ProofSystemIndexes: indexes}
	}
	for i := range b.BlobIndices {
		b.BlobIndices[i] = g.uint256()
	}
	for i := range b.Proofs {
		b.Proofs[i] = g.bytes()
	}
	return b
}

func (g valueGen) l2Entries() []L2ExecutionEntry {
	es := make([]L2ExecutionEntry, g.count())
	for i := range es {
		es[i] = L2ExecutionEntry{
			ProxyEntryHash:        g.hash(),
			IncomingCalls:         g.calls(),
			ExpectedOutgoingCalls: g.expectedCalls(),
			RollingHash:           g.hash(),
			Success:               g.bool(),
			ReturnData:            g.bytes(),
		}
	}
	return es
}

func (g valueGen) l2StaticEntries() []L2StaticExecutionEntry {
	es := make([]L2StaticExecutionEntry, g.count())
	for i := range es {
		es[i] = L2StaticExecutionEntry{
			ExpectedEntryIndex: g.uint256(),
			ProxyEntryHash:     g.hash(),
			IncomingCalls:      g.calls(),
			RollingHash:        g.hash(),
			Success:            g.bool(),
			ReturnData:         g.bytes(),
		}
	}
	return es
}
