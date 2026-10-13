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
	"encoding/binary"
	"math/big"
	"slices"

	"github.com/erigontech/erigon/common"
)

// A reference ABI encoder written from the Solidity ABI specification and
// the IEEZ.sol/IEEZL2.sol struct definitions, independent of execution/abi,
// to cross-check the encodings produced through it.

type refValue interface {
	isDynamic() bool
	encode() []byte
}

type (
	refWord  [32]byte
	refBytes []byte
	refTuple []refValue
	refArray []refValue
)

func (refWord) isDynamic() bool    { return false }
func (refBytes) isDynamic() bool   { return true }
func (refArray) isDynamic() bool   { return true }
func (t refTuple) isDynamic() bool { return slices.ContainsFunc(t, refValue.isDynamic) }

func (w refWord) encode() []byte { return w[:] }

func (b refBytes) encode() []byte {
	out := refUint(uint64(len(b))).encode()
	out = append(out, b...)
	return append(out, make([]byte, (32-len(b)%32)%32)...)
}

func (t refTuple) encode() []byte { return refSequence(t) }

func (a refArray) encode() []byte {
	return append(refUint(uint64(len(a))).encode(), refSequence(a)...)
}

// refSequence lays out values as heads followed by the tails of the dynamic
// ones; a dynamic value's head is its tail offset from the sequence start.
func refSequence(vs []refValue) []byte {
	headSize := 0
	for _, v := range vs {
		if v.isDynamic() {
			headSize += 32
		} else {
			headSize += len(v.encode())
		}
	}
	var head, tail []byte
	for _, v := range vs {
		enc := v.encode()
		if !v.isDynamic() {
			head = append(head, enc...)
			continue
		}
		head = append(head, refUint(uint64(headSize+len(tail))).encode()...)
		tail = append(tail, enc...)
	}
	return append(head, tail...)
}

// refEncode is abi.encode(vs...).
func refEncode(vs ...refValue) []byte { return refSequence(vs) }

func refUint(v uint64) refWord {
	var w refWord
	binary.BigEndian.PutUint64(w[24:], v)
	return w
}

// refInt encodes v in 256-bit two's complement, covering uint256 and int192.
func refInt(v *big.Int) refWord {
	x := new(big.Int).Set(v)
	if x.Sign() < 0 {
		x.Add(x, new(big.Int).Lsh(big.NewInt(1), 256))
	}
	var w refWord
	x.FillBytes(w[:])
	return w
}

func refBool(b bool) refWord {
	if b {
		return refUint(1)
	}
	return refUint(0)
}

func refAddress(a common.Address) refWord {
	var w refWord
	copy(w[12:], a[:])
	return w
}

func refCall(c *CrossChainCall) refValue {
	return refTuple{
		refUint(uint64(c.RevertNextNCalls)), refBool(c.IsStatic), refUint(c.Gas),
		refAddress(c.SourceAddress), refUint(c.SourceRollupID), refAddress(c.TargetAddress),
		refInt(c.Value), refBytes(c.Data),
	}
}

func refCalls(cs []CrossChainCall) refArray {
	out := make(refArray, len(cs))
	for i := range cs {
		out[i] = refCall(&cs[i])
	}
	return out
}

func refExpectedCalls(es []ExpectedCall) refArray {
	out := make(refArray, len(es))
	for i := range es {
		e := &es[i]
		out[i] = refTuple{
			refWord(e.Hash), refCalls(e.Calls), refWord(e.RevertedOrStaticRollingHash),
			refBool(e.Success), refBytes(e.ReturnData),
		}
	}
	return out
}

func refExpectedRoots(rs []ExpectedRootPerRollup) refArray {
	out := make(refArray, len(rs))
	for i := range rs {
		out[i] = refTuple{refUint(rs[i].RollupID), refWord(rs[i].Root)}
	}
	return out
}

func refEntry(e *ExecutionEntry) refValue {
	updates := make(refArray, len(e.RollupUpdates))
	for i := range e.RollupUpdates {
		u := &e.RollupUpdates[i]
		updates[i] = refTuple{refUint(u.RollupID), refInt(u.EtherDelta), refWord(u.CurrentRoot), refWord(u.NewRoot)}
	}
	return refTuple{
		updates, refWord(e.ProxyEntryHash), refCalls(e.L2ToL1Calls), refExpectedCalls(e.ExpectedL1ToL2Calls),
		refWord(e.RollingHash), refUint(e.DestinationRollupID), refBool(e.Success), refBytes(e.ReturnData),
	}
}

func refStaticEntry(e *StaticExecutionEntry) refValue {
	return refTuple{
		refExpectedRoots(e.ExpectedRoots), refWord(e.ProxyEntryHash), refCalls(e.L2ToL1Calls),
		refWord(e.RollingHash), refUint(e.DestinationRollupID), refBool(e.Success), refBytes(e.ReturnData),
	}
}

func refBatch(b *ProofSystemBatch) refValue {
	entries := make(refArray, len(b.Entries))
	for i := range b.Entries {
		entries[i] = refEntry(&b.Entries[i])
	}
	statics := make(refArray, len(b.StaticEntries))
	for i := range b.StaticEntries {
		statics[i] = refStaticEntry(&b.StaticEntries[i])
	}
	proofSystems := make(refArray, len(b.ProofSystems))
	for i, a := range b.ProofSystems {
		proofSystems[i] = refAddress(a)
	}
	rollups := make(refArray, len(b.RollupIDsWithProofSystems))
	for i := range b.RollupIDsWithProofSystems {
		r := &b.RollupIDsWithProofSystems[i]
		indexes := make(refArray, len(r.ProofSystemIndexes))
		for j, x := range r.ProofSystemIndexes {
			indexes[j] = refUint(x)
		}
		rollups[i] = refTuple{refUint(r.RollupID), indexes}
	}
	blobIndices := make(refArray, len(b.BlobIndices))
	for i, x := range b.BlobIndices {
		blobIndices[i] = refInt(x)
	}
	proofs := make(refArray, len(b.Proofs))
	for i, p := range b.Proofs {
		proofs[i] = refBytes(p)
	}
	return refTuple{
		refExpectedRoots(b.ExpectedRootPerRollup), entries, statics,
		refInt(b.ImmediateEntryCount), refInt(b.ImmediateStaticEntryCount),
		proofSystems, rollups, blobIndices, refBytes(b.CallData), proofs,
		refUint(b.BlockNumber), refBool(b.BindMsgSenderInPublicInput),
	}
}

func refL2Entries(es []L2ExecutionEntry) refArray {
	out := make(refArray, len(es))
	for i := range es {
		e := &es[i]
		out[i] = refTuple{
			refWord(e.ProxyEntryHash), refCalls(e.IncomingCalls), refExpectedCalls(e.ExpectedOutgoingCalls),
			refWord(e.RollingHash), refBool(e.Success), refBytes(e.ReturnData),
		}
	}
	return out
}

func refL2StaticEntries(es []L2StaticExecutionEntry) refArray {
	out := make(refArray, len(es))
	for i := range es {
		e := &es[i]
		out[i] = refTuple{
			refInt(e.ExpectedEntryIndex), refWord(e.ProxyEntryHash), refCalls(e.IncomingCalls),
			refWord(e.RollingHash), refBool(e.Success), refBytes(e.ReturnData),
		}
	}
	return out
}
