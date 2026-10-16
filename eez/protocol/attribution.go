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
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// ErrAttribution reports settlement logs that do not match the batch they
// are attributed to.
var ErrAttribution = errors.New("settlement evidence does not match the batch")

// EntryRole is what an applied entry contributes to the Sync block.
type EntryRole uint8

const (
	// RoleAnchor moves the state commitment and emits no system transaction.
	RoleAnchor EntryRole = iota
	// RoleOutbound settles an L2-to-L1 call.
	RoleOutbound
	// RoleInbound delivers an L1-to-L2 call.
	RoleInbound
)

// AppliedEntry is an entry of a batch that L1 applied.
type AppliedEntry struct {
	// Index is the entry's position in ProofSystemBatch.Entries.
	Index int
	Role  EntryRole
	// Ordinal ranks the entry among the batch's entries of the same direction,
	// which is its position in the DA layout; it is zero for anchors.
	Ordinal int
}

// Settlement is what L1 applied of one batch for one rollup.
type Settlement struct {
	// Applied lists the applied entries that update the rollup, in
	// application order.
	Applied []AppliedEntry
	// FinalState is the root of the last applied entry, L1's commitment.
	FinalState common.Hash
	// EntryState is the root the first applied entry started from: a rival
	// batch's endpoint when the batch resumed mid-chain.
	EntryState common.Hash
}

// Resumed reports that entries applied but not the batch's first.
func (s *Settlement) Resumed() bool { return len(s.Applied) > 0 && s.Applied[0].Index > 0 }

// ConsumedEntry is an ExecutionConsumed log of the rollup.
type ConsumedEntry struct {
	// Slot is entryQueueIndex: a transient-table slot when logged before the
	// batch's BatchPosted, a queue slot otherwise.
	Slot     uint64
	CallHash common.Hash
	// LogIndex is the log's index in its block.
	LogIndex uint64
}

// SettlementEvidence is what L1 emitted for a rollup within one batch's
// window.
type SettlementEvidence struct {
	// Roots are the rollup's L2ExecutionPerformed roots in emission order.
	Roots    []common.Hash
	Consumed []ConsumedEntry
	// Skipped are the entry indexes of the batch's L2TxSkipped logs.
	Skipped []uint64
}

// AttributeSettlement returns the entries of b that L1 applied for rollupID,
// read from the evidence in the batch's window. batchLogIndex is the block log
// index of the batch's BatchPosted. The leading run of immediate entries with
// a zero proxy entry hash ran inline unless skipped; every other applied entry
// is named by a consumption, resolved in the transient table or the queue, and
// must carry the consumed call hash. Applied entries updating rollupID must
// match the observed roots one to one.
func AttributeSettlement(b *ProofSystemBatch, rollupID, batchLogIndex uint64, ev *SettlementEvidence) (*Settlement, error) {
	if len(ev.Roots) == 0 {
		return &Settlement{}, nil
	}
	n := uint64(len(b.Entries))
	immediate := uint64(math.MaxUint64)
	if b.ImmediateEntryCount.IsUint64() {
		immediate = b.ImmediateEntryCount.Uint64()
	}
	runEnd := uint64(0)
	for runEnd < min(immediate, n) && b.Entries[runEnd].ProxyEntryHash == (common.Hash{}) {
		runEnd++
	}
	var queue []uint64
	for i := immediate; i < n; i++ {
		if b.Entries[i].DestinationRollupID == rollupID {
			queue = append(queue, i)
		}
	}

	var applied []uint64
	for i := range runEnd {
		if !slices.Contains(ev.Skipped, i) {
			applied = append(applied, i)
		}
	}
	for _, c := range ev.Consumed {
		var index uint64
		var ok bool
		if c.LogIndex < batchLogIndex {
			index = runEnd + c.Slot
			ok = index >= runEnd && index < min(immediate, n)
		} else if ok = c.Slot < uint64(len(queue)); ok {
			index = queue[c.Slot]
		}
		if !ok || b.Entries[index].ProxyEntryHash != c.CallHash || b.Entries[index].DestinationRollupID != rollupID {
			return nil, fmt.Errorf("%w: consumption of slot %d (call hash %x) at log %d names no entry of rollup %d",
				ErrAttribution, c.Slot, c.CallHash, c.LogIndex, rollupID)
		}
		applied = append(applied, index)
	}
	slices.Sort(applied)
	if len(slices.Compact(slices.Clone(applied))) != len(applied) {
		return nil, fmt.Errorf("%w: an entry applied twice", ErrAttribution)
	}

	roles := classifyEntries(b)
	s := &Settlement{}
	for _, i := range applied {
		if update := rollupUpdate(&b.Entries[i], rollupID); update != nil {
			if len(s.Applied) == 0 {
				s.EntryState = update.CurrentRoot
			}
			a := roles[i]
			a.Index = int(i)
			s.Applied = append(s.Applied, a)
		}
	}
	if len(s.Applied) != len(ev.Roots) {
		return nil, fmt.Errorf("%w: %d applied entries update rollup %d, %d roots observed",
			ErrAttribution, len(s.Applied), rollupID, len(ev.Roots))
	}
	s.FinalState = ev.Roots[len(ev.Roots)-1]
	return s, nil
}

// classifyEntries gives each entry its role and per-direction ordinal. A
// delivery carries a proxy entry hash and no calls, so the hash decides first.
func classifyEntries(b *ProofSystemBatch) []AppliedEntry {
	roles := make([]AppliedEntry, len(b.Entries))
	var outbound, inbound int
	for i := range b.Entries {
		e := &b.Entries[i]
		switch {
		case e.ProxyEntryHash != (common.Hash{}):
			roles[i] = AppliedEntry{Role: RoleInbound, Ordinal: inbound}
			inbound++
		case len(e.L2ToL1Calls) == 0:
			roles[i] = AppliedEntry{Role: RoleAnchor}
		default:
			roles[i] = AppliedEntry{Role: RoleOutbound, Ordinal: outbound}
			outbound++
		}
	}
	return roles
}

func rollupUpdate(e *ExecutionEntry, rollupID uint64) *RollupUpdate {
	for i := range e.RollupUpdates {
		if e.RollupUpdates[i].RollupID == rollupID {
			return &e.RollupUpdates[i]
		}
	}
	return nil
}

// PostedBatch is a batch posted in an L1 block.
type PostedBatch struct {
	Batch   *ProofSystemBatch
	TxIndex uint64
	// LogIndex is the block log index of the batch's BatchPosted.
	LogIndex uint64
}

// AttributeBlock attributes the registry logs of one L1 block to the batches
// posted in it, for rollup rollupID; logs must carry TxIndex and Index. A
// batch that verifies the rollup owns its settlement logs from its own
// transaction up to the next transaction whose batch verifies the rollup and
// so wipes its queue; L2TxSkipped logs belong to the emitting transaction.
// The result holds one settlement per batch, empty when the batch does not
// verify rollupID.
func AttributeBlock(batches []PostedBatch, logs []*types.Log, rollupID uint64) ([]Settlement, error) {
	type positioned struct {
		tx, index uint64
		root      common.Hash
		consumed  *ExecutionConsumed
		skipped   *L2TxSkipped
	}
	var records []positioned
	for _, l := range logs {
		if len(l.Topics) == 0 {
			continue
		}
		r := positioned{tx: uint64(l.TxIndex), index: uint64(l.Index)}
		switch l.Topics[0] {
		case L2ExecutionPerformedTopic:
			e, err := ParseL2ExecutionPerformed(l)
			if err != nil {
				return nil, err
			}
			if e.RollupID != rollupID {
				continue
			}
			r.root = e.NewRoot
		case ExecutionConsumedTopic:
			e, err := ParseExecutionConsumed(l)
			if err != nil {
				return nil, err
			}
			if e.RollupID != rollupID {
				continue
			}
			r.consumed = e
		case L2TxSkippedTopic:
			e, err := ParseL2TxSkipped(l)
			if err != nil {
				return nil, err
			}
			r.skipped = e
		default:
			continue
		}
		records = append(records, r)
	}
	slices.SortFunc(records, func(a, b positioned) int { return cmp.Compare(a.index, b.index) })

	verifies := func(b *ProofSystemBatch) bool {
		return slices.ContainsFunc(b.RollupIDsWithProofSystems, func(r RollupIDWithProofSystems) bool { return r.RollupID == rollupID })
	}
	var boundaries []uint64
	for _, p := range batches {
		if verifies(p.Batch) {
			boundaries = append(boundaries, p.TxIndex)
		}
	}
	slices.Sort(boundaries)

	out := make([]Settlement, len(batches))
	for i, p := range batches {
		if !verifies(p.Batch) {
			continue
		}
		end := uint64(math.MaxUint64)
		if j, _ := slices.BinarySearch(boundaries, p.TxIndex+1); j < len(boundaries) {
			end = boundaries[j]
		}
		var ev SettlementEvidence
		for _, r := range records {
			switch {
			case r.skipped != nil:
				if r.tx == p.TxIndex && r.skipped.EntryIndex.IsUint64() {
					ev.Skipped = append(ev.Skipped, r.skipped.EntryIndex.Uint64())
				}
			case r.tx < p.TxIndex || r.tx >= end:
				// Outside the window.
			case r.consumed != nil:
				if !r.consumed.EntryQueueIndex.IsUint64() {
					return nil, fmt.Errorf("%w: queue index %v", ErrAttribution, r.consumed.EntryQueueIndex)
				}
				ev.Consumed = append(ev.Consumed, ConsumedEntry{Slot: r.consumed.EntryQueueIndex.Uint64(), CallHash: r.consumed.CrossChainCallHash, LogIndex: r.index})
			default:
				ev.Roots = append(ev.Roots, r.root)
			}
		}
		s, err := AttributeSettlement(p.Batch, rollupID, p.LogIndex, &ev)
		if err != nil {
			return nil, fmt.Errorf("batch at tx %d: %w", p.TxIndex, err)
		}
		out[i] = *s
	}
	return out, nil
}
