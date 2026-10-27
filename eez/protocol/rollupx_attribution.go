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
	"math"
	"math/big"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// IsRootOnly reports whether e is the inert anchor shape of rollupX: one
// zero-delta update of rollupID, no proxy entry hash, calls or return data,
// and the rolling hash of an entry that runs nothing.
func IsRootOnly(e *ExecutionEntry, rollupID uint64) bool {
	if len(e.RollupUpdates) != 1 {
		return false
	}
	u := &e.RollupUpdates[0]
	seed := L1EntrySeed([]RollupUpdate{{RollupID: rollupID, CurrentRoot: u.CurrentRoot}}, common.Hash{})
	return e.ProxyEntryHash == (common.Hash{}) && len(e.L2ToL1Calls) == 0 && len(e.ExpectedL1ToL2Calls) == 0 &&
		e.Success && len(e.ReturnData) == 0 && e.DestinationRollupID == rollupID &&
		u.RollupID == rollupID && zeroDelta(u.EtherDelta) && e.RollingHash == seed.Sum()
}

// IsLookupOnly reports whether e is a rollupX L1-origin lookup, a deferred
// entry that changes no L2 state: one zero-delta update of rollupID from a
// root to itself, a proxy entry hash, no calls, and the rolling hash of an
// entry that runs nothing.
func IsLookupOnly(e *ExecutionEntry, rollupID uint64) bool {
	if len(e.RollupUpdates) != 1 {
		return false
	}
	u := &e.RollupUpdates[0]
	seed := L1EntrySeed([]RollupUpdate{{RollupID: rollupID, CurrentRoot: u.CurrentRoot}}, e.ProxyEntryHash)
	return e.ProxyEntryHash != (common.Hash{}) && len(e.L2ToL1Calls) == 0 && len(e.ExpectedL1ToL2Calls) == 0 &&
		e.DestinationRollupID == rollupID && u.RollupID == rollupID && u.CurrentRoot == u.NewRoot &&
		zeroDelta(u.EtherDelta) && e.RollingHash == seed.Sum()
}

func zeroDelta(d *big.Int) bool { return d == nil || d.Sign() == 0 }

// claimedChain is the commitment chain a rollupX batch claims for one
// rollup: one step per update of it, in entry order.
type claimedChain struct {
	current common.Hash
	// entries holds each step's entry index.
	entries []int
	roots   []common.Hash
	success []bool
	// effect marks the steps that produce L2 effects: every step after the
	// first except lookups.
	effect []bool
}

func newClaimedChain(b *ProofSystemBatch, rollupID uint64, skipped []uint64) *claimedChain {
	c := &claimedChain{}
	for i := range b.Entries {
		e := &b.Entries[i]
		lookup := IsLookupOnly(e, rollupID)
		for _, u := range e.RollupUpdates {
			if u.RollupID != rollupID {
				continue
			}
			if len(c.roots) == 0 {
				c.current = u.CurrentRoot
			}
			c.effect = append(c.effect, len(c.roots) > 0 && !lookup)
			c.entries = append(c.entries, i)
			c.roots = append(c.roots, u.NewRoot)
			c.success = append(c.success, e.Success && !slices.Contains(skipped, uint64(i)))
		}
	}
	return c
}

// carrier is a step whose successful L1 frame emits its root, with the
// effects whose checkpoints it carries. A failed step whose root equals its
// predecessor's emits nothing and folds into the preceding carrier.
type carrier struct {
	step                   int
	root                   common.Hash
	effectStart, effectEnd int
	// optional marks a lookup, whose no-op root rolls back with its source
	// frame.
	optional bool
}

func (c *claimedChain) carriers() []carrier {
	if len(c.roots) == 0 || c.effect[0] {
		return nil
	}
	effectEnds := make([]int, len(c.roots))
	var effects int
	for i, e := range c.effect {
		if e {
			effects++
		}
		effectEnds[i] = effects
	}
	var starts []int
	for i := 1; i < len(c.roots); i++ {
		if c.success[i] || c.roots[i] != c.roots[i-1] {
			starts = append(starts, i)
		}
	}
	end := func(k int) int {
		if k < len(starts) {
			return effectEnds[starts[k]-1]
		}
		return effects
	}
	out := []carrier{{step: 0, root: c.roots[0], effectStart: 0, effectEnd: end(0)}}
	for k, i := range starts {
		out = append(out, carrier{step: i, root: c.roots[i], effectStart: effectEnds[i-1], effectEnd: end(k + 1), optional: !c.effect[i]})
	}
	return out
}

// rollupXRun is the run of a batch's steps L1 applied, in the virtual
// [anchor | effects] index space: len steps from start.
type rollupXRun struct {
	start, len             int
	finalState, entryState common.Hash
}

// attribute locates observed, the roots emitted in the batch's window, as a
// consecutive run of carriers in which optional carriers may be passed over.
// It returns the earliest such run, none when nothing matches.
func (c *claimedChain) attribute(observed []common.Hash) (rollupXRun, bool) {
	carriers := c.carriers()
	if len(observed) == 0 || len(carriers) == 0 || len(observed) > len(carriers) {
		return rollupXRun{}, false
	}
	// starts[i] is the earliest start of a run that consumed the observed
	// prefix and visits carrier i next; -1 is none.
	starts := make([]int, len(carriers)+1)
	for i := range starts {
		starts[i] = i
	}
	for _, root := range observed {
		next := make([]int, len(carriers)+1)
		for i := range next {
			next[i] = -1
		}
		for i := range carriers {
			start := starts[i]
			if start < 0 {
				continue
			}
			if carriers[i].optional && (starts[i+1] < 0 || start < starts[i+1]) {
				starts[i+1] = start
			}
			if carriers[i].root == root {
				next[i+1] = start
			}
		}
		starts = next
	}
	first, last := -1, -1
	for end, start := range starts {
		if start >= 0 && (first < 0 || start < first) {
			first, last = start, end
		}
	}
	if first < 0 {
		return rollupXRun{}, false
	}
	f, l := carriers[first], carriers[last-1]
	r := rollupXRun{finalState: observed[len(observed)-1]}
	if f.step == 0 {
		r.start, r.len, r.entryState = 0, 1+l.effectEnd, c.current
	} else {
		r.start, r.len, r.entryState = f.effectStart+1, l.effectEnd-f.effectStart, c.roots[f.step-1]
	}
	return r, true
}

// settlement converts run into the applied entries of b: the anchor when
// the run starts with it, then the effect entries it covers. Outbound effects
// are the entries before the immediate count, inbound ones the entries after
// it; ordinals rank them within their direction.
func (c *claimedChain) settlement(b *ProofSystemBatch, r rollupXRun) Settlement {
	s := Settlement{FinalState: r.finalState, EntryState: r.entryState}
	immediate := uint64(math.MaxUint64)
	if b.ImmediateEntryCount.IsUint64() {
		immediate = b.ImmediateEntryCount.Uint64()
	}
	skip, take := r.start-1, r.len
	if r.start == 0 {
		s.Applied = append(s.Applied, AppliedEntry{Index: c.entries[0], Role: RoleAnchor})
		skip, take = 0, r.len-1
	}
	var effect, outbound, inbound int
	for step, e := range c.effect {
		if !e {
			continue
		}
		index := c.entries[step]
		role, ordinal := RoleInbound, inbound
		if uint64(index) < immediate {
			role, ordinal = RoleOutbound, outbound
			outbound++
		} else {
			inbound++
		}
		if effect >= skip && effect < skip+take {
			s.Applied = append(s.Applied, AppliedEntry{Index: index, Role: role, Ordinal: ordinal})
		}
		effect++
	}
	return s
}

// AttributeRollupXBlock attributes the registry logs of one L1 block to the
// batches posted in it, under rollupX; logs must carry TxIndex and Index. A
// batch whose BatchPosted lists rollupID owns the roots rollupID's
// L2ExecutionPerformed logs report from its own transaction up to the next
// such batch's; its L2TxSkipped logs mark entries that did not run. The
// result holds one settlement per batch, empty when the batch does not list
// rollupID or L1 applied none of its steps. Of the anchor-only batches that
// settled the same root again in the block, only the first keeps its
// settlement, and none does beside a batch that settled more.
func AttributeRollupXBlock(batches []PostedBatch, logs []*types.Log, rollupID uint64) ([]Settlement, error) {
	type root struct {
		tx, index uint64
		root      common.Hash
	}
	var roots []root
	skipped := map[uint64][]uint64{}
	for _, l := range logs {
		if len(l.Topics) == 0 {
			continue
		}
		switch l.Topics[0] {
		case L2ExecutionPerformedTopic:
			e, err := ParseL2ExecutionPerformed(l)
			if err != nil {
				return nil, err
			}
			if e.RollupID == rollupID {
				roots = append(roots, root{tx: uint64(l.TxIndex), index: uint64(l.Index), root: e.NewRoot})
			}
		case L2TxSkippedTopic:
			e, err := ParseL2TxSkipped(l)
			if err != nil {
				return nil, err
			}
			if e.EntryIndex.IsUint64() {
				skipped[uint64(l.TxIndex)] = append(skipped[uint64(l.TxIndex)], e.EntryIndex.Uint64())
			}
		}
	}
	slices.SortFunc(roots, func(a, b root) int { return cmp.Or(cmp.Compare(a.tx, b.tx), cmp.Compare(a.index, b.index)) })

	var boundaries []uint64
	for _, p := range batches {
		if p.ListsRollup {
			boundaries = append(boundaries, p.TxIndex)
		}
	}
	slices.Sort(boundaries)

	out := make([]Settlement, len(batches))
	chains := make([]*claimedChain, len(batches))
	for i, p := range batches {
		if !p.ListsRollup {
			continue
		}
		end := uint64(math.MaxUint64)
		if j, _ := slices.BinarySearch(boundaries, p.TxIndex+1); j < len(boundaries) {
			end = boundaries[j]
		}
		var observed []common.Hash
		for _, r := range roots {
			if r.tx >= p.TxIndex && r.tx < end {
				observed = append(observed, r.root)
			}
		}
		chains[i] = newClaimedChain(p.Batch, rollupID, skipped[p.TxIndex])
		if r, ok := chains[i].attribute(observed); ok {
			out[i] = chains[i].settlement(p.Batch, r)
		}
	}

	// An anchor-only batch claiming its current root again is a no-op.
	noop := func(i int) bool {
		c := chains[i]
		return c != nil && len(c.roots) == 1 && c.roots[0] == c.current
	}
	settledMore := map[common.Hash]bool{}
	firstNoop := map[common.Hash]int{}
	for i := range batches {
		if len(out[i].Applied) == 0 {
			continue
		}
		key := chains[i].current
		if !noop(i) {
			settledMore[key] = true
			continue
		}
		if f, ok := firstNoop[key]; !ok || batches[i].TxIndex < batches[f].TxIndex {
			firstNoop[key] = i
		}
	}
	for i := range batches {
		if len(out[i].Applied) == 0 || !noop(i) {
			continue
		}
		if key := chains[i].current; settledMore[key] || firstNoop[key] != i {
			out[i] = Settlement{}
		}
	}
	return out, nil
}
