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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// The cases port the attribution tests of eez-rollupX a50c326
// crates/eez-l1/src/scan.rs.

func root(b byte) common.Hash { return common.Hash{31: b} }

func chainOf(current common.Hash, roots []common.Hash, success, effect []bool) *claimedChain {
	c := &claimedChain{current: current, roots: roots, success: success, effect: effect}
	for i := range roots {
		c.entries = append(c.entries, i)
	}
	if c.success == nil {
		for range roots {
			c.success = append(c.success, true)
		}
	}
	if c.effect == nil {
		for i := range roots {
			c.effect = append(c.effect, i > 0)
		}
	}
	return c
}

// run attributes observed and returns (start, len, producing skip, take),
// or none.
func run(t *testing.T, c *claimedChain, observed ...common.Hash) *rollupXRun {
	t.Helper()
	r, ok := c.attribute(observed)
	if !ok {
		return nil
	}
	return &r
}

func producing(r *rollupXRun) [2]int {
	if r.start == 0 {
		return [2]int{0, r.len - 1}
	}
	return [2]int{r.start - 1, r.len}
}

func TestRollupXAttribution(t *testing.T) {
	a, b, c, d := root(0x0a), root(0x0b), root(0x0c), root(0x0d)
	t.Run("empty window", func(t *testing.T) {
		require.Nil(t, run(t, chainOf(common.Hash{}, []common.Hash{c}, nil, nil)))
		require.Nil(t, run(t, chainOf(common.Hash{}, []common.Hash{c, b}, nil, nil)))
	})
	t.Run("partial consumption", func(t *testing.T) {
		r := run(t, chainOf(common.Hash{}, []common.Hash{b, c, d}, nil, nil), b, c)
		require.Equal(t, rollupXRun{start: 0, len: 2, finalState: c}, *r)
	})
	t.Run("full consumption", func(t *testing.T) {
		r := run(t, chainOf(common.Hash{}, []common.Hash{b, c}, nil, nil), b, c)
		require.Equal(t, rollupXRun{start: 0, len: 2, finalState: c}, *r)
	})
	t.Run("second composer resumes after the first hop", func(t *testing.T) {
		r := run(t, chainOf(a, []common.Hash{b}, nil, nil), b)
		require.Equal(t, rollupXRun{start: 0, len: 1, finalState: b, entryState: a}, *r)
		r = run(t, chainOf(a, []common.Hash{b, c}, nil, nil), c)
		require.Equal(t, rollupXRun{start: 1, len: 1, finalState: c, entryState: b}, *r)
		require.Equal(t, [2]int{0, 1}, producing(r))
	})
	t.Run("anchor skipped", func(t *testing.T) {
		r := run(t, chainOf(common.Hash{}, []common.Hash{b, c, d}, nil, nil), c, d)
		require.Equal(t, rollupXRun{start: 1, len: 2, finalState: d, entryState: b}, *r)
		require.Equal(t, [2]int{0, 2}, producing(r))
	})
	t.Run("resume mid-chain", func(t *testing.T) {
		r := run(t, chainOf(common.Hash{}, []common.Hash{b, c, d}, nil, nil), d)
		require.Equal(t, rollupXRun{start: 2, len: 1, finalState: d, entryState: c}, *r)
		require.Equal(t, [2]int{1, 1}, producing(r))
		roots := []common.Hash{root(0x0b), root(0x0c), root(0x0d), root(0x0e), root(0x0f)}
		r = run(t, chainOf(common.Hash{}, roots, nil, nil), roots[3:]...)
		require.Equal(t, [2]int{3, 2}, [2]int{r.start, r.len})
		require.Equal(t, [2]int{2, 2}, producing(r))
	})
	t.Run("positional matching", func(t *testing.T) {
		r := run(t, chainOf(common.Hash{}, []common.Hash{b, b, c}, nil, nil), b, b)
		require.Equal(t, rollupXRun{start: 0, len: 2, finalState: b}, *r)
		require.Nil(t, run(t, chainOf(common.Hash{}, []common.Hash{b, c}, nil, nil), c, b))
		require.Nil(t, run(t, chainOf(common.Hash{}, []common.Hash{b}, nil, nil), root(0x7a)))
	})
	t.Run("anchor carries a failed effect", func(t *testing.T) {
		r := run(t, chainOf(common.Hash{}, []common.Hash{b, b}, []bool{true, false}, nil), b)
		require.Equal(t, [2]int{0, 2}, [2]int{r.start, r.len})
		require.Equal(t, [2]int{0, 1}, producing(r))
	})
	t.Run("carriers expand over failed effects", func(t *testing.T) {
		ch := chainOf(common.Hash{}, []common.Hash{a, a, a, b, b, c}, []bool{true, false, false, true, false, true}, nil)
		r := run(t, ch, a, b, c)
		require.Equal(t, [2]int{0, 6}, [2]int{r.start, r.len})
		require.Equal(t, [2]int{0, 5}, producing(r))
		r = run(t, ch, b, c)
		require.Equal(t, [2]int{3, 3}, [2]int{r.start, r.len})
		require.Equal(t, [2]int{2, 3}, producing(r))
		require.Equal(t, a, r.entryState)
	})
	t.Run("invalid effect masks", func(t *testing.T) {
		roots := []common.Hash{root(1), root(2), root(3), root(4)}
		require.Nil(t, run(t, chainOf(common.Hash{}, roots[:2], nil, []bool{true, true}), roots[:2]...))
		require.Nil(t, run(t, chainOf(common.Hash{}, roots, nil, []bool{true, true, true, true}), roots...))
		require.Nil(t, run(t, chainOf(common.Hash{}, roots, nil, []bool{false, true, true, true}), roots[0], roots[2], roots[3]))
	})
	t.Run("skipped outbounds", func(t *testing.T) {
		roots := []common.Hash{root(1), root(2), root(3), root(4)}
		for committed := range 3 {
			persisted := make([]bool, 4)
			resumedOK := make([]bool, 4)
			for i := range 4 {
				persisted[i] = i <= committed
				resumedOK[i] = i > committed
			}
			r := run(t, chainOf(common.Hash{}, roots, persisted, nil), roots[:committed+1]...)
			require.Equal(t, [2]int{0, committed}, producing(r))
			require.Equal(t, roots[committed], r.finalState)
			r = run(t, chainOf(common.Hash{}, roots, resumedOK, nil), roots[committed+1:]...)
			require.Equal(t, [2]int{committed, 3 - committed}, producing(r))
			require.Equal(t, roots[committed], r.entryState)
			require.Equal(t, roots[3], r.finalState)
		}
	})
}

// TestRollupXLookups is inbound_lookups_preserve_event_positions_without_
// producing_effects: lookups keep their optional log positions and produce
// nothing.
func TestRollupXLookups(t *testing.T) {
	const rollupID = 17
	before, anchor, endpoint := root(1), root(2), root(3)
	update := func(cur, next common.Hash) []RollupUpdate {
		return []RollupUpdate{{RollupID: rollupID, EtherDelta: new(big.Int), CurrentRoot: cur, NewRoot: next}}
	}
	lookup := func(r common.Hash, success bool) ExecutionEntry {
		proxy := root(0xab)
		seed := L1EntrySeed([]RollupUpdate{{RollupID: rollupID, CurrentRoot: r}}, proxy)
		return ExecutionEntry{DestinationRollupID: rollupID, ProxyEntryHash: proxy, RollupUpdates: update(r, r), RollingHash: seed.Sum(), Success: success}
	}
	for _, success := range []bool{false, true} {
		b := &ProofSystemBatch{Entries: []ExecutionEntry{
			{Success: true, RollupUpdates: update(before, anchor)},
			lookup(anchor, success),
			{Success: true, RollupUpdates: update(anchor, endpoint)},
			lookup(endpoint, success),
		}}
		ch := newClaimedChain(b, rollupID, nil)
		require.Equal(t, []bool{false, false, true, false}, ch.effect)
		full := run(t, ch, anchor, endpoint)
		require.Equal(t, rollupXRun{start: 0, len: 2, finalState: endpoint, entryState: before}, *full)
		require.Equal(t, [2]int{0, 0}, producing(run(t, ch, anchor)))
		resumed := run(t, ch, endpoint)
		require.Equal(t, rollupXRun{start: 1, len: 1, finalState: endpoint, entryState: anchor}, *resumed)
		if !success {
			continue
		}
		for _, observed := range [][]common.Hash{{anchor, anchor, endpoint}, {anchor, endpoint, endpoint}, {anchor, anchor, endpoint, endpoint}} {
			require.Equal(t, full, run(t, ch, observed...))
		}
		require.Equal(t, [2]int{0, 0}, producing(run(t, ch, anchor, anchor)))
		require.Equal(t, resumed, run(t, ch, endpoint, endpoint))
		require.Nil(t, run(t, ch, anchor, anchor, anchor))
	}
	l := lookup(anchor, true)
	require.True(t, IsLookupOnly(&l, rollupID))
	l.RollingHash = common.Hash{}
	require.False(t, IsLookupOnly(&l, rollupID))
}

func TestRollupXSkippedImmediate(t *testing.T) {
	const rollupID = 17
	current, anchor, effect := root(9), root(0x0a), root(0x0b)
	b := &ProofSystemBatch{Entries: []ExecutionEntry{
		{Success: true, RollupUpdates: []RollupUpdate{{RollupID: rollupID, CurrentRoot: current, NewRoot: anchor}}},
		{Success: true, RollupUpdates: []RollupUpdate{{RollupID: rollupID, CurrentRoot: anchor, NewRoot: effect}}},
	}}
	ch := newClaimedChain(b, rollupID, []uint64{1})
	require.Equal(t, current, ch.current)
	require.Equal(t, []bool{true, false}, ch.success)
	require.Equal(t, []bool{false, true}, ch.effect)
	r := run(t, ch, anchor)
	require.Equal(t, [2]int{0, 1}, [2]int{r.start, r.len})
	require.Equal(t, [2]int{0, 0}, producing(r))
	carried := run(t, chainOf(current, []common.Hash{anchor, anchor}, ch.success, nil), anchor)
	require.Equal(t, [2]int{0, 1}, producing(carried))
}
