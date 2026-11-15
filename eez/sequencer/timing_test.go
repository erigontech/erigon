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

package sequencer_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/eez/sequencer"
)

// The timing tests are T-SEQ-1, ported from eez-rollup0
// crates/eez-driver/src/timing.rs at c3db955.

func timing(l1, l2, proof, slack int) sequencer.Timing {
	ms := func(v int) time.Duration { return time.Duration(v) * time.Millisecond }
	return sequencer.Timing{L1: ms(l1), L2: ms(l2), Proof: ms(proof), Slack: ms(slack)}
}

// mainnet: proof+slack 5500 ms, so Future 2, Live 3, trigger at +6 s.
func mainnet() sequencer.Timing { return timing(12_000, 2_000, 4_000, 1_500) }

// chiadoSlow: K 2, proof+slack 1600 ms, so Future 0, Live 1.
func chiadoSlow() sequencer.Timing { return timing(4_000, 2_000, 1_500, 100) }

// chiadoFast: proof+slack 1900 ms, so Future 1, Live 3, trigger at +3 s.
func chiadoFast() sequencer.Timing { return timing(5_000, 1_000, 200, 1_700) }

func slot(live, future uint64) sequencer.Composition {
	return sequencer.Composition{Shape: sequencer.Slot, Live: live, Future: future}
}

func catchup(live uint64) sequencer.Composition {
	return sequencer.Composition{Shape: sequencer.Catchup, Live: live}
}

func TestTimingDerived(t *testing.T) {
	for name, tc := range map[string]struct {
		t                  sequencer.Timing
		k, future, live    uint64
		window, submission time.Duration
	}{
		"mainnet":     {mainnet(), 6, 2, 3, 6 * time.Second, 10_500 * time.Millisecond},
		"chiado-slow": {chiadoSlow(), 2, 0, 1, 2 * time.Second, 3_900 * time.Millisecond},
		"chiado-fast": {chiadoFast(), 5, 1, 3, 3 * time.Second, 3_300 * time.Millisecond},
	} {
		require.NoError(t, tc.t.Validate(), name)
		require.Equal(t, tc.k, tc.t.K(), name)
		require.Equal(t, tc.future, tc.t.Future(), name)
		require.Equal(t, tc.live, tc.t.Live(), name)
		require.Equal(t, tc.window, tc.t.ProofWindowOpen(), name)
		require.Equal(t, tc.submission, tc.t.SubmissionDeadline(), name)
	}
}

func TestTimingRejected(t *testing.T) {
	for name, tc := range map[string]struct {
		t    sequencer.Timing
		want string
	}{
		"non-integer K":           {timing(12_000, 5_000, 2_000, 100), "not a multiple"},
		"sub-second L2":           {timing(5_000, 2_500, 2_000, 100), "whole number of seconds"},
		"K below 2":               {timing(2_000, 2_000, 500, 100), "K = 1"},
		"proof+slack fills L1":    {timing(12_000, 2_000, 10_000, 3_000), "below L1 block time"},
		"proof+slack past Future": {timing(4_000, 2_000, 3_000, 100), "exceed the 1 L2 blocks"},
		"zero L1":                 {timing(0, 2_000, 4_000, 100), "positive"},
		"zero L2":                 {timing(12_000, 0, 4_000, 100), "positive"},
		"zero proof":              {timing(12_000, 2_000, 0, 100), "positive"},
		"K past MaxCatchup":       {timing(600_000, 1_000, 1_000, 100), "K = 600"},
	} {
		err := tc.t.Validate()
		require.ErrorIs(t, err, sequencer.ErrTiming, name)
		require.ErrorContains(t, err, tc.want, name)
	}
}

// TestTimingOffGridRealigns: after an off-grid settlement the cursor sits one
// below the grid; the next trigger still ends on it, the range longer.
func TestTimingOffGridRealigns(t *testing.T) {
	tm := mainnet()
	k := tm.K()
	head, sync := uint64(174_197), uint64(174_198)
	require.NotZero(t, head%k)

	produced := tm.Compose(head, sync, 64).Blocks()
	require.Positive(t, produced)
	require.Equal(t, sync, head+produced)
	require.Zero(t, (head+produced)%k)

	// The target is absolute, so the span stretches to it instead of
	// keeping the offset.
	far := tm.Compose(head, sync+k, sequencer.MaxCatchup).Blocks()
	require.Equal(t, sync+k, head+far)
	require.Equal(t, k+1, far)

	require.Equal(t, sync+k, sync+tm.Compose(sync, sync+k, 64).Blocks())
}

// TestTimingCatchupOnGrid: a Catchup's Sync block is on the grid, and a head
// one higher would shift it, which is why production targets the height.
func TestTimingCatchupOnGrid(t *testing.T) {
	tm := mainnet()
	k := tm.K()
	sync := uint64(174_198)
	require.Zero(t, sync%k)
	checked := 0
	for head := sync - 400; head < sync-1; head++ {
		c := tm.Compose(head, sync, 64)
		if c.Shape != sequencer.Catchup {
			continue
		}
		require.Zero(t, (head+c.Live+1)%k, "head %d", head)
		require.NotZero(t, (head+1+c.Live+1)%k, "head %d", head)
		checked++
	}
	require.Positive(t, checked)
}

func TestTimingCompose(t *testing.T) {
	m := mainnet()
	for name, tc := range map[string]struct {
		t                  sequencer.Timing
		head, sync, budget uint64
		want               sequencer.Composition
	}{
		// Future 2, so the Live blocks end at 3.
		"steady at live end":         {m, 3, 6, sequencer.MaxCatchup, slot(0, 2)},
		"steady one live behind":     {m, 2, 6, sequencer.MaxCatchup, slot(1, 2)},
		"steady at slot start":       {m, 0, 6, sequencer.MaxCatchup, slot(3, 2)},
		"catchup within cap":         {m, 0, 12, sequencer.MaxCatchup, catchup(11)},
		"catchup snapped to cap":     {m, 0, 318, sequencer.MaxCatchup, catchup(299)},
		"converges to slot":          {m, 12, 18, sequencer.MaxCatchup, slot(3, 2)},
		"chiado-slow slot start":     {chiadoSlow(), 0, 2, sequencer.MaxCatchup, slot(1, 0)},
		"chiado-fast slot start":     {chiadoFast(), 0, 5, sequencer.MaxCatchup, slot(3, 1)},
		"idle at sync":               {m, 6, 6, sequencer.MaxCatchup, sequencer.Composition{}},
		"idle past sync":             {m, 7, 6, sequencer.MaxCatchup, sequencer.Composition{}},
		"idle when budget below K":   {m, 60, 6_000, 4, sequencer.Composition{}},
		"remainder inside future":    {m, 5, 6, sequencer.MaxCatchup, slot(0, 0)},
		"budget bounds catchup room": {m, 0, 6_000, 64, catchup(59)},
	} {
		require.Equal(t, tc.want, tc.t.Compose(tc.head, tc.sync, tc.budget), name)
	}
}

func TestTimingCatchupFarBehind(t *testing.T) {
	tm := mainnet()
	k := tm.K()
	sync := uint64(6_000)
	for _, head := range []uint64{0, 1, 5, 100, 137, 5_000, 5_699} {
		c := tm.Compose(head, sync, sequencer.MaxCatchup)
		if c.Shape != sequencer.Catchup {
			continue
		}
		terminal := head + c.Live + 1
		require.Zero(t, terminal%k, "head %d", head)
		require.Greater(t, terminal, head)
		require.LessOrEqual(t, terminal-head, uint64(sequencer.MaxCatchup))
		require.LessOrEqual(t, terminal, sync)
	}
}

// TestTimingCatchupGenesisOffset: with a genesis off the L1 slot boundary the
// Sync heights carry a residue mod K, which a Catchup keeps.
func TestTimingCatchupGenesisOffset(t *testing.T) {
	tm := mainnet()
	k := tm.K()
	sync := uint64(317)
	c := tm.Compose(0, sync, sequencer.MaxCatchup)
	require.Equal(t, sequencer.Catchup, c.Shape)
	terminal := c.Live + 1
	require.Equal(t, sync%k, terminal%k)
	require.Zero(t, (sync-terminal)%k)
}

func TestTimingCatchupWithinBudget(t *testing.T) {
	tm := mainnet()
	c := tm.Compose(0, 6_000, 64)
	require.Equal(t, sequencer.Catchup, c.Shape)
	require.LessOrEqual(t, c.Live+1, uint64(64))
	require.Zero(t, (c.Live+1)%tm.K())
	for _, budget := range []uint64{6, 7, 64, 128, 299, 300} {
		if c := tm.Compose(0, 10_000, budget); c.Shape == sequencer.Catchup {
			require.LessOrEqual(t, c.Live+1, budget, "budget %d", budget)
		}
	}
}

func TestTimingChunkBoundary(t *testing.T) {
	tm := mainnet()
	k := tm.K()
	sync := uint64(174_198)
	for cursor := sync - 500; cursor < sync-10; cursor++ {
		for _, cap := range []uint64{6, 12, 60, 300} {
			if b, ok := tm.ChunkBoundary(cursor, sync, cap); ok {
				require.Zero(t, (sync-b)%k)
				require.True(t, b > cursor && b < sync)
			}
		}
	}
	// 6005 is a genesis off the slot boundary.
	for _, sync := range []uint64{6_000, 6_005} {
		for _, cursor := range []uint64{0, 1, 7, 137, 5_000} {
			b, ok := tm.ChunkBoundary(cursor, sync, sequencer.MaxCatchup)
			require.True(t, ok, "cursor %d", cursor)
			require.Equal(t, sync%k, b%k)
			require.True(t, b > cursor && b < sync)
			require.LessOrEqual(t, b-cursor, uint64(sequencer.MaxCatchup))
		}
	}
	// The backlog fits the cap: steady emission handles it.
	_, ok := tm.ChunkBoundary(100, 400, 300)
	require.False(t, ok)
	_, ok = tm.ChunkBoundary(100, 100, 300)
	require.False(t, ok)
	// The next grid height above 97 is 102, out of reach for a cap of 3; a
	// cap of K always reaches one.
	_, ok = tm.ChunkBoundary(97, 6_000, 3)
	require.False(t, ok)
	b, ok := tm.ChunkBoundary(97, 6_000, 6)
	require.True(t, ok)
	require.Equal(t, uint64(102), b)
}

// TestTimingGenesisOnTheL1Grid: a genesis must sit on the slot grid of L1,
// which places a Sync position every K blocks on an L1 slot.
func TestTimingGenesisOnTheL1Grid(t *testing.T) {
	tm := mainnet()
	for _, c := range []struct {
		genesis, l1 uint64
		ok          bool
	}{
		{1_000, 1_000, true},
		{1_000, 1_036, true},
		{1_036, 1_000, true},
		{1_002, 1_000, false},
		{1_000, 1_030, false},
	} {
		err := tm.CheckGenesis(c.genesis, c.l1)
		if c.ok {
			require.NoError(t, err, "genesis %d, L1 %d", c.genesis, c.l1)
		} else {
			require.ErrorIs(t, err, sequencer.ErrTiming, "genesis %d, L1 %d", c.genesis, c.l1)
		}
	}
}
