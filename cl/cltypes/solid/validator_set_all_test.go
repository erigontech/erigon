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

package solid

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func allTestSet(t testing.TB, count int, progressive bool) *liveSet {
	t.Helper()
	rng := rand.New(rand.NewSource(int64(count)*2 + 1))
	live := newLiveSet(1<<20, progressive)
	for range count {
		live.appendRandom(rng)
	}
	return live
}

func collectAll(set *ValidatorSet, from, to int) (indices []int, views []ValidatorView) {
	for i, v := range set.All(from, to) {
		indices = append(indices, i)
		views = append(views, v)
	}
	return indices, views
}

func TestValidatorSetAllYieldsViewsInIndexOrder(t *testing.T) {
	for _, progressive := range []bool{false, true} {
		for _, tc := range []struct {
			name     string
			count    int
			from, to int
		}{
			{"empty", 0, 0, 0},
			{"full single chunk", 100, 0, 100},
			{"full multiple chunks", 1000, 0, 1000},
			{"from equals to", 100, 40, 40},
			{"from equals to at the end", 100, 100, 100},
			{"inside one chunk", 1000, 10, 200},
			{"crossing a chunk boundary", 1000, 200, 300},
			{"crossing several chunk boundaries", 1000, 100, 900},
			{"starting on a chunk boundary", 1000, 256, 512},
			{"starting at the progressive offset", 1000, 85, 341},
			{"ending on a chunk boundary", 1000, 0, 256},
			{"single element", 1000, 500, 501},
		} {
			t.Run(fmt.Sprintf("progressive=%v/%s", progressive, tc.name), func(t *testing.T) {
				live := allTestSet(t, tc.count, progressive)
				indices, views := collectAll(live.set, tc.from, tc.to)
				require.Len(t, indices, tc.to-tc.from)
				for n, i := range indices {
					require.Equal(t, tc.from+n, i)
					want := live.model.vals[i]
					got, err := views[n].EncodeSSZ(nil)
					require.NoError(t, err)
					require.Equal(t, []byte(want), got, "index %d", i)
				}
			})
		}
	}
}

func TestValidatorSetAllPanicsOutsideBounds(t *testing.T) {
	set := allTestSet(t, 100, false).set
	for _, tc := range []struct{ from, to int }{
		{-1, 10},
		{0, 101},
		{50, 40},
		{101, 101},
		{-5, -1},
	} {
		t.Run(fmt.Sprintf("%d_%d", tc.from, tc.to), func(t *testing.T) {
			require.Panics(t, func() {
				for range set.All(tc.from, tc.to) {
				}
			})
		})
	}
	require.PanicsWithValue(t, "ValidatorSet -- All: out of bounds", func() { set.All(0, 101) })
}

func TestValidatorSetAllEarlyBreak(t *testing.T) {
	set := allTestSet(t, 1000, false).set
	visited := 0
	for i := range set.All(0, 1000) {
		visited++
		if i == 300 {
			break
		}
	}
	require.Equal(t, 301, visited)
}

func TestValidatorSetAllDoesNotVisitAppendedValidators(t *testing.T) {
	live := allTestSet(t, 300, false)
	visited := 0
	for range live.set.All(0, live.set.Length()) {
		visited++
		if visited == 1 {
			live.set.Append(NewValidator())
		}
	}
	require.Equal(t, 300, visited)
	require.Equal(t, 301, live.set.Length())
}

func TestValidatorSetAllAllowsWritingTheCurrentIndex(t *testing.T) {
	for _, progressive := range []bool{false, true} {
		for _, shared := range []bool{false, true} {
			t.Run(fmt.Sprintf("progressive=%v/shared=%v", progressive, shared), func(t *testing.T) {
				live := allTestSet(t, 1000, progressive)
				var sibling *liveSet
				if shared {
					sibling = &liveSet{set: NewValidatorSet(1 << 20), model: newValidatorModel(1 << 20)}
					live.copyInto(sibling)
				}
				before := live.model.clone()
				for i, v := range live.set.All(0, live.set.Length()) {
					require.Equal(t, before.vals[i], Validator(v.Clone()), "index %d must still read as before its own write", i)
					live.set.SetEffectiveBalanceForValidatorAtIndex(i, uint64(i)+1)
					live.model.mutate(i, func(val Validator) { val.SetEffectiveBalance(uint64(i) + 1) })
				}
				live.check(t, "after iterating writer")
				if shared {
					sibling.check(t, "frozen sibling")
				}
			})
		}
	}
}

func TestValidatorSetGetAliasesPrivateChunkAndFreezesSharedChunk(t *testing.T) {
	live := allTestSet(t, 10, false)
	view := live.set.Get(3)
	live.set.SetEffectiveBalanceForValidatorAtIndex(3, 77)
	require.Equal(t, uint64(77), view.EffectiveBalance(), "a private chunk is written in place")

	sibling := NewValidatorSet(1 << 20)
	live.set.CopyTo(sibling)
	view = sibling.Get(3)
	live.set.SetEffectiveBalanceForValidatorAtIndex(3, 78)
	sibling.SetEffectiveBalanceForValidatorAtIndex(4, 79)
	require.Equal(t, uint64(77), view.EffectiveBalance(), "a view of a shared chunk never changes")
	require.Equal(t, uint64(77), sibling.Get(3).EffectiveBalance())
	require.Equal(t, uint64(78), live.set.Get(3).EffectiveBalance())
}

func TestValidatorSetRangeMatchesAll(t *testing.T) {
	live := allTestSet(t, 700, true)
	var visited []int
	live.set.Range(func(i int, v ValidatorView, length int) bool {
		require.Equal(t, 700, length)
		require.Equal(t, live.model.vals[i], Validator(v.Clone()))
		visited = append(visited, i)
		return i < 399
	})
	require.Len(t, visited, 400)
}

func TestValidatorSetAccessAllocatesNothing(t *testing.T) {
	set := allTestSet(t, 600, false).set
	var sink uint64
	require.Zero(t, testing.AllocsPerRun(10, func() {
		for i := range set.Length() {
			sink += set.Get(i).EffectiveBalance()
		}
	}))
	require.Zero(t, testing.AllocsPerRun(10, func() {
		for _, v := range set.All(0, set.Length()) {
			sink += v.EffectiveBalance()
		}
	}))
	require.Zero(t, testing.AllocsPerRun(10, func() {
		for i := range set.Length() {
			pubkey := set.Get(i).PublicKey()
			sink += uint64(pubkey[0])
		}
	}))
	_ = sink
}
