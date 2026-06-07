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

package commitment

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/length"
)

// storageGroup is one whale account plus a disjoint subset of its storage
// slots — the shape a single concurrent storage-fold worker would own.
type storageGroup struct {
	pk      [][]byte
	updates []Update
}

// buildWhaleStorageGroups splits one whale account's `slots` storage entries
// into `groups` disjoint sub-tries (account + subset). Each group is a valid,
// independent account+storage trie whose per-key processing cost matches a real
// storage-nibble subtree at depth 64; the only difference from the production
// mount is the (negligible) repeated single-account prefix.
func buildWhaleStorageGroups(slots, groups int) []storageGroup {
	rnd := rand.New(rand.NewSource(919273))
	addr := make([]byte, length.Addr)
	rnd.Read(addr)
	a := hex.EncodeToString(addr)

	ubs := make([]*UpdateBuilder, groups)
	for i := range ubs {
		ubs[i] = NewUpdateBuilder()
		ubs[i].Balance(a, rnd.Uint64()+1)
	}
	for i := 0; i < slots; i++ {
		loc := make([]byte, length.Hash)
		rnd.Read(loc)
		val := make([]byte, 32)
		rnd.Read(val)
		ubs[i%groups].Storage(a, hex.EncodeToString(loc), hex.EncodeToString(val))
	}

	out := make([]storageGroup, groups)
	for i := range ubs {
		pk, upd := ubs[i].Build()
		out[i] = storageGroup{pk: pk, updates: upd}
	}
	return out
}

type groupRun struct {
	hph  *HexPatriciaHashed
	upds *Updates
}

// setupGroup builds the (untimed) per-iteration state for one group. Must run on
// the test goroutine (uses require). Each group gets its own MockState/trie so
// concurrent process() calls share no mutable state.
func setupGroup(tb testing.TB, g storageGroup) groupRun {
	ms := NewMockState(tb)
	require.NoError(tb, ms.applyPlainUpdates(g.pk, g.updates))
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	upds := WrapKeyUpdates(tb, ModeDirect, KeyToHexNibbleHash, g.pk, g.updates)
	return groupRun{hph: hph, upds: upds}
}

// process is safe to call from any goroutine (no require/FailNow).
func (r groupRun) process() error {
	_, err := r.hph.Process(context.Background(), r.upds, "", nil, WarmupConfig{})
	return err
}

func setupGroups(tb testing.TB, gs []storageGroup) []groupRun {
	rs := make([]groupRun, len(gs))
	for i := range gs {
		rs[i] = setupGroup(tb, gs[i])
	}
	return rs
}

func closeGroups(rs []groupRun) {
	for _, r := range rs {
		r.upds.Close()
	}
}

// Benchmark_StorageConcurrency measures whether processing one whale account's
// storage concurrently (split into N disjoint sub-tries) beats the single
// serial pass — the headline question for concurrent storage-subtree folding.
//
//	Single            one trie, all slots (the current whale-worker cost)
//	GroupsN-Serial    N sub-tries one after another (isolates split overhead)
//	GroupsN-Parallel  N sub-tries concurrently (the win)
//
// All timed regions cover only Process; MockState/Updates construction is under
// StopTimer, matching runDirectBench.
func Benchmark_StorageConcurrency(b *testing.B) {
	for _, slots := range []int{750_000} {
		b.Run(fmt.Sprintf("slots=%d", slots), func(b *testing.B) {
			single := buildWhaleStorageGroups(slots, 1)
			b.Run("Single", func(b *testing.B) {
				for b.Loop() {
					b.StopTimer()
					r := setupGroup(b, single[0])
					b.StartTimer()
					require.NoError(b, r.process())
					b.StopTimer()
					r.upds.Close()
					b.StartTimer()
				}
			})

			for _, groups := range []int{4, 8, 16} {
				gs := buildWhaleStorageGroups(slots, groups)
				b.Run(fmt.Sprintf("Groups%d-Serial", groups), func(b *testing.B) {
					for b.Loop() {
						b.StopTimer()
						rs := setupGroups(b, gs)
						b.StartTimer()
						for _, r := range rs {
							require.NoError(b, r.process())
						}
						b.StopTimer()
						closeGroups(rs)
						b.StartTimer()
					}
				})
				b.Run(fmt.Sprintf("Groups%d-Parallel", groups), func(b *testing.B) {
					for b.Loop() {
						b.StopTimer()
						rs := setupGroups(b, gs)
						b.StartTimer()
						var eg errgroup.Group
						for _, r := range rs {
							r := r
							eg.Go(r.process)
						}
						require.NoError(b, eg.Wait())
						b.StopTimer()
						closeGroups(rs)
						b.StartTimer()
					}
				})
			}
		})
	}
}
