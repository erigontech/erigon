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

package v3

import (
	"bytes"
	"context"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

type discardWrites struct{ commitment.PatriciaContext }

func (discardWrites) PutBranch([]byte, []byte, []byte) error { return nil }

func BenchmarkWitness(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	bed := newWitnessBed(b, witnessState(rng, witnessShape{accounts: 20000, slots: 8}))
	var keys witnessKeys
	for range 1000 {
		keys.plain = append(keys.plain, bed.accounts[rng.Intn(len(bed.accounts))], bed.slots[rng.Intn(len(bed.slots))])
	}
	for range 20 {
		keys.plain = append(keys.plain, craftKey(rng, length.Addr, nil, accountNibbles))
	}
	var batch []commitmenttest.Op
	for i := range 1200 {
		switch {
		case i < 800:
			batch = append(batch, commitmenttest.Op{Key: bed.accounts[rng.Intn(len(bed.accounts))], Account: randomWitnessAccount(rng)})
		case i < 1100:
			batch = append(batch, commitmenttest.Op{Key: bed.slots[rng.Intn(len(bed.slots))], Delete: true})
		default:
			addr := bed.accounts[rng.Intn(len(bed.accounts))]
			batch = append(batch, commitmenttest.Op{Key: append(bytes.Clone(addr), craftKey(rng, length.Hash, nil, slotNibbles(addr))...), Storage: randomWitnessValue(rng)})
		}
	}
	ctx := context.Background()
	dir := b.TempDir()
	for _, exclusion := range []bool{false, true} {
		name := map[bool]string{false: "canonical", true: "exclusion"}[exclusion]
		b.Run("nodes/"+name+"/hph", func(b *testing.B) {
			for b.Loop() {
				reader, _ := bed.hphMem.Open(ctx)
				hph := commitment.NewHexPatriciaHashed(length.Addr, reader, commitment.DefaultTrieConfig())
				require.NoError(b, hph.SetState(bed.hphState))
				u := commitment.NewUpdates(commitment.ModeUpdate, dir, commitment.KeyToHexNibbleHash)
				keys.touch(u)
				byHash, proved, root, err := hph.WitnessesByHash(ctx, u, exclusion)
				require.NoError(b, err)
				_, err = trie.WitnessNodesForKeysByHash(byHash, root, proved)
				require.NoError(b, err)
				u.Close()
			}
		})
		b.Run("nodes/"+name+"/v3", func(b *testing.B) {
			for b.Loop() {
				tr := bed.openV3(ctx)
				_, _, err := tr.RestoreState(bed.v3State)
				require.NoError(b, err)
				u := commitment.NewUpdates(commitment.ModeCollect, dir, commitment.KeyToHexNibbleHash)
				keys.touch(u)
				byHash, proved, root, err := tr.WitnessesByHash(ctx, u, exclusion)
				require.NoError(b, err)
				_, err = trie.WitnessNodesForKeysByHash(byHash, root, proved)
				require.NoError(b, err)
				u.Close()
			}
		})
	}

	bed.hphMem.Apply(batch)
	for _, trace := range []bool{false, true} {
		suffix := map[bool]string{false: "process", true: "process+trace"}[trace]
		b.Run("collapse/hph/"+suffix, func(b *testing.B) {
			for b.Loop() {
				reader, _ := bed.hphMem.Open(ctx)
				hph := commitment.NewHexPatriciaHashed(length.Addr, discardWrites{reader}, commitment.DefaultTrieConfig())
				require.NoError(b, hph.SetState(bed.hphState))
				if trace {
					hph.SetCollapseTracer(func([]byte, []byte) {})
				}
				u := commitment.NewUpdates(commitment.ModeUpdate, dir, commitment.KeyToHexNibbleHash)
				for _, op := range batch {
					u.TouchPlainKeyDirect(string(op.Key), runner.Update(op))
				}
				_, err := hph.Process(ctx, u, "", nil, commitment.WarmupConfig{})
				require.NoError(b, err)
				u.Close()
			}
		})
		b.Run("collapse/v3/"+suffix, func(b *testing.B) {
			for b.Loop() {
				reader, _ := bed.v3Mem.Open(ctx)
				tr := &Trie{}
				tr.ResetContext(discardWrites{reader})
				_, _, err := tr.RestoreState(bed.v3State)
				require.NoError(b, err)
				if trace {
					tr.SetCollapseTracer(func([]byte, []byte) {})
				}
				u := commitment.NewUpdates(commitment.ModeCollect, dir, commitment.KeyToHexNibbleHash)
				for _, op := range batch {
					u.TouchPlainKeyDirect(string(op.Key), runner.Update(op))
				}
				_, err = tr.Process(ctx, u, "", nil, commitment.WarmupConfig{})
				require.NoError(b, err)
				u.Close()
			}
		})
	}
	items := make([]feedEntry, len(batch))
	for i, op := range batch {
		items[i] = feedEntry{plainKey: string(op.Key), hashedKey: commitment.KeyToHexNibbleHash(op.Key), update: runner.Update(op)}
	}
	slices.SortFunc(items, compareFeed)
	b.Run("collapse/v3/trace-only", func(b *testing.B) {
		reader, _ := bed.v3Mem.Open(ctx)
		for b.Loop() {
			require.NoError(b, traceCollapses(reader, items, func([]byte, []byte) {}))
		}
	})
}
