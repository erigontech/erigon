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

package v4

import (
	"bytes"
	"context"
	"maps"
	"testing"

	"github.com/erigontech/erigon/execution/commitment"
)

func BenchmarkV4Incremental(b *testing.B) {
	const contracts = 100000
	seed := make([]parityUpdate, 0, contracts*5)
	next := make([]parityUpdate, 0, contracts)
	for i := range contracts {
		addr := benchAddr(i)
		seed = append(seed, parityUpdate{key: addr, update: accountParityUpdate(i)})
		for j := range 4 {
			seed = append(seed, parityUpdate{key: append(bytes.Clone(addr), benchSlot(i*4+j)...), update: storageParityUpdate(i + j)})
		}
		if i%5 == 0 {
			next = append(next,
				parityUpdate{key: addr, update: accountParityUpdate(i + 1)},
				parityUpdate{key: append(bytes.Clone(addr), benchSlot(i*4)...), update: storageParityUpdate(i + 7)},
				parityUpdate{key: append(bytes.Clone(addr), benchSlot(contracts*4+i)...), update: storageParityUpdate(i + 9)})
		}
	}
	for i := contracts; i < contracts+contracts/20; i++ {
		next = append(next, parityUpdate{key: benchAddr(i), update: accountParityUpdate(i)})
	}

	base := newShardedContext()
	tr := &Trie{}
	tr.ResetContext(base)
	tr.SetTrieContextFactory(base.factory)
	if _, err := tr.Process(context.Background(), benchUpdatesIn(b.TempDir(), commitment.ModeCollect, seed), "", nil, commitment.WarmupConfig{}); err != nil {
		b.Fatal(err)
	}
	tr.Release()

	dir := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		c := newShardedContext()
		for s := range base.shards {
			c.shards[s].branches = maps.Clone(base.shards[s].branches)
		}
		tr := &Trie{}
		tr.ResetContext(c)
		tr.SetTrieContextFactory(c.factory)
		u := benchUpdatesIn(dir, commitment.ModeCollect, next)
		b.StartTimer()
		if _, err := tr.Process(context.Background(), u, "", nil, commitment.WarmupConfig{}); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		tr.Release()
		b.StartTimer()
	}
}
