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
	"maps"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
)

type leafRefContext struct {
	*mockContext
	corrupt bool
}

func (c *leafRefContext) LeafRefs(key, data []byte) *commitment.LeafRefs {
	refs := ComputeLeafRefs(key, data)
	if refs != nil && c.corrupt {
		for i := range refs.Refs {
			refs.Refs[i][0] ^= 0xff
		}
	}
	return refs
}

func TestPrefetch(t *testing.T) {
	serial := v3Config{workers: 1}
	seeded := func(t *testing.T, contracts int) (*mockContext, *Trie) {
		seed := make([]parityUpdate, 0, contracts*3)
		for i := range contracts {
			seed = append(seed, parityUpdate{key: benchAddr(i), update: accountParityUpdate(i)})
			for j := range 2 {
				seed = append(seed, parityUpdate{key: slotKey(benchAddr(i), benchSlot(i*2+j)), update: storageParityUpdate(i + j)})
			}
		}
		ctx := newMockContext()
		tr := &Trie{}
		t.Cleanup(tr.Release)
		runV3(t, ctx, v3Config{workers: 1, trie: tr}, seed)
		return ctx, tr
	}

	t.Run("path_covers_round_reads", func(t *testing.T) {
		const contracts = 3000
		ctx, tr := seeded(t, contracts)
		cases := []struct{ account, slot int }{
			{7, 14},
			{1234, 2468},
			{2999, 5998},
			{contracts + 5, 0},
			{11, contracts*2 + 11},
			{contracts + 17, contracts*2 + 17},
		}
		for i := range contracts {
			first, second := keccak.Sum256(benchSlot(i*2)), keccak.Sum256(benchSlot(i*2+1))
			if first[0]>>4 == second[0]>>4 {
				cases = append(cases, struct{ account, slot int }{i, i * 2})
				break
			}
		}
		require.Len(t, cases, 7)
		for _, c := range cases {
			addr, slot := benchAddr(c.account), benchSlot(c.slot)
			addrHash, slotHash := keccak.Sum256(addr), keccak.Sum256(slot)
			prefetched := map[string]bool{}
			read := func(key []byte) []byte {
				prefetched[string(key)] = true
				return ctx.branches[string(key)]
			}
			PrefetchPath(read, addrHash[:], nil, 0)
			PrefetchPath(read, addrHash[:], slotHash[:], 64)

			ctx.branchCalls = nil
			runV3(t, ctx, v3Config{workers: 1, trie: tr}, []parityUpdate{
				{key: addr, update: accountParityUpdate(c.account + 1)},
				{key: slotKey(addr, slot), update: storageParityUpdate(c.slot + 5)},
			})
			require.NotEmpty(t, ctx.branchCalls)
			for _, key := range ctx.branchCalls {
				require.Truef(t, prefetched[string(key)], "account %d slot %d: round read %x was not prefetched", c.account, c.slot, key)
			}
		}
	})

	t.Run("leaf_refs_keep_roots_and_records", func(t *testing.T) {
		const contracts = 2000
		base, _ := seeded(t, contracts)
		next := make([]parityUpdate, 0, 600)
		for i := 0; i < contracts; i += 10 {
			next = append(next,
				parityUpdate{key: benchAddr(i), update: accountParityUpdate(i + 3)},
				parityUpdate{key: slotKey(benchAddr(i), benchSlot(i*2)), update: storageParityUpdate(i + 7)},
				parityUpdate{key: benchAddr(contracts + i), update: accountParityUpdate(i + 11)})
		}
		run := func(refs, corrupt bool) ([]byte, map[string][]byte) {
			clone := newMockContext()
			maps.Copy(clone.branches, base.branches)
			var ctx commitment.PatriciaContext = clone
			if refs {
				ctx = &leafRefContext{mockContext: clone, corrupt: corrupt}
			}
			roots, _, _ := runV3(t, ctx, serial, next)
			return roots[0], clone.branches
		}
		plainRoot, plainBranches := run(false, false)
		refRoot, refBranches := run(true, false)
		require.Equal(t, plainRoot, refRoot)
		require.Equal(t, plainBranches, refBranches)

		corruptRoot, _ := run(true, true)
		require.NotEqual(t, plainRoot, corruptRoot)
	})
}
