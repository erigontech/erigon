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
	"maps"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
)

func TestPrefetchPathCoversRoundReads(t *testing.T) {
	const contracts = 3000
	seed := make([]parityUpdate, 0, contracts*3)
	for i := range contracts {
		addr := benchAddr(i)
		seed = append(seed, parityUpdate{key: addr, update: accountParityUpdate(i)})
		for j := range 2 {
			seed = append(seed, parityUpdate{key: append(bytes.Clone(addr), benchSlot(i*2+j)...), update: storageParityUpdate(i + j)})
		}
	}
	ctx := newMockContext()
	tr := &Trie{}
	tr.ResetContext(ctx)
	_, err := tr.Process(context.Background(), benchUpdatesIn(t.TempDir(), commitment.ModeCollect, seed), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)

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
		next := []parityUpdate{
			{key: addr, update: accountParityUpdate(c.account + 1)},
			{key: append(bytes.Clone(addr), slot...), update: storageParityUpdate(c.slot + 5)},
		}
		_, err := tr.Process(context.Background(), benchUpdatesIn(t.TempDir(), commitment.ModeCollect, next), "", nil, commitment.WarmupConfig{})
		require.NoError(t, err)
		require.NotEmpty(t, ctx.branchCalls)
		for _, key := range ctx.branchCalls {
			require.Truef(t, prefetched[string(key)], "account %d slot %d: round read %x was not prefetched", c.account, c.slot, key)
		}
	}
}

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

func TestLeafRefsKeepRootsAndRecords(t *testing.T) {
	const contracts = 2000
	seed := make([]parityUpdate, 0, contracts*3)
	for i := range contracts {
		addr := benchAddr(i)
		seed = append(seed, parityUpdate{key: addr, update: accountParityUpdate(i)})
		for j := range 2 {
			seed = append(seed, parityUpdate{key: append(bytes.Clone(addr), benchSlot(i*2+j)...), update: storageParityUpdate(i + j)})
		}
	}
	base := newMockContext()
	tr := &Trie{}
	tr.ResetContext(base)
	_, err := tr.Process(context.Background(), benchUpdatesIn(t.TempDir(), commitment.ModeCollect, seed), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)

	next := make([]parityUpdate, 0, 600)
	for i := 0; i < contracts; i += 10 {
		addr := benchAddr(i)
		next = append(next,
			parityUpdate{key: addr, update: accountParityUpdate(i + 3)},
			parityUpdate{key: append(bytes.Clone(addr), benchSlot(i*2)...), update: storageParityUpdate(i + 7)},
			parityUpdate{key: benchAddr(contracts + i), update: accountParityUpdate(i + 11)})
	}
	run := func(ctx commitment.PatriciaContext, branches map[string][]byte) ([]byte, map[string][]byte) {
		clone := newMockContext()
		maps.Copy(clone.branches, branches)
		switch c := ctx.(type) {
		case *leafRefContext:
			c.mockContext = clone
		default:
			ctx = clone
		}
		trie := &Trie{}
		trie.ResetContext(ctx)
		root, err := trie.Process(context.Background(), benchUpdatesIn(t.TempDir(), commitment.ModeCollect, next), "", nil, commitment.WarmupConfig{})
		require.NoError(t, err)
		return root, clone.branches
	}
	plainRoot, plainBranches := run(nil, base.branches)
	refRoot, refBranches := run(&leafRefContext{}, base.branches)
	require.Equal(t, plainRoot, refRoot)
	require.Equal(t, plainBranches, refBranches)

	corruptRoot, _ := run(&leafRefContext{corrupt: true}, base.branches)
	require.NotEqual(t, plainRoot, corruptRoot)
}
