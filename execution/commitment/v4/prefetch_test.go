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

	for _, i := range []int{7, 1234, 2999} {
		addr, slot := benchAddr(i), benchSlot(i*2)
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
			{key: addr, update: accountParityUpdate(i + 1)},
			{key: append(bytes.Clone(addr), slot...), update: storageParityUpdate(i + 5)},
		}
		_, err := tr.Process(context.Background(), benchUpdatesIn(t.TempDir(), commitment.ModeCollect, next), "", nil, commitment.WarmupConfig{})
		require.NoError(t, err)
		require.NotEmpty(t, ctx.branchCalls)
		for _, key := range ctx.branchCalls {
			require.Truef(t, prefetched[string(key)], "contract %d: round read %x was not prefetched", i, key)
		}
	}
}
