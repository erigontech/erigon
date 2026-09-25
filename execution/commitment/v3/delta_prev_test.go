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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
)

type prevCheckContext struct {
	*shardedContext
}

func (c *prevCheckContext) PutBranch(key, data, prev []byte) error {
	stored, _, err := c.shardedContext.Branch(key)
	if err != nil {
		return err
	}
	if !bytes.Equal(stored, prev) {
		return fmt.Errorf("prev mismatch at %x: store holds %d bytes, delta claims %d", key, len(stored), len(prev))
	}
	return c.shardedContext.PutBranch(key, data, prev)
}

func (c *prevCheckContext) factory(context.Context) (commitment.PatriciaContext, func()) {
	return c, nil
}

var _ commitment.PatriciaContext = (*prevCheckContext)(nil)

func TestRecordDeltaPrevMatchesStore(t *testing.T) {
	const n = 2000
	fresh := make([]parityUpdate, 0, n/5)
	for i := n; i < n+n/10; i++ {
		fresh = append(fresh,
			parityUpdate{key: benchAddr(i), update: accountParityUpdate(i)},
			parityUpdate{key: append(benchAddr(i), benchSlot(i)...), update: storageParityUpdate(i)})
	}

	c := &prevCheckContext{shardedContext: newShardedContext()}
	dir := t.TempDir()
	round := func(entries []parityUpdate) error {
		tr := &Trie{}
		tr.ResetContext(c)
		tr.SetTrieContextFactory(c.factory)
		defer tr.Release()
		_, err := tr.Process(context.Background(),
			benchUpdatesIn(dir, commitment.ModeCollect, entries), "", nil, commitment.WarmupConfig{})
		return err
	}

	require.NoError(t, round(benchEntries("storage", n)))
	require.NoError(t, round(fresh))
}

func TestRecordDeltaPrevMatchesStoreWhenLiveKeysAreRecreated(t *testing.T) {
	whale := benchAddr(7)
	slot := func(i int) []byte { return append(bytes.Clone(whale), benchSlot(i)...) }
	put := func(i int) parityUpdate { return parityUpdate{key: slot(i), update: storageParityUpdate(i)} }
	del := func(i int) parityUpdate {
		return parityUpdate{key: slot(i), update: &commitment.Update{Flags: commitment.DeleteUpdate}}
	}
	account := parityUpdate{key: whale, update: accountParityUpdate(7)}
	collapseRounds := [][]parityUpdate{{account}, {account, del(1), put(50)}}
	for i := range 40 {
		collapseRounds[0] = append(collapseRounds[0], put(i))
	}

	for name, rounds := range map[string][][]parityUpdate{
		"leaf root split":      {{account, put(0)}, {account, put(6)}},
		"root extension split": {{account, put(0), put(6)}, {account, put(40)}},
		"collapse and resplit": collapseRounds,
	} {
		t.Run(name, func(t *testing.T) {
			c := &prevCheckContext{shardedContext: newShardedContext()}
			dir := t.TempDir()
			for _, entries := range rounds {
				tr := &Trie{}
				tr.ResetContext(c)
				tr.SetTrieContextFactory(c.factory)
				_, err := tr.Process(context.Background(),
					benchUpdatesIn(dir, commitment.ModeCollect, entries), "", nil, commitment.WarmupConfig{})
				tr.Release()
				require.NoError(t, err)
			}
		})
	}
}
