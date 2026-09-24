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
	"context"
	"slices"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
)

func feedOf(entries []parityUpdate) *commitment.Feed {
	feed := &commitment.Feed{Keys: len(entries)}
	index := make(map[string]int)
	for _, e := range entries {
		addr := string(e.key[:length.Addr])
		at, ok := index[addr]
		if !ok {
			at = len(feed.Accounts)
			index[addr] = at
			feed.Accounts = append(feed.Accounts, commitment.FeedAccount{Hash: keccak.Sum256(e.key[:length.Addr])})
		}
		account := &feed.Accounts[at]
		if len(e.key) == length.Addr {
			account.Update = e.update
			continue
		}
		slot := commitment.FeedSlot{Hash: keccak.Sum256(e.key[length.Addr:])}
		if !e.update.Deleted() {
			slot.Value = e.update.Storage[:e.update.StorageLen]
		}
		account.Slots = append(account.Slots, slot)
	}
	return feed
}

func TestProcessFeedMatchesProcess(t *testing.T) {
	deleted := func() *commitment.Update { return &commitment.Update{Flags: commitment.DeleteUpdate} }
	seed := append(benchEntries("storage", 300), benchEntries("whale", 3*storageFanOutMin)...)
	var next []parityUpdate
	for i := range 300 {
		addr, slot := benchAddr(i), benchSlot(i)
		switch i % 5 {
		case 0:
			next = append(next, parityUpdate{key: addr, update: accountParityUpdate(i + 1000)})
		case 1:
			next = append(next, parityUpdate{key: append(addr, slot...), update: storageParityUpdate(i + 1000)})
		case 2:
			next = append(next, parityUpdate{key: append(addr, slot...), update: deleted()})
		case 3:
			next = append(next, parityUpdate{key: addr, update: deleted()})
		default:
			next = append(next,
				parityUpdate{key: addr, update: deleted()},
				parityUpdate{key: append(addr, slot...), update: deleted()})
		}
	}
	whale := benchEntries("whale", 3*storageFanOutMin)
	for i := 1; i < len(whale); i += 3 {
		next = append(next, parityUpdate{key: whale[i].key, update: deleted()})
	}

	run := func(feed, deferred bool) ([][]byte, map[string]string, []string) {
		c := newShardedContext()
		tr := &Trie{}
		tr.ResetContext(c)
		tr.SetTrieContextFactory(c.factory)
		tr.SetDeferCommitmentUpdates(deferred)
		defer tr.Release()
		var roots [][]byte
		var deltas []string
		for _, round := range [][]parityUpdate{seed, next} {
			var root []byte
			var err error
			if feed {
				root, err = tr.ProcessFeed(context.Background(), feedOf(round), nil)
			} else {
				root, err = tr.Process(context.Background(), benchUpdatesIn(t.TempDir(), commitment.ModeCollect, round), "", nil, commitment.WarmupConfig{})
			}
			require.NoError(t, err)
			roots = append(roots, root)
			for _, part := range tr.TakeDeferredDeltas() {
				for _, d := range part {
					deltas = append(deltas, string(d.Key)+"|"+string(d.Data)+"|"+string(d.Prev))
					require.NoError(t, c.PutBranch(d.Key, d.Data, d.Prev))
				}
			}
		}
		slices.Sort(deltas)
		return roots, storeSnapshot(c), deltas
	}

	for _, deferred := range []bool{false, true} {
		wantRoots, wantStore, wantDeltas := run(false, deferred)
		gotRoots, gotStore, gotDeltas := run(true, deferred)
		require.NotEqual(t, wantRoots[0], wantRoots[1])
		require.Equal(t, wantRoots, gotRoots)
		require.Equal(t, wantStore, gotStore)
		require.Equal(t, deferred, len(wantDeltas) != 0)
		require.Equal(t, wantDeltas, gotDeltas)
	}
}
