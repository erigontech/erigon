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
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

func TestDeltas(t *testing.T) {
	t.Run("E40/DeltaPartsDropUnchangedRecords", func(t *testing.T) {
		var parts deltaParts
		parts.add(recordDelta{Key: []byte{1}, Data: []byte{2}, Prev: []byte{2}})
		parts.add(recordDelta{Key: []byte{3}, Data: []byte{4}, Prev: []byte{5}})
		parts.add(recordDelta{Key: []byte{6}, Data: nil, Prev: nil})
		require.Equal(t, deltaParts{{{Key: []byte{3}, Data: []byte{4}, Prev: []byte{5}}}}, parts)
	})
	t.Run("E40/ApplyDeltasReturnsPutError", func(t *testing.T) {
		wantErr := errors.New("put failed")
		err := applyDeltas(deltaParts{{{Key: []byte{1}, Data: []byte{2}, Prev: []byte{3}}}}, func(_, _, _ []byte) error {
			return wantErr
		}, nil)
		require.ErrorIs(t, err, wantErr)
	})
	t.Run("E40/MaterializeFoldsAndEncodesRecordInOneWalk", func(t *testing.T) {
		ctx := newMockContext()
		n := newNode(PlaneStorage)
		n.addLeaf(3, 4, 9)

		var acc deltaParts
		hash, err := graph{plane: PlaneStorage, addrHash: make([]byte, 32)}.materialize(context.Background(), ctx, n, n, &acc)
		require.NoError(t, err)
		require.Len(t, hash, 32)
		require.Len(t, acc, 1)
		require.Len(t, acc[0], 1)
		delta := acc[0][0]
		require.Equal(t, StorageNodeKey([32]byte{}, nil, nil), delta.Key)
		require.NotEmpty(t, delta.Data)
		require.NoError(t, Validate(delta.Data, 0))
		require.Empty(t, ctx.putCalls)
	})
	t.Run("E43/PersistGraphRetainsOnlyFoldedDeltasAfterChildWalk", func(t *testing.T) {
		for _, count := range []int{1000, 100000} {
			t.Run(strconv.Itoa(count), func(t *testing.T) {
				ctx := newMockContext()
				var addr [32]byte
				root := newNode(PlaneStorage)
				for i := range count {
					path := make([]byte, 64)
					path[0] = byte(i % 16)
					value := i
					for j := len(path) - 1; j >= 1 && value != 0; j-- {
						path[j] = byte(value & 0x0f)
						value >>= 4
					}
					require.NoError(t, insert(root, path, []byte{byte(i)}))
				}
				g := graph{plane: PlaneStorage, addrHash: addr[:]}
				parts, _, err := g.persistGraph(context.Background(), ctx, root, foldPlan{})
				require.NoError(t, err)
				require.NoError(t, applyDeltas(parts, ctx.PutBranch, nil))
				require.NotEmpty(t, ctx.branches)
				require.Equal(t, 1, linkedNodeCount(root))
				require.Empty(t, ctx.accountCalls)
				require.Empty(t, ctx.storageCalls)
			})
		}
	})
	t.Run("ParallelPersistKeepsRecreatedChild", func(t *testing.T) {
		addr := [32]byte{1}
		ctx := runner.NewMemory(runner.ContextSpec{CaptureDeltas: true, CheckPrevious: true})
		seed := storageOps([][]byte{slotPath(1, 2), slotPath(1, 3), slotPath(8, 2), slotPath(8, 3)})
		storageRound(t, ctx, addr, seed)
		previous, start := ctx.Records(), len(ctx.Deltas())
		next := []commitmenttest.Op{{Key: seed[0].Key, Delete: true}, {Key: slotPath(1, 4), Storage: []byte{5}}, seed[2]}
		root, parts, err := runStorageTaskWithPlan(t.Context(), ctx, storageTaskFor(addr, next), foldPlan{ctx: t.Context(), factory: ctx.Open, workers: 2, fanOutMin: 1})
		require.NoError(t, err)
		require.NoError(t, applyDeltas(parts, ctx.PutBranch, nil))
		for _, delta := range ctx.Deltas()[start:] {
			require.Equal(t, previous[string(delta.Key)], delta.Prev, "previous value for %x", delta.Key)
		}
		rebuilt := runner.NewMemory(runner.ContextSpec{})
		want := storageRound(t, rebuilt, addr, []commitmenttest.Op{seed[1], seed[2], seed[3], next[1]})
		require.Equal(t, want, root)
		requireLiveRecords(t, rebuilt.Records(), ctx.Records(), "recreated child")
	})
	t.Run("RootPromotionKeepsTombstones", func(t *testing.T) {
		addr := [32]byte{1}
		ctx := runner.NewMemory(runner.ContextSpec{CheckPrevious: true})
		seed := storageOps([][]byte{slotPath(1, 1), slotPath(1, 2), slotPath(8, 1), slotPath(8, 2)})
		storageRound(t, ctx, addr, seed)
		added := commitmenttest.Op{Key: slotPath(9, 1), Storage: []byte{5}}
		root := storageRound(t, ctx, addr, []commitmenttest.Op{{Key: seed[2].Key, Delete: true}, {Key: seed[3].Key, Delete: true}, added})
		rebuilt := runner.NewMemory(runner.ContextSpec{})
		want := storageRound(t, rebuilt, addr, []commitmenttest.Op{seed[0], seed[1], added})
		require.Equal(t, want, root)
		reachable, err := enumerateStorageRecords(t.Context(), ctx, addr)
		require.NoError(t, err)
		orphans := len(liveStorageRecords(ctx.Records())) - len(reachable)
		require.Zero(t, orphans)
		requireLiveRecords(t, rebuilt.Records(), ctx.Records(), "root promotion")
	})
	t.Run("E43/PersistGraphKeepsRecordsThatOnlyMovedDeeper", func(t *testing.T) {
		ctx := newMockContext()
		var addr [32]byte
		addr[0] = 0x7e
		g := graph{plane: PlaneStorage, addrHash: addr[:]}

		deepPath := []byte{0x0c, 0x06}
		deepKey := StorageNodeKey(addr, deepPath, nil)
		deepData := []byte{0xde, 0xad, 0xbe, 0xef}
		ctx.branches[string(deepKey)] = deepData
		siblingKey := StorageNodeKey(addr, []byte{0x02}, nil)
		ctx.branches[string(siblingKey)] = []byte{0xca, 0xfe}

		root := newNode(PlaneStorage)
		root.setStoredChild(0x0c, bytes.Repeat([]byte{0x11}, 32), []byte{0x06})
		root.setStoredChild(0x02, bytes.Repeat([]byte{0x22}, 32), nil)

		diverging := commitmenttest.Path([]byte{0x0c, 0x07}, 0x05)
		require.NoError(t, insert(root, diverging, []byte{0x01}))
		parts, _, err := g.persistGraph(context.Background(), ctx, root, foldPlan{})
		require.NoError(t, err)
		require.NoError(t, applyDeltas(parts, ctx.PutBranch, nil))

		require.Equal(t, deepData, ctx.branches[string(deepKey)], "record that only moved from depth 1 to depth 2 must not be tombstoned")
		require.Equal(t, []byte{0xca, 0xfe}, ctx.branches[string(siblingKey)])
	})

	generated := commitmenttest.Generate(commitmenttest.Seed(0), commitmenttest.SequenceSpec{Kind: "storage", Count: 2200})
	whale := commitmenttest.Key(commitmenttest.KeySpec{Kind: "bench-address", Size: 20}, 7)
	owner := parityAccount(whale, 7)
	put := func(i int) commitmenttest.Op {
		return commitmenttest.Op{Key: append(bytes.Clone(whale), commitmenttest.Key(commitmenttest.KeySpec{Kind: "bench-slot", Size: 32}, i)...), Storage: commitmenttest.Storage(i)}
	}
	deleted := put(1)
	deleted.Delete = true
	collapsed := []commitmenttest.Op{owner}
	for i := range 40 {
		collapsed = append(collapsed, put(i))
	}
	for _, tc := range []struct {
		name   string
		rounds [][]commitmenttest.Op
	}{
		{"E41/previous-store", [][]commitmenttest.Op{generated.Rounds[0][:4000], generated.Rounds[0][4000:]}},
		{"E41/recreated/leaf root split", [][]commitmenttest.Op{{owner, put(0)}, {owner, put(6)}}},
		{"E41/recreated/root extension split", [][]commitmenttest.Op{{owner, put(0), put(6)}, {owner, put(40)}}},
		{"E41/recreated/collapse and resplit", [][]commitmenttest.Op{collapsed, {owner, deleted, put(50)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := runner.NewMemory(runner.ContextSpec{CheckPrevious: true, ForbidStateReads: true})
			for _, ops := range tc.rounds {
				tr := newFactoryTrie(0, ctx, ctx.Open)
				_, err := tr.Process(context.Background(), testUpdates(t, commitment.ModeCollect, ops), "", nil, commitment.WarmupConfig{})
				tr.Release()
				require.NoError(t, err)
			}
		})
	}
	for _, tc := range []struct {
		name  string
		addr  byte
		paths [][]byte
		next  []commitmenttest.Op
		wipe  bool
	}{
		{"E41/changed-previous", 1, [][]byte{commitmenttest.Path([]byte{2}, 3), commitmenttest.Path([]byte{9}, 4)}, []commitmenttest.Op{{Key: commitmenttest.Path([]byte{2}, 3), Storage: []byte{3}}}, false},
		{"E37/E42/wipe-tombstones", 0x71, [][]byte{commitmenttest.Path([]byte{2, 3}, 4), commitmenttest.Path([]byte{2, 5}, 6)}, nil, true},
		{"E42/replay-reload", 0x81, [][]byte{commitmenttest.Path([]byte{1}, 2), commitmenttest.Path([]byte{8}, 3)}, []commitmenttest.Op{{Key: commitmenttest.Path([]byte{1}, 2), Delete: true}, {Key: commitmenttest.Path([]byte{13}, 4), Storage: []byte{3}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := [32]byte{tc.addr}
			ctx := runner.NewMemory(runner.ContextSpec{Borrowed: true, CaptureDeltas: true, CheckPrevious: true, ForbidStateReads: true})
			ops := []commitmenttest.Op{{Key: tc.paths[0], Storage: []byte{1}}, {Key: tc.paths[1], Storage: []byte{2}}}
			storageRound(t, ctx, addr, ops)
			previous, start := ctx.Records(), len(ctx.Deltas())
			task := storageTaskFor(addr, tc.next)
			task.wipe = tc.wipe
			root, err := runStorageTask(ctx, task)
			require.NoError(t, err)
			deltas := ctx.Deltas()[start:]
			require.NotEmpty(t, deltas)
			for _, delta := range deltas {
				require.Equal(t, previous[string(delta.Key)], delta.Prev, "previous value for %x", delta.Key)
				require.NotEqual(t, delta.Prev, delta.Data, "unchanged record emitted for %x", delta.Key)
			}
			if tc.wipe {
				require.Len(t, deltas, len(previous))
				for key, data := range previous {
					if len(data) == 0 {
						continue
					}
					at := slices.IndexFunc(deltas, func(d recordDelta) bool { return string(d.Key) == key })
					require.NotEqual(t, -1, at, "missing tombstone for %x", key)
					require.Empty(t, deltas[at].Data)
					require.NotEmpty(t, deltas[at].Prev)
					require.Contains(t, ctx.Records(), key)
					require.Empty(t, ctx.Records()[key])
				}
			}
			replayed := runner.NewMemory(runner.ContextSpec{CheckPrevious: true})
			require.NoError(t, applyDeltas(deltaParts{ctx.Deltas()}, replayed.PutBranch, nil))
			require.Equal(t, ctx.Records(), replayed.Records())
			n, err := unfold(replayed, nil, PlaneStorage, addr[:])
			require.NoError(t, err)
			require.NotNil(t, n)
			got, err := fold(n, 0)
			require.NoError(t, err)
			require.Equal(t, root, got)
			state := make(commitmenttest.State)
			if !tc.wipe {
				state.Apply(ops)
				state.Apply(tc.next)
			}
			requireStorageState(t, replayed, addr, state)
		})
	}
}

func linkedNodeCount(n *node) int {
	if n == nil {
		return 0
	}
	count := 1
	for i := range n.slots {
		count += linkedNodeCount(n.slots[i].node)
	}
	return count
}

func newTestRecordMatcher(t *testing.T) *RecordMatcher {
	m := NewRecordMatcher(t.TempDir())
	t.Cleanup(m.Close)
	return m
}

func TestRecordMatcherSpillsStorageRoots(t *testing.T) {
	addr := func(b byte) (a [32]byte) {
		a[0] = b
		return a
	}
	roots := map[byte][32]byte{0x10: {0xa1}, 0x20: {0xa2}, 0x30: {0xa3}, 0x40: {0xa4}}
	expectAll := func(t *testing.T) *RecordMatcher {
		dir := t.TempDir()
		m := newRecordMatcher(dir, 1)
		t.Cleanup(func() {
			m.Close()
			left, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Empty(t, left)
		})
		for _, b := range []byte{0x30, 0x10, 0x40, 0x20} {
			root := roots[b]
			require.NoError(t, m.Expect(StorageNodeKey(addr(b), nil, nil), root[:]))
		}
		spilled, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.NotEmpty(t, spilled)
		require.NoError(t, m.Record(AccountNodeKey(nil, nil), [32]byte{1}))
		return m
	}
	t.Run("MatchesGoodRoots", func(t *testing.T) {
		m := expectAll(t)
		for _, b := range []byte{0x10, 0x20, 0x30, 0x40} {
			require.NoError(t, m.Record(StorageNodeKey(addr(b), nil, nil), roots[b]))
		}
		root, records, orphans, err := m.Finish()
		require.NoError(t, err)
		require.Equal(t, [32]byte{1}, root)
		require.Equal(t, uint64(5), records)
		require.Zero(t, orphans)
	})
	t.Run("ReportsRootMismatch", func(t *testing.T) {
		m := expectAll(t)
		require.NoError(t, m.Record(StorageNodeKey(addr(0x10), nil, nil), roots[0x10]))
		require.ErrorContains(t, m.Record(StorageNodeKey(addr(0x20), nil, nil), roots[0x30]), "storage trie folds to")
	})
	t.Run("ReportsMissingStorageTrie", func(t *testing.T) {
		m := expectAll(t)
		require.NoError(t, m.Record(StorageNodeKey(addr(0x10), nil, nil), roots[0x10]))
		err := m.Record(StorageNodeKey(addr(0x30), nil, nil), roots[0x30])
		require.ErrorContains(t, err, fmt.Sprintf("account %x has storage root", addr(0x20)))
	})
	t.Run("ReportsMissingLastStorageTrie", func(t *testing.T) {
		m := expectAll(t)
		for _, b := range []byte{0x10, 0x20, 0x30} {
			require.NoError(t, m.Record(StorageNodeKey(addr(b), nil, nil), roots[b]))
		}
		_, _, _, err := m.Finish()
		require.ErrorContains(t, err, fmt.Sprintf("account %x has storage root", addr(0x40)))
	})
}

func TestRecordGraph(t *testing.T) {
	t.Run("StorageRootExpectedAfterStoragePlane", func(t *testing.T) {
		matcher := newTestRecordMatcher(t)
		addr := [32]byte{1}
		hash := [32]byte{2}
		require.NoError(t, matcher.Record(StorageNodeKey(addr, []byte{3}, nil), hash))
		require.ErrorContains(t, matcher.Expect(StorageNodeKey(addr, nil, nil), hash[:]), "expected after the account plane")
	})
	t.Run("E66/RecordHasherExpectsEveryExtensionChild", func(t *testing.T) {
		n := newNode(PlaneAccount)
		n.setStoredChild(1, bytes.Repeat([]byte{0x11}, 32), []byte{2, 3})
		n.setStoredChild(4, bytes.Repeat([]byte{0x44}, 32), []byte{5, 6, 7})
		record := encodeRecord(n, 0, nil)
		want, err := fold(n, 0)
		require.NoError(t, err)

		var keys [][]byte
		got, err := NewRecordHasher().Hash(AccountNodeKey(nil, nil), record, func(key, _ []byte) error {
			keys = append(keys, bytes.Clone(key))
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, want, got)
		require.Equal(t, [][]byte{AccountNodeKey([]byte{1, 2, 3}, nil), AccountNodeKey([]byte{4, 5, 6, 7}, nil)}, keys)
	})
	t.Run("E66/RecordMatcherChecksOnlyReferencedStorageTries", func(t *testing.T) {
		var addrHash [32]byte
		addrHash[0] = 0xe7
		child := StorageNodeKey(addrHash, []byte{8, 2}, nil)
		parent := StorageNodeKey(addrHash, []byte{8}, nil)

		dead := newTestRecordMatcher(t)
		require.NoError(t, dead.Record(AccountNodeKey(nil, nil), [32]byte{1}))
		require.NoError(t, dead.Expect(child, bytes.Repeat([]byte{0x22}, 32)))
		require.NoError(t, dead.Record(parent, [32]byte{8}))
		_, _, orphans, err := dead.Finish()
		require.NoError(t, err)
		require.Equal(t, uint64(1), orphans)

		var root [32]byte
		copy(root[:], bytes.Repeat([]byte{0x77}, 32))
		live := newTestRecordMatcher(t)
		require.NoError(t, live.Expect(StorageNodeKey(addrHash, nil, nil), root[:]))
		require.NoError(t, live.Record(AccountNodeKey(nil, nil), [32]byte{1}))
		require.NoError(t, live.Record(StorageNodeKey(addrHash, nil, nil), root))
		require.NoError(t, live.Expect(child, bytes.Repeat([]byte{0x22}, 32)))
		require.NoError(t, live.Record(parent, [32]byte{8}))
		_, _, _, err = live.Finish()
		require.ErrorContains(t, err, "is referenced but missing")
	})
	t.Run("E66/RecordMatcherRejectsHashMismatch", func(t *testing.T) {
		matcher := newTestRecordMatcher(t)
		key := AccountNodeKey([]byte{1}, nil)
		h1, h2 := [32]byte{1}, [32]byte{2}
		require.NoError(t, matcher.Expect(key, h1[:]))
		require.ErrorContains(t, matcher.Record(key, h2), "folds to")
	})
	t.Run("E66/RecordMatcherRejectsStorageRootMismatch", func(t *testing.T) {
		matcher := newTestRecordMatcher(t)
		var addrHash [32]byte
		addrHash[0] = 1
		stored, folded := [32]byte{2}, [32]byte{3}
		require.NoError(t, matcher.Expect(StorageNodeKey(addrHash, nil, nil), stored[:]))
		require.NoError(t, matcher.Record(AccountNodeKey(nil, nil), [32]byte{4}))
		require.ErrorContains(t, matcher.Record(StorageNodeKey(addrHash, nil, nil), folded), "storage trie folds to")
	})
	t.Run("E66/RecordMatcherRejectsDuplicateRecord", func(t *testing.T) {
		matcher := newTestRecordMatcher(t)
		key := AccountNodeKey([]byte{1}, nil)
		hash := [32]byte{1}
		require.NoError(t, matcher.Record(key, hash))
		require.ErrorContains(t, matcher.Record(key, hash), "recorded or expected twice")
	})
	t.Run("E66/RecordMatcherRejectsMissingStorageRoot", func(t *testing.T) {
		matcher := newTestRecordMatcher(t)
		var addrHash [32]byte
		addrHash[0] = 1
		root := [32]byte{2}
		require.NoError(t, matcher.Expect(StorageNodeKey(addrHash, nil, nil), root[:]))
		require.NoError(t, matcher.Record(AccountNodeKey(nil, nil), [32]byte{3}))
		_, _, _, err := matcher.Finish()
		require.ErrorContains(t, err, "no storage trie")
	})
	t.Run("E66/EmptyChildRecordIsInvalid", func(t *testing.T) {
		var addrHash [32]byte
		parent := newNode(PlaneStorage)
		parent.setStoredChild(4, bytes.Repeat([]byte{0x44}, 32), nil)
		ctx := newMockContext()
		ctx.branches[string(StorageNodeKey(addrHash, []byte{4}, nil))] = []byte{}

		g := graph{plane: PlaneStorage, addrHash: addrHash[:]}
		_, err := g.unfoldChild(ctx, parent.childPath(4, nil))
		require.ErrorIs(t, err, errNodeRecord)
	})
	t.Run("SingleHashChildBelowRootIsInvalid", func(t *testing.T) {
		ctx := newMockContext()
		data := commitmenttest.Record(&commitmenttest.RecordSpec{ChildMask: 1})
		ctx.branches[string(AccountNodeKey([]byte{4}, nil))] = data
		require.ErrorIs(t, Validate(data, 1), ErrRecordMasks)

		root := newNode(PlaneAccount)
		root.setStoredChild(4, bytes.Repeat([]byte{1}, 32), nil)
		root.setStoredChild(8, bytes.Repeat([]byte{2}, 32), nil)
		err := (graph{plane: PlaneAccount}).ensurePath(ctx, root, commitmenttest.Path([]byte{4, 0}, 1))
		require.ErrorIs(t, err, ErrRecordMasks)
	})
	t.Run("E66/ZeroMaskChildRecordIsInvalid", func(t *testing.T) {
		var addrHash [32]byte
		parent := newNode(PlaneStorage)
		parent.setStoredChild(4, bytes.Repeat([]byte{0x44}, 32), nil)
		ctx := newMockContext()
		ctx.branches[string(StorageNodeKey(addrHash, []byte{4}, nil))] = make([]byte, 5)

		g := graph{plane: PlaneStorage, addrHash: addrHash[:]}
		_, err := g.unfoldChild(ctx, parent.childPath(4, nil))
		require.ErrorIs(t, err, ErrRecordMasks)
		for _, depth := range []int{0, 1, 63} {
			require.ErrorIs(t, Validate(make([]byte, 5), depth), ErrRecordMasks)
		}
	})
}
