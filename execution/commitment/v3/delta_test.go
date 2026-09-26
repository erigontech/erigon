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
	"slices"
	"testing"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
	"github.com/stretchr/testify/require"
)

func TestDeltas(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E40/DeltaPartsDropUnchangedRecords", func(t *testing.T) {
			var parts deltaParts
			parts.add(recordDelta{Key: []byte{1}, Data: []byte{2}, Prev: []byte{2}})
			parts.add(recordDelta{Key: []byte{3}, Data: []byte{4}, Prev: []byte{5}})
			parts.add(recordDelta{Key: []byte{6}, Data: nil, Prev: nil})
			require.Equal(t, deltaParts{{{Key: []byte{3}, Data: []byte{4}, Prev: []byte{5}}}}, parts)
		}},
		{"E40/ApplyDeltasReturnsPutError", func(t *testing.T) {
			wantErr := errors.New("put failed")
			err := applyDeltas(deltaParts{{{Key: []byte{1}, Data: []byte{2}, Prev: []byte{3}}}}, func(_, _, _ []byte) error {
				return wantErr
			})
			require.ErrorIs(t, err, wantErr)
		}},
		{"E40/MaterializeFoldsAndEncodesRecordInOneWalk", func(t *testing.T) {
			ctx := newMockContext()
			n := fork(nil)
			n.plane = planeStorage
			path := append([]byte{3}, bytes.Repeat([]byte{4}, 63)...)
			n.setLeaf(int(path[0]), packPath(path[1:], nil), []byte{9})

			var acc deltaParts
			hash, err := graph{plane: planeStorage, addrHash: make([]byte, 32)}.materialize(ctx, n, n, &acc)
			require.NoError(t, err)
			require.Len(t, hash, 32)
			require.Len(t, acc, 1)
			require.Len(t, acc[0], 1)
			delta := acc[0][0]
			require.Equal(t, StorageNodeKey([32]byte{}, nil, nil), delta.Key)
			require.NotEmpty(t, delta.Data)
			require.NoError(t, Validate(delta.Data, 0))
			require.Empty(t, ctx.putCalls)
		}},
		{"E43/PersistGraphRetainsOnlyFoldedDeltasAfterChildWalk", func(t *testing.T) {
			for _, count := range []int{1000, 100000} {
				t.Run(itoa(count), func(t *testing.T) {
					ctx := newMockContext()
					var addr [32]byte
					root := fork(nil)
					root.plane = planeStorage
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
					g := graph{plane: planeStorage, addrHash: addr[:]}
					parts, err := g.persistGraph(ctx, root, foldPlan{})
					require.NoError(t, err)
					require.NoError(t, applyDeltas(parts, ctx.PutBranch))
					require.NotEmpty(t, ctx.branches)
					require.Equal(t, 1, linkedNodeCount(root))
					require.Empty(t, ctx.accountCalls)
					require.Empty(t, ctx.storageCalls)
				})
			}
		}},
		{"E43/PersistGraphKeepsRecordsThatOnlyMovedDeeper", func(t *testing.T) {
			ctx := newMockContext()
			var addr [32]byte
			addr[0] = 0x7e
			g := graph{plane: planeStorage, addrHash: addr[:]}

			deepPath := []byte{0x0c, 0x06}
			deepKey := StorageNodeKey(addr, deepPath, nil)
			deepData := []byte{0xde, 0xad, 0xbe, 0xef}
			ctx.branches[string(deepKey)] = deepData
			siblingKey := StorageNodeKey(addr, []byte{0x02}, nil)
			ctx.branches[string(siblingKey)] = []byte{0xca, 0xfe}

			root := fork(nil)
			root.plane = planeStorage
			root.setStoredChild(0x0c, bytes.Repeat([]byte{0x11}, 32), []byte{0x06})
			root.setStoredChild(0x02, bytes.Repeat([]byte{0x22}, 32), nil)

			diverging := append([]byte{0x0c, 0x07}, bytes.Repeat([]byte{0x05}, 62)...)
			require.NoError(t, insert(root, diverging, []byte{0x01}))
			parts, err := g.persistGraph(ctx, root, foldPlan{})
			require.NoError(t, err)
			require.NoError(t, applyDeltas(parts, ctx.PutBranch))

			require.Equal(t, deepData, ctx.branches[string(deepKey)], "record that only moved from depth 1 to depth 2 must not be tombstoned")
			require.Equal(t, []byte{0xca, 0xfe}, ctx.branches[string(siblingKey)])
		}},
	} {
		t.Run(tc.name, tc.run)
	}

	generated, err := commitmenttest.Generate(commitmenttest.MathRand(0), commitmenttest.SequenceSpec{Kind: "storage", Count: 2200})
	require.NoError(t, err)
	whale := commitmenttest.Key(commitmenttest.KeySpec{Kind: "bench-address", Size: 20}, 7)
	account := commitmenttest.Account(commitmenttest.AccountSpec{Kind: "parity", Number: 7})
	owner := commitmenttest.Op{Key: whale, Account: &account}
	put := func(i int) commitmenttest.Op {
		return commitmenttest.Op{Key: append(bytes.Clone(whale), commitmenttest.Key(commitmenttest.KeySpec{Kind: "bench-slot", Size: 32}, i)...), Storage: commitmenttest.Storage(commitmenttest.StorageSpec{Number: i})}
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
				tr := &Trie{}
				tr.ResetContext(ctx)
				tr.SetTrieContextFactory(ctx.Open)
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
		{"E41/changed-previous", 1, [][]byte{append([]byte{2}, bytes.Repeat([]byte{3}, 63)...), append([]byte{9}, bytes.Repeat([]byte{4}, 63)...)}, []commitmenttest.Op{{Key: append([]byte{2}, bytes.Repeat([]byte{3}, 63)...), Storage: []byte{3}}}, false},
		{"E37/E42/wipe-tombstones", 0x71, [][]byte{append([]byte{2, 3}, bytes.Repeat([]byte{4}, 62)...), append([]byte{2, 5}, bytes.Repeat([]byte{6}, 62)...)}, nil, true},
		{"E42/replay-reload", 0x81, [][]byte{append([]byte{1}, bytes.Repeat([]byte{2}, 63)...), append([]byte{8}, bytes.Repeat([]byte{3}, 63)...)}, []commitmenttest.Op{{Key: append([]byte{1}, bytes.Repeat([]byte{2}, 63)...), Delete: true}, {Key: append([]byte{13}, bytes.Repeat([]byte{4}, 63)...), Storage: []byte{3}}}, false},
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
			require.NoError(t, applyDeltas(deltaParts{ctx.Deltas()}, replayed.PutBranch))
			require.Equal(t, ctx.Records(), replayed.Records())
			n, err := unfold(replayed, nil, planeStorage, addr[:])
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
