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
	"encoding/binary"
	"errors"
	"maps"
	"math/rand"
	"slices"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
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
			n := newNode(planeStorage)
			n.addLeaf(3, 4, 9)

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
					root := newNode(planeStorage)
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

			root := newNode(planeStorage)
			root.setStoredChild(0x0c, bytes.Repeat([]byte{0x11}, 32), []byte{0x06})
			root.setStoredChild(0x02, bytes.Repeat([]byte{0x22}, 32), nil)

			diverging := commitmenttest.Path([]byte{0x0c, 0x07}, 0x05)
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

	generated := commitmenttest.Generate(commitmenttest.MathRand(0), commitmenttest.SequenceSpec{Kind: "storage", Count: 2200})
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

func TestRecordGraph(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E66/RecordHasherExpectsEveryExtensionChild", func(t *testing.T) {
			n := newNode(planeAccount)
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
		}},
		{"E66/RecordMatcherChecksOnlyReferencedStorageTries", func(t *testing.T) {
			var addrHash [32]byte
			addrHash[0] = 0xe7
			child := StorageNodeKey(addrHash, []byte{8, 2}, nil)
			parent := StorageNodeKey(addrHash, []byte{8}, nil)

			dead := NewRecordMatcher()
			require.NoError(t, dead.Record(AccountNodeKey(nil, nil), [32]byte{1}))
			require.NoError(t, dead.Expect(child, bytes.Repeat([]byte{0x22}, 32)))
			require.NoError(t, dead.Record(parent, [32]byte{8}))
			_, _, orphans, err := dead.Finish()
			require.NoError(t, err)
			require.Equal(t, uint64(1), orphans)

			var root [32]byte
			copy(root[:], bytes.Repeat([]byte{0x77}, 32))
			live := NewRecordMatcher()
			require.NoError(t, live.Expect(StorageNodeKey(addrHash, nil, nil), root[:]))
			require.NoError(t, live.Record(AccountNodeKey(nil, nil), [32]byte{1}))
			require.NoError(t, live.Record(StorageNodeKey(addrHash, nil, nil), root))
			require.NoError(t, live.Expect(child, bytes.Repeat([]byte{0x22}, 32)))
			require.NoError(t, live.Record(parent, [32]byte{8}))
			_, _, _, err = live.Finish()
			require.ErrorContains(t, err, "is referenced but missing")
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

type legacyWorld struct {
	*runner.Memory
	trie    *commitment.HexPatriciaHashed
	written map[string][]byte
	touched map[string]struct{}
}

func newLegacyWorld() *legacyWorld {
	w := &legacyWorld{Memory: runner.NewMemory(runner.ContextSpec{}), touched: map[string]struct{}{}}
	w.trie = commitment.NewHexPatriciaHashed(length.Addr, w, commitment.TrieConfig{})
	return w
}

func (w *legacyWorld) PutBranch(key, data, prev []byte) error {
	w.written[string(key)] = bytes.Clone(data)
	return w.Memory.PutBranch(key, data, prev)
}

type legacyValues struct{ *runner.Memory }

func (v legacyValues) Account(key []byte) ([]byte, error) {
	u, err := v.Memory.Account(key)
	if err != nil || u.Deleted() {
		return nil, err
	}
	acc := accounts.Account{Nonce: u.Nonce, Balance: u.Balance, CodeHash: accounts.EmptyCodeHash}
	if u.CodeHash != empty.CodeHash {
		acc.CodeHash = accounts.InternCodeHash(u.CodeHash)
	}
	return accounts.SerialiseV3(&acc), nil
}

func (v legacyValues) Storage(key []byte) ([]byte, error) {
	u, err := v.Memory.Storage(key)
	if err != nil || u.Deleted() {
		return nil, err
	}
	return bytes.Clone(u.Storage[:u.StorageLen]), nil
}

func (w *legacyWorld) apply(op commitmenttest.Op) {
	w.Apply([]commitmenttest.Op{op})
	w.touched[string(op.Key)] = struct{}{}
}

func (w *legacyWorld) setAccount(addr []byte, nonce, balance uint64) {
	w.apply(commitmenttest.AccountAt(addr, commitmenttest.AccountSpec{Nonce: nonce, Balance: balance, CodeHash: empty.CodeHash}))
}

func (w *legacyWorld) setSlot(addr, slot []byte, value uint64) {
	w.apply(commitmenttest.Op{Key: commitmenttest.SlotKey(addr, slot), Storage: bytes.TrimLeft(binary.BigEndian.AppendUint64(nil, value), "\x00")})
}

func (w *legacyWorld) live(key []byte) bool {
	u, err := w.Memory.Storage(key)
	return err == nil && !u.Deleted()
}

func (w *legacyWorld) process(t *testing.T) []byte {
	t.Helper()
	w.written = map[string][]byte{}
	batch := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash)
	defer batch.Close()
	for _, key := range slices.Sorted(maps.Keys(w.touched)) {
		read := w.Memory.Storage
		if len(key) == length.Addr {
			read = w.Memory.Account
		}
		u, err := read([]byte(key))
		require.NoError(t, err)
		batch.TouchPlainKeyDirect(key, u)
	}
	w.touched = map[string]struct{}{}
	root, err := w.trie.Process(context.Background(), batch, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	return root
}

func (w *legacyWorld) convert(t *testing.T, prevs map[string][]byte, incremental bool, records map[string][]byte) []byte {
	t.Helper()
	grouped := map[string][]LegacyEntry{}
	conv := NewLegacyConverter(legacyValues{w.Memory}, false, incremental)
	for _, key := range slices.Sorted(maps.Keys(w.written)) {
		err := conv.Convert([]byte(key), w.written[key], prevs[key], func(k, v []byte, kind LegacyKind) error {
			grouped[string(k)] = append(grouped[string(k)], LegacyEntry{Kind: kind, Value: bytes.Clone(v)})
			return nil
		})
		require.NoError(t, err, "legacy key %x", key)
	}
	for key, entries := range grouped {
		value, write, err := ResolveLegacy([]byte(key), entries)
		require.NoError(t, err)
		if write {
			records[key] = value
		}
	}
	hasher, matcher := NewRecordHasher(), NewRecordMatcher()
	for _, key := range slices.Sorted(maps.Keys(records)) {
		if len(records[key]) == 0 || key == string(commitment.KeyCommitmentV3State) {
			continue
		}
		hash, err := hasher.Hash([]byte(key), records[key], matcher.Expect)
		require.NoError(t, err)
		require.NoError(t, matcher.Record([]byte(key), hash))
	}
	root, count, _, err := matcher.Finish()
	require.NoError(t, err)
	require.Positive(t, count)
	return root[:]
}

type legacyAccount struct {
	addr  []byte
	slots [][]byte
}

func keccakSlice(b []byte) []byte {
	h := keccak.Sum256(b)
	return h[:]
}

func randBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	rng.Read(b)
	return b
}

func seedLegacyWorld(t *testing.T, rng *rand.Rand) (*legacyWorld, []legacyAccount, []byte) {
	t.Helper()
	var accounts []legacyAccount
	add := func(slots ...[]byte) {
		accounts = append(accounts, legacyAccount{addr: randBytes(rng, length.Addr), slots: slots})
	}
	sharingPrefix := func(nibbles int) {
		first := randBytes(rng, 32)
		want := unpackPath(keccakSlice(first), 64, nil)[:nibbles]
		for {
			next := randBytes(rng, 32)
			if bytes.Equal(unpackPath(keccakSlice(next), 64, nil)[:nibbles], want) {
				add(first, next)
				return
			}
		}
	}
	for range 120 {
		add()
	}
	for range 40 {
		add(randBytes(rng, 32))
	}
	for range 10 {
		sharingPrefix(1)
	}
	for range 3 {
		sharingPrefix(2)
	}
	for range 15 {
		slots := make([][]byte, 30)
		for i := range slots {
			slots[i] = randBytes(rng, 32)
		}
		add(slots...)
	}
	w := newLegacyWorld()
	t.Cleanup(w.trie.Release)
	for i, acc := range accounts {
		w.setAccount(acc.addr, uint64(i+1), uint64(1000+i))
		for j, slot := range acc.slots {
			w.setSlot(acc.addr, slot, uint64(j+1))
		}
	}
	root := w.process(t)
	return w, accounts, root
}

func TestConversion(t *testing.T) {
	t.Run("trie_matches_hex_patricia_root", func(t *testing.T) {
		w, accounts, root := seedLegacyWorld(t, rand.New(rand.NewSource(7)))
		records := map[string][]byte{}
		require.Equal(t, root, w.convert(t, nil, false, records))

		trieState, err := w.trie.EncodeCurrentState(nil)
		require.NoError(t, err)
		legacy := binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, 22), 11)
		legacy = append(binary.BigEndian.AppendUint16(legacy, uint16(len(trieState))), trieState...)
		state, err := ConvertLegacyState(legacy)
		require.NoError(t, err)
		blockNum, txNum, stateRoot, err := commitment.DecodeCommitmentV3State(state)
		require.NoError(t, err)
		require.Equal(t, root, stateRoot)
		require.Equal(t, uint64(11), blockNum)
		require.Equal(t, uint64(22), txNum)
		ctx := newMockContext()
		ctx.branches = records
		tr := &Trie{}
		tr.ResetContext(ctx)
		defer tr.Release()
		blockNum, txNum, err = tr.RestoreState(state)
		require.NoError(t, err)
		require.Equal(t, uint64(11), blockNum)
		require.Equal(t, uint64(22), txNum)
		restoredRoot, err := tr.RootHash()
		require.NoError(t, err)
		require.Equal(t, root, restoredRoot)
		op := commitmenttest.AccountAt(accounts[0].addr, commitmenttest.AccountSpec{Nonce: 12, Balance: 23, CodeHash: empty.CodeHash})
		w.apply(op)
		nextRoot, err := tr.Process(context.Background(), testUpdates(t, commitment.ModeCollect, []commitmenttest.Op{op}), "", nil, commitment.WarmupConfig{})
		require.NoError(t, err)
		require.Equal(t, w.process(t), nextRoot)
		require.Zero(t, ctx.accountCalls)
		require.Zero(t, ctx.storageCalls)
	})

	t.Run("incremental_tombstones_dropped_storage_roots", func(t *testing.T) {
		w, accounts, _ := seedLegacyWorld(t, rand.New(rand.NewSource(11)))
		records := map[string][]byte{}
		w.convert(t, nil, false, records)
		prevs := w.Records()
		dropSlots := func(acc legacyAccount, keep int) {
			for _, slot := range acc.slots[keep:] {
				w.apply(commitmenttest.DeleteAt(commitmenttest.SlotKey(acc.addr, slot)))
			}
		}
		for i, acc := range accounts {
			switch {
			case len(acc.slots) == 0 && i%2 == 0:
				w.setAccount(acc.addr, 7, uint64(i))
			case len(acc.slots) == 1 && i%2 == 0:
				dropSlots(acc, 0)
			case len(acc.slots) == 1:
				w.apply(commitmenttest.DeleteAt(acc.addr))
				dropSlots(acc, 0)
			case len(acc.slots) == 2:
				dropSlots(acc, 1)
			case len(acc.slots) > 2 && i%3 == 0:
			case len(acc.slots) > 2 && i%3 == 1:
				dropSlots(acc, 1)
			case len(acc.slots) > 2:
				dropSlots(acc, 0)
			}
		}
		root := w.process(t)
		require.Equal(t, root, w.convert(t, prevs, true, records))
		for _, acc := range accounts {
			if !slices.ContainsFunc(acc.slots, func(slot []byte) bool { return w.live(commitmenttest.SlotKey(acc.addr, slot)) }) {
				require.Empty(t, records[string(StorageNodeKey(keccak.Sum256(acc.addr), nil, nil))], "account %x has no storage but keeps a storage root record", acc.addr)
			}
		}
	})

	t.Run("drops_stale_storage_root_after_collapse", func(t *testing.T) {
		rng := rand.New(rand.NewSource(3))
		w := newLegacyWorld()
		t.Cleanup(w.trie.Release)
		for i := range 8 {
			w.setAccount(randBytes(rng, length.Addr), uint64(i+1), uint64(100+i))
		}
		owner := randBytes(rng, length.Addr)
		w.setAccount(owner, 1, 1)
		slots := [][]byte{randBytes(rng, 32)}
		for {
			next := randBytes(rng, 32)
			if keccakSlice(next)[0]>>4 != keccakSlice(slots[0])[0]>>4 {
				slots = append(slots, next)
				break
			}
		}
		for j, slot := range slots {
			w.setSlot(owner, slot, uint64(j+1))
		}
		w.process(t)
		storageRootKey := string(append([]byte{0}, keccakSlice(owner)...))
		stale := w.Records()[storageRootKey]
		require.NotEmpty(t, stale)

		w.apply(commitmenttest.DeleteAt(commitmenttest.SlotKey(owner, slots[1])))
		root := w.process(t)
		w.written = w.Records()
		w.written[storageRootKey] = stale
		require.Equal(t, root, w.convert(t, nil, false, map[string][]byte{}))
	})
}

func TestConvertLegacyStateRejectsLeafRoot(t *testing.T) {
	for _, slots := range []int{0, 3} {
		w := newLegacyWorld()
		addr := common.Address{19: 7}
		w.setAccount(addr[:], 1, 1000)
		for i := range slots {
			slot := common.Hash{0: byte(i + 1)}
			w.setSlot(addr[:], slot[:], uint64(i+5))
		}
		w.process(t)
		trieState, err := w.trie.EncodeCurrentState(nil)
		require.NoError(t, err)
		legacy := binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, 22), 11)
		legacy = append(binary.BigEndian.AppendUint16(legacy, uint16(len(trieState))), trieState...)
		_, err = ConvertLegacyState(legacy)
		require.ErrorIs(t, err, errLegacyLeafRoot, "slots=%d", slots)
	}
}
