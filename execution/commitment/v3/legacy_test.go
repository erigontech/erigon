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
	"maps"
	"math/rand"
	"slices"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

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
	w.apply(accountOp(addr, commitmenttest.AccountSpec{Nonce: nonce, Balance: balance, CodeHash: empty.CodeHash}))
}

func (w *legacyWorld) setSlot(addr, slot []byte, value uint64) {
	w.apply(commitmenttest.Op{Key: slotKey(addr, slot), Storage: bytes.TrimLeft(binary.BigEndian.AppendUint64(nil, value), "\x00")})
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
		op := accountOp(accounts[0].addr, commitmenttest.AccountSpec{Nonce: 12, Balance: 23, CodeHash: empty.CodeHash})
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
				w.apply(commitmenttest.Op{Key: slotKey(acc.addr, slot), Delete: true})
			}
		}
		for i, acc := range accounts {
			switch {
			case len(acc.slots) == 0 && i%2 == 0:
				w.setAccount(acc.addr, 7, uint64(i))
			case len(acc.slots) == 1 && i%2 == 0:
				dropSlots(acc, 0)
			case len(acc.slots) == 1:
				w.apply(commitmenttest.Op{Key: acc.addr, Delete: true})
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
			if !slices.ContainsFunc(acc.slots, func(slot []byte) bool { return w.live(slotKey(acc.addr, slot)) }) {
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

		w.apply(commitmenttest.Op{Key: slotKey(owner, slots[1]), Delete: true})
		root := w.process(t)
		w.written = w.Records()
		w.written[storageRootKey] = stale
		require.Equal(t, root, w.convert(t, nil, false, map[string][]byte{}))
	})
}
