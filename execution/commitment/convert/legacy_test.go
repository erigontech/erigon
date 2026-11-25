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

package convert

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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	v2 "github.com/erigontech/erigon/execution/commitment/v2"
	"github.com/erigontech/erigon/execution/commitment/v2/branch"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

type legacyWorld struct {
	*runner.Memory
	trie    *v2.HexPatriciaHashed
	written map[string][]byte
	touched map[string]struct{}
}

func newLegacyWorld() *legacyWorld {
	w := &legacyWorld{Memory: runner.NewMemory(runner.ContextSpec{}), touched: map[string]struct{}{}}
	w.trie = v2.NewHexPatriciaHashed(length.Addr, w, commitment.TrieConfig{})
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
	hasher, matcher := v3.NewRecordHasher(), v3.NewRecordMatcher(t.TempDir())
	t.Cleanup(matcher.Close)
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
	sharingPrefix := func(n int) {
		first := randBytes(rng, 32)
		want := nibbles.KeybytesToHex(crypto.Keccak256(first))[:n]
		for {
			next := randBytes(rng, 32)
			if bytes.Equal(nibbles.KeybytesToHex(crypto.Keccak256(next))[:n], want) {
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
	t.Run("rejects_invalid_branches", func(t *testing.T) {
		owner := common.Address{19: 1}
		other := common.Address{19: 2}
		slot := common.Hash{31: 1}
		ownerHash := keccak.Sum256(owner[:])
		slotHash := keccak.Sum256(slot[:])
		accountNib := int(ownerHash[0] >> 4)
		slotNib := int(slotHash[0] >> 4)
		storageKey := commitmenttest.SlotKey(other[:], slot[:])
		hash := bytes.Repeat([]byte{1}, 32)
		for _, tc := range []struct {
			name   string
			prefix []byte
			cells  map[int]branch.BranchCell
		}{
			{"storage_owner_mismatch", nibbles.KeybytesToHex(ownerHash[:])[:64], map[int]branch.BranchCell{
				slotNib:            {StorageAddr: storageKey},
				(slotNib + 1) % 16: {Hash: hash},
			}},
			{"child_beyond_hash", bytes.Repeat([]byte{1}, 62), map[int]branch.BranchCell{
				0: {Hash: hash, Extension: []byte{1}},
				1: {Hash: hash},
			}},
			{"single_child", []byte{1}, map[int]branch.BranchCell{0: {Hash: hash}}},
			{"account_storage_owner_mismatch", nil, map[int]branch.BranchCell{
				accountNib:            {AccountAddr: owner[:], StorageAddr: storageKey},
				(accountNib + 1) % 16: {Hash: hash},
			}},
			{"leaf_outside_prefix", []byte{ownerHash[0]>>4 ^ 1}, map[int]branch.BranchCell{
				int(ownerHash[0] & 15):          {AccountAddr: owner[:]},
				(int(ownerHash[0]&15) + 1) % 16: {Hash: hash},
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				memory := runner.NewMemory(runner.ContextSpec{})
				memory.Apply([]commitmenttest.Op{
					commitmenttest.AccountAt(owner[:], commitmenttest.AccountSpec{Nonce: 1, Balance: 1, CodeHash: empty.CodeHash}),
					{Key: storageKey, Storage: []byte{1}},
				})
				data := []byte{0, 0, 0, 0}
				var mask uint16
				for nib := range 16 {
					cell, ok := tc.cells[nib]
					if !ok {
						continue
					}
					mask |= 1 << nib
					fields := [][]byte{cell.Extension, cell.AccountAddr, cell.StorageAddr, cell.Hash}
					var flags byte
					for i, field := range fields {
						if len(field) != 0 {
							flags |= 1 << i
						}
					}
					data = append(data, flags)
					for _, field := range fields {
						if len(field) != 0 {
							data = append(binary.AppendUvarint(data, uint64(len(field))), field...)
						}
					}
				}
				binary.BigEndian.PutUint16(data, mask)
				binary.BigEndian.PutUint16(data[2:], mask)
				converter := NewLegacyConverter(legacyValues{memory}, false, false)
				err := converter.Convert(nibbles.HexToCompact(tc.prefix), data, nil, func(_, _ []byte, _ LegacyKind) error { return nil })
				require.ErrorIs(t, err, errLegacyBranch)
			})
		}
	})
	t.Run("rejects_conflicting_records", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			entries []LegacyEntry
		}{
			{"direct", []LegacyEntry{{Kind: LegacyDirect, Value: []byte{1}}, {Kind: LegacyDirect, Value: []byte{2}}}},
			{"synthesized", []LegacyEntry{{Kind: LegacySynthesized, Value: []byte{1}}, {Kind: LegacySynthesized, Value: []byte{2}}}},
			{"kept_tombstone", []LegacyEntry{{Kind: LegacyDirect}, {Kind: LegacyKeep}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, _, err := ResolveLegacy(v3.StorageNodeKey([32]byte{}, nil, nil), tc.entries)
				require.ErrorIs(t, err, ErrLegacyConflict)
			})
		}
	})
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
		ctx := runner.NewMemory(runner.ContextSpec{Borrowed: true})
		for key, value := range records {
			require.NoError(t, ctx.PutBranch([]byte(key), value, nil))
		}
		tr := &v3.Trie{}
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
		updates := commitment.NewUpdates(commitment.ModeCollect, t.TempDir(), commitment.KeyToHexNibbleHash)
		t.Cleanup(updates.Close)
		updates.TouchPlainKeyDirect(string(op.Key), runner.Update(op))
		nextRoot, err := tr.Process(context.Background(), updates, "", nil, commitment.WarmupConfig{})
		require.NoError(t, err)
		require.Equal(t, w.process(t), nextRoot)
		require.Zero(t, ctx.Counts().AccountReads)
		require.Zero(t, ctx.Counts().StorageReads)
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
				require.Empty(t, records[string(v3.StorageNodeKey(keccak.Sum256(acc.addr), nil, nil))], "account %x has no storage but keeps a storage root record", acc.addr)
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
			if crypto.Keccak256(next)[0]>>4 != crypto.Keccak256(slots[0])[0]>>4 {
				slots = append(slots, next)
				break
			}
		}
		for j, slot := range slots {
			w.setSlot(owner, slot, uint64(j+1))
		}
		w.process(t)
		storageRootKey := string(append([]byte{0}, crypto.Keccak256(owner)...))
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
