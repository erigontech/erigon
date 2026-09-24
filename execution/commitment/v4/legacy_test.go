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
	"encoding/binary"
	"maps"
	"math/rand"
	"slices"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type legacyWorld struct {
	branches map[string][]byte
	written  map[string][]byte
	accounts map[string]commitment.Update
	storage  map[string]commitment.Update
	trie     *commitment.HexPatriciaHashed
}

func newLegacyWorld() *legacyWorld {
	w := &legacyWorld{
		branches: map[string][]byte{},
		accounts: map[string]commitment.Update{},
		storage:  map[string]commitment.Update{},
	}
	w.trie = commitment.NewHexPatriciaHashed(length.Addr, w, commitment.TrieConfig{})
	return w
}

func (w *legacyWorld) Branch(key []byte) ([]byte, kv.Step, error) {
	return bytes.Clone(w.branches[string(key)]), 0, nil
}

func (w *legacyWorld) PutBranch(key, data, _ []byte) error {
	w.branches[string(key)] = bytes.Clone(data)
	w.written[string(key)] = bytes.Clone(data)
	return nil
}

func (w *legacyWorld) Account(key []byte) (*commitment.Update, error) {
	if u, ok := w.accounts[string(key)]; ok {
		return &u, nil
	}
	return &commitment.Update{Flags: commitment.DeleteUpdate}, nil
}

func (w *legacyWorld) Storage(key []byte) (*commitment.Update, error) {
	if u, ok := w.storage[string(key)]; ok {
		return &u, nil
	}
	return &commitment.Update{Flags: commitment.DeleteUpdate}, nil
}

type legacyValues legacyWorld

func (v *legacyValues) Account(key []byte) ([]byte, error) {
	u, ok := v.accounts[string(key)]
	if !ok {
		return nil, nil
	}
	acc := accounts.Account{Nonce: u.Nonce, Balance: u.Balance, CodeHash: accounts.EmptyCodeHash}
	if u.CodeHash != empty.CodeHash {
		acc.CodeHash = accounts.InternCodeHash(u.CodeHash)
	}
	return accounts.SerialiseV3(&acc), nil
}

func (v *legacyValues) Storage(key []byte) ([]byte, error) {
	u, ok := v.storage[string(key)]
	if !ok {
		return nil, nil
	}
	return bytes.Clone(u.Storage[:u.StorageLen]), nil
}

func (w *legacyWorld) setAccount(addr []byte, nonce, balance uint64) {
	w.accounts[string(addr)] = commitment.Update{
		Flags:    commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate,
		Nonce:    nonce,
		Balance:  *uint256.NewInt(balance),
		CodeHash: empty.CodeHash,
	}
}

func (w *legacyWorld) setSlot(addr, slot []byte, value uint64) {
	var u commitment.Update
	trimmed := bytes.TrimLeft(binary.BigEndian.AppendUint64(nil, value), "\x00")
	u.Flags = commitment.StorageUpdate
	u.StorageLen = int8(copy(u.Storage[:], trimmed))
	w.storage[string(append(bytes.Clone(addr), slot...))] = u
}

func (w *legacyWorld) process(t *testing.T, touched map[string]struct{}) []byte {
	t.Helper()
	w.written = map[string][]byte{}
	batch := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash)
	for _, key := range slices.Sorted(maps.Keys(touched)) {
		var u commitment.Update
		switch stored, ok := w.accounts[key]; {
		case len(key) == length.Addr && ok:
			u = stored
		case len(key) == length.Addr:
			u.Flags = commitment.DeleteUpdate
		default:
			if stored, ok := w.storage[key]; ok {
				u = stored
			} else {
				u.Flags = commitment.DeleteUpdate
			}
		}
		batch.TouchPlainKeyDirect(key, &u)
	}
	root, err := w.trie.Process(context.Background(), batch, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	return root
}

func (w *legacyWorld) legacyState(t *testing.T, blockNum, txNum uint64) []byte {
	t.Helper()
	trieState, err := w.trie.EncodeCurrentState(nil)
	require.NoError(t, err)
	out := binary.BigEndian.AppendUint64(nil, txNum)
	out = binary.BigEndian.AppendUint64(out, blockNum)
	out = binary.BigEndian.AppendUint16(out, uint16(len(trieState)))
	return append(out, trieState...)
}

func convertLegacyRound(t *testing.T, w *legacyWorld, prevs map[string][]byte, incremental bool, records map[string][]byte) {
	t.Helper()
	grouped := map[string][]LegacyEntry{}
	conv := NewLegacyConverter((*legacyValues)(w), false, incremental)
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
}

func verifyRecords(t *testing.T, records map[string][]byte) ([]byte, uint64) {
	t.Helper()
	hasher, matcher := NewRecordHasher(), NewRecordMatcher()
	for _, key := range slices.Sorted(maps.Keys(records)) {
		if len(records[key]) == 0 || key == string(StateKey()) {
			continue
		}
		hash, err := hasher.Hash([]byte(key), records[key], matcher.Expect)
		require.NoError(t, err)
		require.NoError(t, matcher.Record([]byte(key), hash))
	}
	root, count, _, err := matcher.Finish()
	require.NoError(t, err)
	return root[:], count
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

func slotsSharingPrefix(rng *rand.Rand, nibbles int) [][]byte {
	first := randBytes(rng, 32)
	want := unpackPath(keccakSlice(first), 64, nil)[:nibbles]
	for {
		next := randBytes(rng, 32)
		if bytes.Equal(unpackPath(keccakSlice(next), 64, nil)[:nibbles], want) {
			return [][]byte{first, next}
		}
	}
}

func buildLegacyAccounts(rng *rand.Rand) []legacyAccount {
	var out []legacyAccount
	add := func(slots [][]byte) {
		out = append(out, legacyAccount{addr: randBytes(rng, length.Addr), slots: slots})
	}
	for range 120 {
		add(nil)
	}
	for range 40 {
		add([][]byte{randBytes(rng, 32)})
	}
	for range 10 {
		add(slotsSharingPrefix(rng, 1))
	}
	for range 3 {
		add(slotsSharingPrefix(rng, 2))
	}
	for range 15 {
		slots := make([][]byte, 30)
		for i := range slots {
			slots[i] = randBytes(rng, 32)
		}
		add(slots)
	}
	return out
}

func seedLegacyWorld(t *testing.T, rng *rand.Rand) (*legacyWorld, []legacyAccount, []byte) {
	t.Helper()
	w := newLegacyWorld()
	accounts := buildLegacyAccounts(rng)
	touched := map[string]struct{}{}
	for i, acc := range accounts {
		w.setAccount(acc.addr, uint64(i+1), uint64(1000+i))
		touched[string(acc.addr)] = struct{}{}
		for j, slot := range acc.slots {
			w.setSlot(acc.addr, slot, uint64(j+1))
			touched[string(append(bytes.Clone(acc.addr), slot...))] = struct{}{}
		}
	}
	root := w.process(t, touched)
	return w, accounts, root
}

func TestConvertLegacyTrieMatchesHexPatriciaRoot(t *testing.T) {
	w, _, root := seedLegacyWorld(t, rand.New(rand.NewSource(7)))

	records := map[string][]byte{}
	convertLegacyRound(t, w, nil, false, records)
	got, count := verifyRecords(t, records)
	require.Equal(t, root, got)
	require.Positive(t, count)

	state, err := ConvertLegacyState(w.legacyState(t, 11, 22))
	require.NoError(t, err)
	blockNum, txNum, stateRoot, err := DecodeState(state)
	require.NoError(t, err)
	require.Equal(t, root, stateRoot)
	require.Equal(t, uint64(11), blockNum)
	require.Equal(t, uint64(22), txNum)
}

func TestConvertLegacyIncrementalTombstonesDroppedStorageRoots(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	w, accounts, _ := seedLegacyWorld(t, rng)
	records := map[string][]byte{}
	convertLegacyRound(t, w, nil, false, records)
	prevs := maps.Clone(w.branches)

	touched := map[string]struct{}{}
	dropSlots := func(acc legacyAccount, keep int) {
		for _, slot := range acc.slots[keep:] {
			key := string(append(bytes.Clone(acc.addr), slot...))
			delete(w.storage, key)
			touched[key] = struct{}{}
		}
	}
	for i, acc := range accounts {
		switch {
		case len(acc.slots) == 0 && i%2 == 0:
			w.setAccount(acc.addr, 7, uint64(i))
			touched[string(acc.addr)] = struct{}{}
		case len(acc.slots) == 1 && i%2 == 0:
			dropSlots(acc, 0)
		case len(acc.slots) == 1:
			delete(w.accounts, string(acc.addr))
			touched[string(acc.addr)] = struct{}{}
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
	root := w.process(t, touched)

	convertLegacyRound(t, w, prevs, true, records)
	got, _ := verifyRecords(t, records)
	require.Equal(t, root, got)

	for _, acc := range accounts {
		hasStorage := false
		for _, slot := range acc.slots {
			_, ok := w.storage[string(append(bytes.Clone(acc.addr), slot...))]
			hasStorage = hasStorage || ok
		}
		if !hasStorage {
			require.Empty(t, records[string(StorageRootKey(keccak.Sum256(acc.addr)))], "account %x has no storage but keeps a storage root record", acc.addr)
		}
	}
}

func TestConvertLegacyDropsStaleStorageRootAfterCollapse(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	w := newLegacyWorld()
	touched := map[string]struct{}{}
	for i := range 8 {
		addr := randBytes(rng, length.Addr)
		w.setAccount(addr, uint64(i+1), uint64(100+i))
		touched[string(addr)] = struct{}{}
	}
	owner := randBytes(rng, length.Addr)
	w.setAccount(owner, 1, 1)
	touched[string(owner)] = struct{}{}
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
		touched[string(append(bytes.Clone(owner), slot...))] = struct{}{}
	}
	w.process(t, touched)
	storageRootKey := string(append([]byte{0}, keccakSlice(owner)...))
	stale := w.branches[storageRootKey]
	require.NotEmpty(t, stale)

	dropped := string(append(bytes.Clone(owner), slots[1]...))
	delete(w.storage, dropped)
	root := w.process(t, map[string]struct{}{dropped: {}})

	w.written = maps.Clone(w.branches)
	w.written[storageRootKey] = stale
	records := map[string][]byte{}
	convertLegacyRound(t, w, nil, false, records)
	got, _ := verifyRecords(t, records)
	require.Equal(t, root, got)
}
