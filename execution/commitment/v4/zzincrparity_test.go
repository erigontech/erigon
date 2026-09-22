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
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
)

type incrWorld struct {
	t        *testing.T
	v4       *Trie
	hph      *commitment.HexPatriciaHashed
	ctxV4    *parityContext
	ctxHPH   *parityContext
	accounts map[string]*commitment.Update
	storage  map[string]*commitment.Update
	history  []string
}

func newIncrWorld(t *testing.T) *incrWorld {
	t.Helper()
	w := &incrWorld{
		t:        t,
		ctxV4:    newParityContext(),
		ctxHPH:   newParityContext(),
		accounts: make(map[string]*commitment.Update),
		storage:  make(map[string]*commitment.Update),
	}
	w.v4 = &Trie{}
	w.v4.ResetContext(w.ctxV4)
	w.v4.SetTrieContextFactory(w.ctxV4.factory)
	w.hph = commitment.NewHexPatriciaHashed(length.Addr, w.ctxHPH, commitment.DefaultTrieConfig())
	t.Cleanup(func() {
		w.v4.Release()
		w.hph.Release()
	})
	return w
}

func (w *incrWorld) apply(entries []parityUpdate) {
	for _, e := range entries {
		switch {
		case len(e.key) == length.Addr && e.update.Deleted():
			delete(w.accounts, string(e.key))
			for k := range w.storage {
				if k[:length.Addr] == string(e.key) {
					delete(w.storage, k)
				}
			}
		case len(e.key) == length.Addr:
			w.accounts[string(e.key)] = e.update.Copy()
		case e.update.Deleted():
			delete(w.storage, string(e.key))
		default:
			w.storage[string(e.key)] = e.update.Copy()
		}
	}
	w.ctxHPH.mu.Lock()
	w.ctxHPH.accounts = make(map[string]*commitment.Update, len(w.accounts))
	for k, v := range w.accounts {
		w.ctxHPH.accounts[k] = v.Copy()
	}
	w.ctxHPH.storage = make(map[string]*commitment.Update, len(w.storage))
	for k, v := range w.storage {
		w.ctxHPH.storage[k] = v.Copy()
	}
	w.ctxHPH.mu.Unlock()
}

func (w *incrWorld) block(n int, entries []parityUpdate) {
	w.t.Helper()
	if len(entries) == 0 {
		return
	}
	w.apply(entries)
	ctx := context.Background()
	rootV4, err := w.v4.Process(ctx, makeParityUpdates(w.t, commitment.ModeCollect, entries), "", nil, commitment.WarmupConfig{})
	require.NoErrorf(w.t, err, "block %d v4", n)
	rootHPH, err := w.hph.Process(ctx, makeParityUpdates(w.t, commitment.ModeUpdate, entries), "", nil, commitment.WarmupConfig{})
	require.NoErrorf(w.t, err, "block %d hph", n)
	require.Zero(w.t, w.ctxV4.accountCalls)
	require.Zero(w.t, w.ctxV4.storageCalls)
	w.history = append(w.history, fmt.Sprintf("block %d:\n%s", n, formatEntries(entries)))
	if !bytes.Equal(rootHPH, rootV4) {
		truth := w.rebuildFromScratch()
		w.t.Fatalf("block %d root mismatch\n v4    %x\n hph   %x\n fresh %x (v4Right=%v hphRight=%v)\nhistory:\n%s",
			n, rootV4, rootHPH, truth, bytes.Equal(truth, rootV4), bytes.Equal(truth, rootHPH), strings.Join(w.history, ""))
	}
}

func (w *incrWorld) rebuildFromScratch() []byte {
	entries := make([]parityUpdate, 0, len(w.accounts)+len(w.storage))
	for k, v := range w.accounts {
		entries = append(entries, parityUpdate{key: []byte(k), update: v.Copy()})
	}
	for k, v := range w.storage {
		if _, ok := w.accounts[k[:length.Addr]]; !ok {
			continue
		}
		entries = append(entries, parityUpdate{key: []byte(k), update: v.Copy()})
	}
	if len(entries) == 0 {
		return nil
	}
	fresh := newIncrWorld(w.t)
	fresh.apply(entries)
	root, err := fresh.hph.Process(context.Background(), makeParityUpdates(w.t, commitment.ModeUpdate, entries), "", nil, commitment.WarmupConfig{})
	require.NoError(w.t, err)
	rootV4, err := fresh.v4.Process(context.Background(), makeParityUpdates(w.t, commitment.ModeCollect, entries), "", nil, commitment.WarmupConfig{})
	require.NoError(w.t, err)
	if !bytes.Equal(root, rootV4) {
		w.t.Fatalf("fresh rebuild disagrees: v4 %x hph %x", rootV4, root)
	}
	return root
}

func formatEntries(entries []parityUpdate) string {
	out := ""
	for _, e := range entries {
		out += fmt.Sprintf("  key=%x flags=%d nonce=%d bal=%s codeHash=%x storageLen=%d storage=%x\n",
			e.key, e.update.Flags, e.update.Nonce, e.update.Balance.String(), e.update.CodeHash,
			e.update.StorageLen, e.update.Storage[:e.update.StorageLen])
	}
	return out
}

func incrAddress(i int) []byte {
	addr := make([]byte, length.Addr)
	binary.BigEndian.PutUint64(addr[length.Addr-8:], uint64(i))
	return addr
}

func incrSlot(addr []byte, j int) []byte {
	key := make([]byte, 0, length.Addr+length.Hash)
	key = append(key, addr...)
	slot := make([]byte, length.Hash)
	binary.BigEndian.PutUint64(slot[length.Hash-8:], uint64(j))
	return append(key, slot...)
}

func incrAccountUpdate(rng *rand.Rand) *commitment.Update {
	u := &commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate}
	u.Nonce = uint64(rng.Intn(1 << 20))
	u.Balance = *uint256.NewInt(rng.Uint64())
	u.CodeHash = empty.CodeHash
	if rng.Intn(3) == 0 {
		u.CodeHash = common.BigToHash(uint256.NewInt(rng.Uint64()).ToBig())
	}
	return u
}

func incrStorageUpdate(rng *rand.Rand) *commitment.Update {
	u := &commitment.Update{Flags: commitment.StorageUpdate}
	n := 1 + rng.Intn(length.Hash)
	rng.Read(u.Storage[:n])
	u.Storage[0] |= 1
	u.StorageLen = int8(n)
	return u
}

func runIncrStress(t *testing.T, seed int64, blocks, addrs, slots, opsPerBlock int) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	w := newIncrWorld(t)
	live := make(map[string]struct{})
	for n := range blocks {
		count := 1 + rng.Intn(opsPerBlock)
		touched := make(map[string]struct{}, count)
		entries := make([]parityUpdate, 0, count)
		for range count {
			addr := incrAddress(rng.Intn(addrs))
			if _, busy := touched[string(addr)]; busy {
				continue
			}
			_, alive := live[string(addr)]
			switch {
			case !alive:
				touched[string(addr)] = struct{}{}
				live[string(addr)] = struct{}{}
				entries = append(entries, parityUpdate{key: addr, update: incrAccountUpdate(rng)})
			case rng.Intn(16) == 0:
				touched[string(addr)] = struct{}{}
				delete(live, string(addr))
				entries = append(entries, parityUpdate{key: addr, update: &commitment.Update{Flags: commitment.DeleteUpdate}})
			default:
				touched[string(addr)] = struct{}{}
				if rng.Intn(4) == 0 {
					entries = append(entries, parityUpdate{key: addr, update: incrAccountUpdate(rng)})
				}
				for range 1 + rng.Intn(3) {
					key := incrSlot(addr, rng.Intn(slots))
					if rng.Intn(5) == 0 {
						entries = append(entries, parityUpdate{key: key, update: &commitment.Update{Flags: commitment.DeleteUpdate}})
						continue
					}
					entries = append(entries, parityUpdate{key: key, update: incrStorageUpdate(rng)})
				}
			}
		}
		w.block(n, entries)
	}
}

func TestParityIncrementalStress(t *testing.T) {
	for _, tc := range []struct{ blocks, addrs, slots, ops int }{
		{blocks: 120, addrs: 6, slots: 4, ops: 3},
		{blocks: 120, addrs: 24, slots: 10, ops: 5},
		{blocks: 60, addrs: 200, slots: 40, ops: 8},
	} {
		t.Run(fmt.Sprintf("a%d_s%d", tc.addrs, tc.slots), func(t *testing.T) {
			for seed := int64(1); seed <= 4; seed++ {
				t.Run(fmt.Sprint(seed), func(t *testing.T) {
					runIncrStress(t, seed, tc.blocks, tc.addrs, tc.slots, tc.ops)
				})
			}
		})
	}
}

func TestParityRootExtensionSplitsAgainstStoredChild(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first []int
		then  []int
	}{
		{name: "account_root_extension", first: []int{0, 3}, then: []int{1}},
		{name: "account_root_extension_pair", first: []int{0, 3}, then: []int{1, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newIncrWorld(t)
			build := func(ids []int) []parityUpdate {
				entries := make([]parityUpdate, 0, len(ids))
				for _, i := range ids {
					u := &commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate}
					u.Nonce = uint64(i + 1)
					u.Balance = *uint256.NewInt(uint64(i + 1))
					u.CodeHash = empty.CodeHash
					entries = append(entries, parityUpdate{key: incrAddress(i), update: u})
				}
				return entries
			}
			w.block(0, build(tc.first))
			w.block(1, build(tc.then))
		})
	}
}

func TestParityAccountRootExtensionWithStoredChild(t *testing.T) {
	acct := func(i int) *commitment.Update {
		u := &commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate}
		u.Nonce = uint64(i + 1)
		u.Balance = *uint256.NewInt(uint64(i + 1))
		u.CodeHash = empty.CodeHash
		return u
	}
	storageOf := func(i, slot, value int) parityUpdate {
		u := &commitment.Update{Flags: commitment.StorageUpdate, StorageLen: 1}
		u.Storage[0] = byte(value)
		return parityUpdate{key: incrSlot(incrAddress(i), slot), update: u}
	}

	t.Run("insert_under_shared_prefix", func(t *testing.T) {
		w := newIncrWorld(t)
		w.block(0, []parityUpdate{{key: incrAddress(5), update: acct(5)}})
		w.block(1, []parityUpdate{{key: incrAddress(8), update: acct(8)}})
		w.block(2, []parityUpdate{{key: incrAddress(15), update: acct(15)}})
	})

	t.Run("update_keeps_storage_root", func(t *testing.T) {
		w := newIncrWorld(t)
		w.block(0, []parityUpdate{{key: incrAddress(5), update: acct(5)}, storageOf(5, 1, 0x11), storageOf(5, 2, 0x22)})
		w.block(1, []parityUpdate{{key: incrAddress(8), update: acct(8)}})
		w.block(2, []parityUpdate{{key: incrAddress(5), update: acct(50)}})
	})

	t.Run("delete_under_shared_prefix", func(t *testing.T) {
		w := newIncrWorld(t)
		w.block(0, []parityUpdate{{key: incrAddress(5), update: acct(5)}})
		w.block(1, []parityUpdate{{key: incrAddress(8), update: acct(8)}})
		w.block(2, []parityUpdate{{key: incrAddress(15), update: acct(15)}, {key: incrAddress(31), update: acct(31)}})
		w.block(3, []parityUpdate{{key: incrAddress(5), update: &commitment.Update{Flags: commitment.DeleteUpdate}}})
	})
}
