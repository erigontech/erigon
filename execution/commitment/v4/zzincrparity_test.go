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
	"sort"
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
	hphDrift int
}

func newIncrWorld(t *testing.T) *incrWorld {
	t.Helper()
	w := &incrWorld{
		t:        t,
		hphDrift: -1,
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

func (w *incrWorld) block(n int, entries []parityUpdate) bool {
	w.t.Helper()
	if len(entries) == 0 {
		return true
	}
	w.apply(entries)
	ctx := context.Background()
	rootV4, err := w.v4.Process(ctx, makeParityUpdates(w.t, commitment.ModeCollect, entries), "", nil, commitment.WarmupConfig{})
	require.NoErrorf(w.t, err, "block %d v4", n)
	require.Zero(w.t, w.ctxV4.accountCalls)
	require.Zero(w.t, w.ctxV4.storageCalls)
	w.checkRecords(n)
	w.history = append(w.history, fmt.Sprintf("block %d:\n%s", n, formatEntries(entries)))
	rootHPH, err := w.hph.Process(ctx, makeParityUpdates(w.t, commitment.ModeUpdate, entries), "", nil, commitment.WarmupConfig{})
	if err != nil {
		w.hphDrift = n
		return false
	}
	if bytes.Equal(rootHPH, rootV4) {
		return true
	}
	truth := w.rebuildFromScratch()
	if !bytes.Equal(truth, rootV4) {
		w.t.Fatalf("block %d: v4 root differs from a trie rebuilt from the same state\n v4    %x\n hph   %x\n fresh %x\nhistory:\n%s",
			n, rootV4, rootHPH, truth, strings.Join(w.history, ""))
	}
	w.hphDrift = n
	return false
}

func walkRecords(ctx *parityContext, plane byte, addrHash [32]byte) (leaves map[string][]byte, issues []string) {
	leaves = make(map[string][]byte)
	key := func(path []byte) []byte { return nodeKey(plane, addrHash[:], path, nil) }
	if plane == planeAccount {
		key = func(path []byte) []byte { return AccountNodeKey(path, nil) }
	}
	var visit func(path []byte)
	visit = func(path []byte) {
		data, _, _ := ctx.Branch(key(path))
		if len(data) == 0 {
			issues = append(issues, fmt.Sprintf("missing record at path %x", path))
			return
		}
		depth := len(path)
		if err := Validate(data, depth); err != nil {
			issues = append(issues, fmt.Sprintf("invalid record at %x: %v", path, err))
			return
		}
		r := NewRecord(data, depth)
		if r.isLeafRoot() {
			hashedKey, value := r.LeafRootBody()
			leaves[string(unpackPath(hashedKey, 64, nil))] = bytes.Clone(value)
			return
		}
		l := r.layout()
		if depth == 0 && l.selfExtLen != 0 {
			visit(unpackPath(r.SelfExt()[1:], l.selfExtLen, nil))
			return
		}
		for nib := range 16 {
			bit := uint16(1) << nib
			if l.child&bit == 0 {
				continue
			}
			if l.leaf&bit != 0 {
				suffix, value := r.leafAt(l, nib)
				full := append(append([]byte(nil), path...), byte(nib))
				full = append(full, unpackPath(suffix, 64-depth-1, nil)...)
				if _, dup := leaves[string(full)]; dup {
					issues = append(issues, fmt.Sprintf("duplicate leaf %x", full))
				}
				leaves[string(full)] = bytes.Clone(value)
				continue
			}
			childPath := append(append([]byte(nil), path...), byte(nib))
			if e := r.extAt(l, nib); len(e) != 0 {
				decoded, err := decodeExtension(e)
				if err != nil {
					issues = append(issues, fmt.Sprintf("bad extension at %x nibble %d: %v", path, nib, err))
					continue
				}
				childPath = append(childPath, decoded...)
			}
			visit(childPath)
		}
	}
	visit(nil)
	return leaves, issues
}

func (w *incrWorld) checkRecords(n int) {
	w.t.Helper()
	var noAddr [32]byte
	leaves, issues := walkRecords(w.ctxV4, planeAccount, noAddr)
	want := make(map[string]struct{}, len(w.accounts))
	for plain := range w.accounts {
		want[string(commitment.KeyToHexNibbleHash([]byte(plain)))] = struct{}{}
	}
	for k := range want {
		if _, ok := leaves[k]; !ok {
			issues = append(issues, fmt.Sprintf("account leaf %x is missing", k))
		}
	}
	for k := range leaves {
		if _, ok := want[k]; !ok {
			issues = append(issues, fmt.Sprintf("account leaf %x is unexpected", k))
		}
	}
	slots := make(map[string]map[string]struct{})
	for plain := range w.storage {
		if _, ok := w.accounts[plain[:length.Addr]]; !ok {
			continue
		}
		hashed := commitment.KeyToHexNibbleHash([]byte(plain))
		addr := string(hashed[:64])
		if slots[addr] == nil {
			slots[addr] = make(map[string]struct{})
		}
		slots[addr][string(hashed[64:])] = struct{}{}
	}
	for addr, wantSlots := range slots {
		got, storageIssues := walkRecords(w.ctxV4, planeStorage, hashAddressPath([]byte(addr)))
		issues = append(issues, storageIssues...)
		for k := range wantSlots {
			if _, ok := got[k]; !ok {
				issues = append(issues, fmt.Sprintf("storage leaf %x/%x is missing", addr, k))
			}
		}
		for k := range got {
			if _, ok := wantSlots[k]; !ok {
				issues = append(issues, fmt.Sprintf("storage leaf %x/%x is unexpected", addr, k))
			}
		}
	}
	if len(issues) != 0 {
		sort.Strings(issues)
		w.t.Fatalf("block %d record integrity: %d issues\n%s", n, len(issues), strings.Join(issues, "\n"))
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

func (w *incrWorld) mustBlock(n int, entries []parityUpdate) {
	w.t.Helper()
	require.Truef(w.t, w.block(n, entries), "block %d: HexPatriciaHashed drifted from ground truth", n)
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
		if !w.block(n, entries) {
			t.Logf("HexPatriciaHashed drifted from ground truth at block %d; v4 matched", w.hphDrift)
			return
		}
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
			w.mustBlock(0, build(tc.first))
			w.mustBlock(1, build(tc.then))
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
		w.mustBlock(0, []parityUpdate{{key: incrAddress(5), update: acct(5)}})
		w.mustBlock(1, []parityUpdate{{key: incrAddress(8), update: acct(8)}})
		w.mustBlock(2, []parityUpdate{{key: incrAddress(15), update: acct(15)}})
	})

	t.Run("update_keeps_storage_root", func(t *testing.T) {
		w := newIncrWorld(t)
		w.mustBlock(0, []parityUpdate{{key: incrAddress(5), update: acct(5)}, storageOf(5, 1, 0x11), storageOf(5, 2, 0x22)})
		w.mustBlock(1, []parityUpdate{{key: incrAddress(8), update: acct(8)}})
		w.mustBlock(2, []parityUpdate{{key: incrAddress(5), update: acct(50)}})
	})

	t.Run("delete_under_shared_prefix", func(t *testing.T) {
		w := newIncrWorld(t)
		w.mustBlock(0, []parityUpdate{{key: incrAddress(5), update: acct(5)}})
		w.mustBlock(1, []parityUpdate{{key: incrAddress(8), update: acct(8)}})
		w.mustBlock(2, []parityUpdate{{key: incrAddress(15), update: acct(15)}, {key: incrAddress(31), update: acct(31)}})
		w.mustBlock(3, []parityUpdate{{key: incrAddress(5), update: &commitment.Update{Flags: commitment.DeleteUpdate}}})
	})
}

func TestParityDeleteCollapsesBranchBelowRoot(t *testing.T) {
	acct := func(i int) *commitment.Update {
		u := &commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate}
		u.Nonce = uint64(i + 1)
		u.Balance = *uint256.NewInt(uint64(i + 1))
		u.CodeHash = empty.CodeHash
		return u
	}
	w := newIncrWorld(t)
	w.mustBlock(0, []parityUpdate{{key: incrAddress(5), update: acct(5)}})
	w.mustBlock(1, []parityUpdate{{key: incrAddress(8), update: acct(8)}})
	w.mustBlock(2, []parityUpdate{
		{key: incrAddress(15), update: acct(15)},
		{key: incrAddress(8), update: &commitment.Update{Flags: commitment.DeleteUpdate}},
	})
}
