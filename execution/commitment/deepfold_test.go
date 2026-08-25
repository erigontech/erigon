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

package commitment

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"math/rand"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
)

// keepWholeNibble: multi-slot branch survivor vs single leaf survivor.
func whaleSurvivorCorpus(keepWholeNibble bool) (pk [][]byte, upds []Update, k2 [][]byte, u2 []Update) {
	var addr []byte
	var groups [16][]storKV
	addr, _, _, _, pk, upds, groups = whaleByNibble(30_000)

	surv := -1
	for x := range 16 {
		if len(groups[x]) >= 2 {
			surv = x
			break
		}
	}

	k2 = [][]byte{addr}
	u2 = []Update{{Flags: BalanceUpdate | NonceUpdate}}
	u2[0].Balance.SetUint64(99)
	u2[0].Nonce = 7
	for x := range 16 {
		for i := range groups[x] {
			kv := &groups[x][i]
			if x == surv && (keepWholeNibble || i == 0) {
				continue
			}
			k2 = append(k2, kv.pk)
			u2 = append(u2, Update{Flags: DeleteUpdate})
		}
	}
	return pk, upds, k2, u2
}

// setAccountStorageRoot must shed a reused cell's stale storage identity.
func TestDeepFold_InjectedRootClearsStaleStorage(t *testing.T) {
	t.Parallel()
	hph := NewHexPatriciaHashed(length.Addr, NewMockState(t), DefaultTrieConfig())

	staleAddr := common.HexToAddress("0x00000000000000000000000000000000deadbeef")
	staleLoc := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	addStorageToCell(&hph.root, staleAddr, staleLoc, []byte{0xAA, 0xBB, 0xCC, 0xDD})
	require.NotZero(t, hph.root.storageAddrLen, "precondition: root holds a stale storage addr")
	require.True(t, hph.root.loaded.storage(), "precondition: root is flagged storage-loaded")

	acct := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	setAccountStorageRoot(hph, KeyToHexNibbleHash(acct[:]), cell{hash: common.HexToHash("0x1234"), hashLen: 32})

	require.Zerof(t, hph.root.storageAddrLen,
		"injecting a storage root must clear the stale storage plain key (got len=%d)", hph.root.storageAddrLen)
	require.Falsef(t, hph.root.loaded.storage(),
		"injecting a storage root must clear the stale storage-loaded flag")
}

func TestDeepFold_InjectedStorageRootWins(t *testing.T) {
	t.Parallel()

	acct := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	accHashed := KeyToHexNibbleHash(acct[:])
	accUpd := Update{Flags: BalanceUpdate | NonceUpdate}
	accUpd.Balance.SetUint64(1_000_000)
	accUpd.Nonce = 3

	injectedSR := common.HexToHash("0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")

	clean := NewHexPatriciaHashed(length.Addr, NewMockState(t), DefaultTrieConfig())
	clean.updateCell(acct[:], accHashed, &accUpd)
	setAccountStorageRoot(clean, accHashed, cell{hash: injectedSR, hashLen: 32})
	cleanHash, err := clean.computeCellHash(&clean.root, 0, nil)
	require.NoError(t, err)

	staleAddr := common.HexToAddress("0x00000000000000000000000000000000deadbeef")
	staleLoc := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	stale := NewHexPatriciaHashed(length.Addr, NewMockState(t), DefaultTrieConfig())
	addStorageToCell(&stale.root, staleAddr, staleLoc, []byte{0xAA, 0xBB, 0xCC, 0xDD})
	stale.updateCell(acct[:], accHashed, &accUpd)
	setAccountStorageRoot(stale, accHashed, cell{hash: injectedSR, hashLen: 32})
	staleHash, err := stale.computeCellHash(&stale.root, 0, nil)
	require.NoError(t, err)

	require.Equal(t, cleanHash, staleHash,
		"account hash must be driven by the injected storage root, not a stale storage slot")
}

// A hash-only storage-root injection must clear a stale single-child extension.
func TestDeepFold_HashOnlyRootClearsStaleExtension(t *testing.T) {
	t.Parallel()

	acct := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	accHashed := KeyToHexNibbleHash(acct[:])
	accUpd := Update{Flags: BalanceUpdate | NonceUpdate}
	accUpd.Balance.SetUint64(1_000_000)
	accUpd.Nonce = 3

	var singleChildSR cell
	singleChildSR.hash = common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	singleChildSR.hashLen = 32
	singleChildSR.extLen = 3
	singleChildSR.extension[0], singleChildSR.extension[1], singleChildSR.extension[2] = 0x0a, 0x0b, 0x0c

	var multiSR cell
	multiSR.hash = common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")
	multiSR.hashLen = 32

	reused := NewHexPatriciaHashed(length.Addr, NewMockState(t), DefaultTrieConfig())
	reused.updateCell(acct[:], accHashed, &accUpd)
	setAccountStorageRoot(reused, accHashed, singleChildSR)
	require.NotZero(t, reused.root.extLen, "single-child collapse must set the storage extension")
	setAccountStorageRoot(reused, accHashed, multiSR)
	require.Zerof(t, reused.root.extLen,
		"a hash-only storage root must clear the prior single-child extension (got len=%d)", reused.root.extLen)
	reusedHash, err := reused.computeCellHash(&reused.root, 0, nil)
	require.NoError(t, err)

	clean := NewHexPatriciaHashed(length.Addr, NewMockState(t), DefaultTrieConfig())
	clean.updateCell(acct[:], accHashed, &accUpd)
	setAccountStorageRoot(clean, accHashed, multiSR)
	cleanHash, err := clean.computeCellHash(&clean.root, 0, nil)
	require.NoError(t, err)

	require.Equal(t, cleanHash, reusedHash,
		"a reused cell's stale storage extension must not change the account hash")
}

type engineBatch struct {
	keys [][]byte
	upds []Update
}

func runEngineBatches(t *testing.T, mode runMode, workers int, batches []engineBatch) ([][]byte, *MockState) {
	t.Helper()
	ms := NewMockState(t)
	if mode != modeSeq {
		ms.SetConcurrentCommitment(true)
	}
	roots := make([][]byte, len(batches))
	var blob []byte
	for i, b := range batches {
		roots[i], blob = processModeBatchState(t, ms, mode, workers, b.keys, b.upds, blob)
	}
	return roots, ms
}

// Re-touching an extension-topped subtree must not desync the stitched cell's extension.
func TestStreaming_ExtensionToppedMountSplit(t *testing.T) {
	t.Parallel()

	a := findAddressForHexPrefix([]byte{7, 0xa, 1}, 1)
	b := findAddressForHexPrefix([]byte{7, 0xa, 2}, 2)

	seed := NewUpdateBuilder()
	seed.Balance(addrHex(a), 10)
	seed.Balance(addrHex(b), 20)
	for n := range 16 {
		if n == 7 {
			continue
		}
		seed.Balance(addrHex(findAddressForNibble(n, 100+n)), uint64(1000+n))
	}
	k1, u1 := seed.Build()

	retouch := func(bal1, bal2 uint64) engineBatch {
		ub := NewUpdateBuilder()
		ub.Balance(addrHex(a), bal1)
		ub.Balance(addrHex(b), bal2)
		k, u := ub.Build()
		return engineBatch{k, u}
	}

	batches := []engineBatch{
		{k1, u1},
		retouch(11, 22),
		retouch(12, 23),
	}

	seqRoots, seqMs := runEngineBatches(t, modeSeq, 0, batches)
	for _, tc := range []struct {
		name string
		mode runMode
	}{
		{"parallel", modeParallel},
	} {
		for _, w := range []int{1, 4, 8} {
			roots, ms := runEngineBatches(t, tc.mode, w, batches)
			for i := range batches {
				require.Equalf(t, seqRoots[i], roots[i], "%s(workers=%d) batch %d root != sequential", tc.name, w, i+1)
			}
			requireBranchParity(t, seqMs, ms)
		}
	}
}

// Must not prepend the 64-nibble account prefix, which would overflow cell.extension.
func TestDeepFold_BranchSurvivorCollapse(t *testing.T) {
	t.Parallel()
	wk1, wu1, wk2, wu2 := whaleSurvivorCorpus(true)
	mk, mu := buildMixedCorpus(0xC0FFEE, 4000)
	k1 := append(append([][]byte{}, mk...), wk1...)
	u1 := append(append([]Update{}, mu...), wu1...)
	for _, w := range []int{1, 4, 8} {
		requireAllEnginesParity(t, k1, u1, wk2, wu2, w)
	}
}

// Leaf hash must be the storage root, not the leaf cell's zero hash.
func TestDeepFold_LeafSurvivorCollapse(t *testing.T) {
	t.Parallel()
	wk1, wu1, wk2, wu2 := whaleSurvivorCorpus(false)
	mk, mu := buildMixedCorpus(0x5EED, 3000)
	k1 := append(append([][]byte{}, mk...), wk1...)
	u1 := append(append([]Update{}, mu...), wu1...)
	for _, w := range []int{1, 4, 8} {
		requireAllEnginesParity(t, k1, u1, wk2, wu2, w)
	}
}

// Account-only re-touch must keep the singleton's storage slot; cross-engine parity is
// blind to this since a shared updateCell bug drops it identically everywhere.
func TestSingletonAccountOnlyRetouchKeepsStorage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	a := hex.EncodeToString(findAddressForNibble(3, 4242))
	loc := "00000000000000000000000000000000000000000000000000000000000000aa"
	val := "00000000000000000000000000000000000000000000000000000000cafebabe"

	ms := NewMockState(t)
	tr := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())

	ub1 := NewUpdateBuilder().Balance(a, 100)
	ub1.Storage(a, loc, val)
	k1, u1 := ub1.Build()
	require.NoError(t, ms.applyPlainUpdates(k1, u1))
	ut1 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, k1, u1)
	_, err := tr.Process(ctx, ut1, "", nil, WarmupConfig{})
	require.NoError(t, err)
	ut1.Close()

	k2, u2 := NewUpdateBuilder().Balance(a, 200).Build()
	require.NoError(t, ms.applyPlainUpdates(k2, u2))
	ut2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, k2, u2)
	got, err := tr.Process(ctx, ut2, "", nil, WarmupConfig{})
	require.NoError(t, err)
	got = bytes.Clone(got)
	ut2.Close()

	msr := NewMockState(t)
	trr := NewHexPatriciaHashed(length.Addr, msr, DefaultTrieConfig())
	ubr := NewUpdateBuilder().Balance(a, 200)
	ubr.Storage(a, loc, val)
	kr, ur := ubr.Build()
	require.NoError(t, msr.applyPlainUpdates(kr, ur))
	utr := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, kr, ur)
	want, err := trr.Process(ctx, utr, "", nil, WarmupConfig{})
	require.NoError(t, err)
	utr.Close()

	require.Equal(t, want, got, "account-only re-touch dropped the singleton's storage slot")
}

// wide-minus-touch nibbles stay untouched on disk and must survive batch 2.
func buildSubsetTouchedWhale(seed int64, wide, touch []byte, perNibble1, perNibble2 int) (k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update) {
	rnd := rand.New(rand.NewSource(seed))
	addr := make([]byte, length.Addr)
	rnd.Read(addr)
	a := hex.EncodeToString(addr)

	firstStorageNibble := func(loc []byte) byte {
		pk := make([]byte, 0, length.Addr+len(loc))
		pk = append(pk, addr...)
		pk = append(pk, loc...)
		return KeyToHexNibbleHash(pk)[64]
	}
	genSlot := func(want byte) (string, string) {
		for {
			loc := make([]byte, length.Hash)
			rnd.Read(loc)
			if firstStorageNibble(loc) == want {
				val := make([]byte, 32)
				rnd.Read(val)
				return hex.EncodeToString(loc), hex.EncodeToString(val)
			}
		}
	}

	ub1 := NewUpdateBuilder()
	ub1.Balance(a, 1)
	for _, n := range wide {
		for range perNibble1 {
			l, v := genSlot(n)
			ub1.Storage(a, l, v)
		}
	}
	k1, u1 = ub1.Build()

	ub2 := NewUpdateBuilder()
	ub2.Balance(a, 2)
	for _, n := range touch {
		for range perNibble2 {
			l, v := genSlot(n)
			ub2.Storage(a, l, v)
		}
	}
	k2, u2 = ub2.Build()
	return k1, u1, k2, u2
}

// Deep-folding a touched subset must preserve the untouched on-disk first-nibble siblings.
func TestDeepFold_PreExistingWhale_SubsetTouched(t *testing.T) {
	wide := nibs(0, 1, 2, 3, 4, 5, 6, 7)
	touch := nibs(0, 1, 2)
	k1, u1, k2, u2 := buildSubsetTouchedWhale(20260622, wide, touch, 60, 420)
	fk, fu := buildMixedCorpus(7777, 200)
	k1 = append(append([][]byte{}, fk...), k1...)
	u1 = append(append([]Update{}, fu...), u1...)
	requireAllEnginesParity(t, k1, u1, k2, u2, 4)
}

// Single-nibble on-disk storage has no branch record at the account prefix; unfoldStorageBase
// must still recover it rather than seeding an empty base.
func TestDeepFold_PreExistingWhale_SingleNibbleOnDisk(t *testing.T) {
	onDisk := nibs(0)
	touch := nibs(3, 7)
	k1, u1, k2, u2 := buildSubsetTouchedWhale(20260702, onDisk, touch, 120, 700)
	fk, fu := buildMixedCorpus(4242, 200)
	k1 = append(append([][]byte{}, fk...), k1...)
	u1 = append(append([]Update{}, fu...), u1...)
	requireAllEnginesParity(t, k1, u1, k2, u2, 4)
}

// A fresh whale has nothing on disk beneath its prefix, so the fold must run concurrently.
func TestDeepFold_FreshWhaleFoldsParallel(t *testing.T) {
	k1, u1, _, _ := buildSubsetTouchedWhale(20260707, nibs(3, 7), nil, 700, 0)
	fk, fu := buildMixedCorpus(555, 200)
	keys := append(append([][]byte{}, fk...), k1...)
	upds := append(append([]Update{}, fu...), u1...)

	seqRoot, _ := engineRoot(t, modeSeq, 0, keys, upds)

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	parRoot, _, deepFolds := parallelBatchDeepFolds(t, ms, 4, keys, upds, nil)
	require.Equal(t, seqRoot, parRoot)
	require.Positive(t, deepFolds, "a fresh whale must take the concurrent deep fold, not the serial demotion")
}

func TestDeepFold_ExistingWhaleStillDemotes(t *testing.T) {
	k1, u1, k2, u2 := buildSubsetTouchedWhale(20260708, nibs(0), nibs(3, 7), 1, 700)
	fk, fu := buildMixedCorpus(556, 200)
	k1 = append(append([][]byte{}, fk...), k1...)
	u1 = append(append([]Update{}, fu...), u1...)

	seqRoot, _ := incrementalRoot(t, modeSeq, 0, k1, u1, k2, u2)

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	_, blob, _ := parallelBatchDeepFolds(t, ms, 4, k1, u1, nil)
	parRoot, _, deepFolds := parallelBatchDeepFolds(t, ms, 4, k2, u2, blob)
	require.Equal(t, seqRoot, parRoot)
	require.Zero(t, deepFolds, "an account present in the pre-state must keep the serial demotion")
}

// A streaming collapse leaves an afterMap==0 tombstone at the account prefix, distinct from
// the deep fold's outright branch-key delete; unfoldStorageBase must not seed an empty base
// from it, or a later re-expansion drops the untouched survivor.
func TestDeepFold_SingleSlotCollapseThenDeepReexpand(t *testing.T) {
	t.Parallel()

	const seed = 100
	addr, _, _, _, wk1, wu1, groups := whaleByNibble(seed)
	a := hex.EncodeToString(addr)

	survNib := -1
	for x := range 16 {
		if len(groups[x]) >= 1 {
			survNib = x
			break
		}
	}
	require.GreaterOrEqual(t, survNib, 0, "need a survivor nibble")

	mk, mu := buildMixedCorpus(0xC0FFEE, 4000)
	k1 := append(append([][]byte{}, mk...), wk1...)
	u1 := append(append([]Update{}, mu...), wu1...)

	wk2 := [][]byte{addr}
	wu2 := []Update{{Flags: BalanceUpdate | NonceUpdate}}
	wu2[0].Balance.SetUint64(99)
	wu2[0].Nonce = 7
	deleted := 0
	kept := false
	for x := range 16 {
		for _, kv := range groups[x] {
			if !kept {
				kept = true
				continue
			}
			wk2 = append(wk2, kv.pk)
			wu2 = append(wu2, Update{Flags: DeleteUpdate})
			deleted++
		}
	}
	require.True(t, kept, "need a survivor slot")
	require.LessOrEqual(t, deleted, int(deepStorageThreshold), "collapse must stream, not deep-fold")

	b3 := NewUpdateBuilder()
	b3.Balance(a, 200)
	added := 0
	for nib := 0; nib < 16 && added <= int(deepStorageThreshold)+200; nib++ {
		if nib == survNib {
			continue
		}
		for _, loc := range storageLocsForNibble(byte(nib), 200, nib*1_000_003+7) {
			b3.Storage(a, loc, "01")
			added++
		}
	}
	require.Greater(t, added, int(deepStorageThreshold), "re-expansion must cross the deep-fold threshold")
	wk3, wu3 := b3.Build()

	batches := []engineBatch{{k1, u1}, {wk2, wu2}, {wk3, wu3}}

	seqRoots, seqMs := runEngineBatches(t, modeSeq, 0, batches)
	for _, tc := range []struct {
		name string
		mode runMode
	}{
		{"parallel", modeParallel},
	} {
		for _, w := range []int{1, 4, 8} {
			roots, ms := runEngineBatches(t, tc.mode, w, batches)
			for i := range batches {
				require.Equalf(t, seqRoots[i], roots[i], "%s(workers=%d) batch %d root != sequential", tc.name, w, i+1)
			}
			requireBranchParity(t, seqMs, ms)
		}
	}
}

// A single-child collapse persists the storage root as a bare hash; a later re-touch must
// not demand a branch record the collapse never wrote.
func TestDeepFold_SurvivorCollapseThenRetouch(t *testing.T) {
	t.Parallel()

	addr, _, _, _, wk1, wu1, groups := whaleByNibble(30_000)

	surv := -1
	for x := range 16 {
		if len(groups[x]) >= 2 {
			surv = x
			break
		}
	}
	require.GreaterOrEqual(t, surv, 0, "need a survivor nibble with >=2 slots")

	wk2 := [][]byte{addr}
	wu2 := []Update{{Flags: BalanceUpdate | NonceUpdate}}
	wu2[0].Balance.SetUint64(99)
	wu2[0].Nonce = 7
	var reAdd storKV
	haveReAdd := false
	for x := range 16 {
		if x == surv {
			continue
		}
		for _, kv := range groups[x] {
			wk2 = append(wk2, kv.pk)
			wu2 = append(wu2, Update{Flags: DeleteUpdate})
			if !haveReAdd {
				reAdd = kv
				haveReAdd = true
			}
		}
	}
	require.True(t, haveReAdd, "need a deleted slot to re-add")

	wk3 := [][]byte{reAdd.pk}
	wu3 := []Update{reAdd.upd}

	mk, mu := buildMixedCorpus(0xC0FFEE, 4000)
	k1 := append(append([][]byte{}, mk...), wk1...)
	u1 := append(append([]Update{}, mu...), wu1...)

	batches := []engineBatch{{k1, u1}, {wk2, wu2}, {wk3, wu3}}

	seqRoots, seqMs := runEngineBatches(t, modeSeq, 0, batches)
	for _, tc := range []struct {
		name string
		mode runMode
	}{
		{"parallel", modeParallel},
	} {
		for _, w := range []int{1, 4, 8} {
			roots, ms := runEngineBatches(t, tc.mode, w, batches)
			for i := range batches {
				require.Equalf(t, seqRoots[i], roots[i], "%s(workers=%d) batch %d root != sequential", tc.name, w, i+1)
			}
			requireBranchParity(t, seqMs, ms)
		}
	}
}

// Emptying storage entirely must not persist a stored empty.RootHash on the account leaf
// (hashLen 0 instead), or a later re-populate descends into a branch record that no longer exists.
func TestDeepFold_EmptyStorageThenRepopulate(t *testing.T) {
	t.Parallel()

	w := findAddressForHexPrefix([]byte{7, 8, 1}, 101)
	s1 := findAddressForHexPrefix([]byte{7, 8, 2}, 102)
	s2 := findAddressForHexPrefix([]byte{7, 8, 3}, 103)
	f0 := findAddressForHexPrefix([]byte{0}, 104)
	ff := findAddressForHexPrefix([]byte{0xf}, 105)

	const slots = 1500

	locs := make([]string, slots)
	for i := range locs {
		locs[i] = common.Bytes2Hex(slotHashBytes(i))
	}

	b1 := NewUpdateBuilder().
		Balance(addrHex(w), 100).Balance(addrHex(s1), 5).Balance(addrHex(s2), 6).
		Balance(addrHex(f0), 7).Balance(addrHex(ff), 8)
	for _, loc := range locs {
		b1.Storage(addrHex(w), loc, "01")
	}
	k1, u1 := b1.Build()

	b2 := NewUpdateBuilder().Balance(addrHex(w), 200)
	for _, loc := range locs {
		b2.DeleteStorage(addrHex(w), loc)
	}
	k2, u2 := b2.Build()

	b3 := NewUpdateBuilder()
	for _, loc := range locs {
		b3.Storage(addrHex(w), loc, "02")
	}
	k3, u3 := b3.Build()

	batches := []engineBatch{{k1, u1}, {k2, u2}, {k3, u3}}

	seqRoots, seqMs := runEngineBatches(t, modeSeq, 0, batches)
	for _, tc := range []struct {
		name string
		mode runMode
	}{
		{"parallel", modeParallel},
	} {
		for _, w := range []int{1, 4, 8} {
			roots, ms := runEngineBatches(t, tc.mode, w, batches)
			for i := range batches {
				require.Equalf(t, seqRoots[i], roots[i], "%s(workers=%d) batch %d root != sequential", tc.name, w, i+1)
			}
			requireBranchParity(t, seqMs, ms)
		}
	}
}

func storageLocsForNibble(nibble byte, n, seed int) []string {
	out := make([]string, 0, n)
	var s [32]byte
	for i := seed; len(out) < n; i++ {
		binary.BigEndian.PutUint64(s[24:], uint64(i))
		h := keccak.Sum256(s[:])
		if (h[0]>>4)&0xf == nibble {
			out = append(out, common.Bytes2Hex(s[:]))
		}
	}
	return out
}

// The survivor decoded from disk carries a cached stateHash but no loaded value;
// setAccountStorageRoot must not drop that cached hash.
func TestDeepFold_SingleSlotSurvivorNotLoaded(t *testing.T) {
	t.Parallel()

	w := findAddressForHexPrefix([]byte{7, 8, 1}, 201)
	s1 := findAddressForHexPrefix([]byte{7, 8, 2}, 202)
	f0 := findAddressForHexPrefix([]byte{0}, 203)
	ff := findAddressForHexPrefix([]byte{0xf}, 204)

	survLoc := storageLocsForNibble(0x1, 1, 1)[0]
	delB := storageLocsForNibble(0x2, 700, 1000)
	delC := storageLocsForNibble(0x3, 700, 1000000)

	b1 := NewUpdateBuilder().
		Balance(addrHex(w), 100).Balance(addrHex(s1), 5).
		Balance(addrHex(f0), 7).Balance(addrHex(ff), 8).
		Storage(addrHex(w), survLoc, "01")
	for _, loc := range delB {
		b1.Storage(addrHex(w), loc, "01")
	}
	for _, loc := range delC {
		b1.Storage(addrHex(w), loc, "01")
	}
	k1, u1 := b1.Build()

	b2 := NewUpdateBuilder().Balance(addrHex(w), 200)
	for _, loc := range delB {
		b2.DeleteStorage(addrHex(w), loc)
	}
	for _, loc := range delC {
		b2.DeleteStorage(addrHex(w), loc)
	}
	k2, u2 := b2.Build()

	for _, wk := range []int{1, 4, 8} {
		requireAllEnginesParity(t, k1, u1, k2, u2, wk)
	}
}

func TestDeepIntegration_BranchParity(t *testing.T) {
	pk, upds := buildWhaleCorpus(bigAccountWhale(15_000))
	ctx := context.Background()

	seqMs := NewMockState(t)
	seq := NewHexPatriciaHashed(length.Addr, seqMs, DefaultTrieConfig())
	seqRoot := processBatch(t, seqMs, seq, pk, upds)

	for _, workers := range benchWorkerCounts() {
		parMs := NewMockState(t)
		parMs.SetConcurrentCommitment(true)
		require.NoError(t, parMs.applyPlainUpdates(pk, upds))
		pph := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr, DefaultTrieConfig())
		pph.SetNumWorkers(workers)
		pph.ResetContext(parMs)
		pUpd := WrapKeyUpdates(t, ModeParallel, KeyToHexNibbleHash, pk, upds)
		parRoot, err := pph.Process(ctx, pUpd, "", nil, WarmupConfig{})
		require.NoError(t, err)
		pUpd.Close()
		pph.Release()

		require.Equalf(t, seqRoot, parRoot, "deep parallel(workers=%d) root != sequential", workers)

		requireBranchParity(t, seqMs, parMs)
	}
}

type storKV struct {
	hk  []byte
	pk  []byte
	upd Update
}

func whaleByNibble(slots int) (addr []byte, accHash []byte, accNib int, accUpd Update, pk [][]byte, upds []Update, groups [16][]storKV) {
	rnd := rand.New(rand.NewSource(424242))
	addr = make([]byte, length.Addr)
	rnd.Read(addr)
	a := hex.EncodeToString(addr)
	ub := NewUpdateBuilder()
	ub.Balance(a, 12345)
	for range slots {
		addRandomSlot(ub, rnd, a)
	}
	pk, upds = ub.Build()
	accHash = KeyToHexNibbleHash(addr)
	accNib = int(accHash[63])
	for i, k := range pk {
		if len(k) == length.Addr {
			accUpd = upds[i]
			continue
		}
		h := KeyToHexNibbleHash(k)
		x := int(h[64])
		groups[x] = append(groups[x], storKV{hk: h, pk: k, upd: upds[i]})
	}
	return addr, accHash, accNib, accUpd, pk, upds, groups
}

func foldChildAt(w *HexPatriciaHashed, accNib int, g []storKV) (cell, error) {
	for i := range g {
		if err := w.followAndUpdate(g[i].hk, g[i].pk, &g[i].upd); err != nil {
			return cell{}, err
		}
	}
	for w.activeRows > 1 {
		if err := w.fold(); err != nil {
			return cell{}, err
		}
	}
	c := w.grid[0][accNib]
	if c.hashedExtLen > 0 {
		c.hashedExtLen--
		copy(c.hashedExtension[:], c.hashedExtension[1:])
	}
	if c.extLen > 0 {
		c.extLen--
		copy(c.extension[:], c.extension[1:])
	}
	return c, nil
}

func concurrentAccountRoot(ms *MockState, addr, accHash []byte, accNib int, accUpd Update, groups [16][]storKV, parallel bool) ([]byte, error) {
	var children [16]cell
	var present uint16
	run := func(x int) error {
		w := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
		c, err := foldChildAt(w, accNib, groups[x])
		w.Release()
		if err != nil {
			return err
		}
		children[x] = c
		return nil
	}
	if parallel {
		var eg errgroup.Group
		for x := range 16 {
			if len(groups[x]) == 0 {
				continue
			}
			present |= uint16(1) << x
			x := x
			eg.Go(func() error { return run(x) })
		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}
	} else {
		for x := range 16 {
			if len(groups[x]) == 0 {
				continue
			}
			present |= uint16(1) << x
			if err := run(x); err != nil {
				return nil, err
			}
		}
	}

	asm := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer asm.Release()
	copy(asm.currentKey[:], accHash[:64])
	asm.currentKeyLen = 64
	asm.depths[0] = 64
	asm.depths[1] = 65
	asm.activeRows = 2
	var ac cell
	ac.accountAddrLen = int16(len(addr))
	copy(ac.accountAddr[:], addr)
	ac.CodeHash = empty.CodeHash
	ac.setFromUpdate(&accUpd)
	asm.grid[0][accNib] = ac
	asm.touchMap[0] = uint16(1) << accNib
	asm.afterMap[0] = uint16(1) << accNib
	for x := range 16 {
		if present&(uint16(1)<<x) != 0 {
			asm.grid[1][x] = children[x]
		}
	}
	asm.touchMap[1] = present
	asm.afterMap[1] = present
	for asm.activeRows > 0 {
		if err := asm.fold(); err != nil {
			return nil, err
		}
	}
	return asm.RootHash()
}

func TestDeepConcurrent_WhaleParity(t *testing.T) {
	addr, accHash, accNib, accUpd, pk, upds, groups := whaleByNibble(750_000)

	ms := NewMockState(t)
	seq := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	seqRoot := processBatch(t, ms, seq, pk, upds)

	conRoot, err := concurrentAccountRoot(ms, addr, accHash, accNib, accUpd, groups, true)
	require.NoError(t, err)
	require.Equal(t, seqRoot, conRoot, "concurrent storage-fold root != sequential")
}

// A storage fold must not overwrite the account cell's hashedExtension with the storage
// extension: only a plain-key-less cell navigates by its extension.
func TestFillFromLowerCell_StorageFoldKeepsAccountNavPath(t *testing.T) {
	t.Parallel()

	accountCell := &cell{accountAddrLen: length.Addr, hashLen: 32}
	navPath := []byte{0x3, 0xc, 0x1, 0x9, 0xe}
	copy(accountCell.hashedExtension[:], navPath)
	accountCell.hashedExtLen = int16(len(navPath))

	storageBranch := &cell{hashLen: 32}

	accountCell.fillFromLowerCell(storageBranch, 65, nil, 0x7)

	require.Equalf(t, navPath, accountCell.hashedExtension[:accountCell.hashedExtLen],
		"account cell must keep its account navigation path across a storage propagate fold; "+
			"got hashedExtLen=%d", accountCell.hashedExtLen)
	require.EqualValues(t, length.Addr, accountCell.accountAddrLen, "fold must not drop the account plain key")
	require.EqualValues(t, 1, accountCell.extLen, "storage extension still travels up in extension space")
}

func TestFillFromLowerCell_AccountBranchSyncsNavPath(t *testing.T) {
	t.Parallel()

	branchCell := &cell{hashLen: 32}
	lowBranch := &cell{hashLen: 32, extLen: 2}
	lowBranch.extension[0] = 0xa
	lowBranch.extension[1] = 0xb

	branchCell.fillFromLowerCell(lowBranch, 3, []byte{0x1}, 0x2)

	want := []byte{0x1, 0x2, 0xa, 0xb}
	require.Equal(t, want, branchCell.extension[:branchCell.extLen])
	require.Equalf(t, want, branchCell.hashedExtension[:branchCell.hashedExtLen],
		"a branch cell navigates by its extension, so hashedExtension must stay in sync; got hashedExtLen=%d",
		branchCell.hashedExtLen)
}

// The sync skip is keyed on the plain key, not on depth.
func TestFillFromLowerCell_StorageBranchSyncsNavPath(t *testing.T) {
	t.Parallel()

	branchCell := &cell{hashLen: 32}
	lowBranch := &cell{hashLen: 32, extLen: 1}
	lowBranch.extension[0] = 0xd

	branchCell.fillFromLowerCell(lowBranch, 70, nil, 0x5)

	require.Equal(t, []byte{0x5, 0xd}, branchCell.hashedExtension[:branchCell.hashedExtLen],
		"a keyless cell deep in storage still navigates by its extension")
}

func TestKeyArena_PointerStability(t *testing.T) {
	var arena keyArena

	inputs := make([][]byte, 0, 4096)
	got := make([][]byte, 0, 4096)
	for i := range 4096 {
		in := bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 32)
		inputs = append(inputs, in)
		got = append(got, arena.copy(in))
	}
	big := bytes.Repeat([]byte{0xAB}, keyArenaChunk+128)
	inputs = append(inputs, big)
	got = append(got, arena.copy(big))

	for i, in := range inputs {
		require.True(t, bytes.Equal(in, got[i]),
			"key %d corrupted: returned slice does not equal its input", i)
		require.Equal(t, len(got[i]), cap(got[i]),
			"key %d not full-cap: a caller append could overwrite the next key", i)
	}

	for i := range got {
		for j := range got[i] {
			got[i][j] = byte(i)
		}
	}
	for i := range got {
		for j := range got[i] {
			require.Equal(t, byte(i), got[i][j],
				"key %d overlaps another arena slice (overwritten at byte %d)", i, j)
		}
	}
}

func TestKeyArena_ChunkSizedFromRemaining(t *testing.T) {
	const keyLen = 144

	t.Run("small subtree does not burn a full chunk", func(t *testing.T) {
		const keys = 32
		arena := keyArena{remaining: keys}
		for range keys {
			arena.copy(make([]byte, keyLen))
		}
		require.Equal(t, keys*keyLen, cap(arena.buf))
	})

	t.Run("large subtree still caps at one chunk", func(t *testing.T) {
		arena := keyArena{remaining: 10 * keyArenaChunk / keyLen}
		arena.copy(make([]byte, keyLen))
		require.Equal(t, keyArenaChunk, cap(arena.buf))
	})

	t.Run("oversized key gets its own backing", func(t *testing.T) {
		arena := keyArena{remaining: 4}
		got := arena.copy(make([]byte, 2*keyArenaChunk))
		require.Len(t, got, 2*keyArenaChunk)
		require.Equal(t, 2*keyArenaChunk, cap(arena.buf))
	})

	t.Run("copies stay stable across a chunk swap", func(t *testing.T) {
		arena := keyArena{remaining: 2}
		first := arena.copy(bytes.Repeat([]byte{0xAA}, keyLen))
		for i := range 8 {
			arena.copy(bytes.Repeat([]byte{byte(i)}, keyLen))
		}
		require.Equal(t, bytes.Repeat([]byte{0xAA}, keyLen), first)
	})

	t.Run("unhinted arena falls back to one key per chunk growth", func(t *testing.T) {
		var arena keyArena
		got := arena.copy(make([]byte, keyLen))
		require.Len(t, got, keyLen)
		require.Equal(t, keyLen, cap(arena.buf))
	})
}
