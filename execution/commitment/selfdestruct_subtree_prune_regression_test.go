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
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// These tests pin the self-destruct "empty storage base" invariant: instead of
// enumerating and deleting each of a destroyed account's storage slots (the
// current commitment-calculator behaviour), the account update carries a
// transient marker and the trie treats the storage base as empty — building the
// subtree from only the recreate's touched slots without re-mounting the old
// persisted subtree. Store cleanup of the orphaned branches is a separate,
// live-domain concern (modelled by deleteBranchesUnderAccount below).
//
// The decisive case is recreation of the destroyed address. Two regimes emerge:
//
//   - shallow storage (below deepStorageThreshold): the root self-heals from the
//     account-key delete / cell-reset alone — a freshly-recreated account rebuilds
//     its subtree from only the touched slots, and orphaned branches are never
//     mounted.
//   - deep-fold storage (above deepStorageThreshold): the parallel/streaming deep
//     -fold worker mounts the account's storage subtree from persisted branches,
//     so without the empty-base signal orphaned branches leak stale slots. Here
//     the marker is load-bearing — the deep-fold skips unfoldStorageBase.
//
// deleteBranchesUnderAccount deletes the storage-subtree commitment branches of
// an account directly from the mock store, keyed by its 64-nibble hashed prefix.
// It models the live-domain GC prefix-delete that runs BETWEEN blocks (apply
// time), NOT during the commitment Process — so it is applied after a block, not
// via a trie context capability.
func deleteBranchesUnderAccount(t *testing.T, ms *MockState, accountHex string) {
	t.Helper()
	addrBytes, err := hex.DecodeString(accountHex)
	require.NoError(t, err)
	prefix := KeyToHexNibbleHash(addrBytes)
	if ms.concurrent.Load() {
		ms.mu.Lock()
		defer ms.mu.Unlock()
	}
	for k := range ms.cm {
		nib := nibbles.CompactToHex([]byte(k))
		if len(nib) >= len(prefix) && bytes.HasPrefix(nib, prefix) {
			delete(ms.cm, k)
		}
	}
}

// sdRecreateLifecycle folds the self-destruct-then-recreate lifecycle through one
// engine mode and returns the final root:
//
//	b1:          create A with nSlots storage slots + a sibling account
//	state clean: remove A's slots from state (sm) only — the apply path's
//	             DomainDelPrefix(StorageDomain) analogue — WITHOUT feeding per-slot
//	             storage deletes to the trie
//	b2 trie:     delete A's account key only; if pruneBranches, also prefix-delete
//	             the persisted commitment branches under A
//	b3:          recreate A per `recreate`
func sdRecreateLifecycle(t *testing.T, mode runMode, workers, nSlots int, a, sib string, pruneBranches bool, recreate func(*UpdateBuilder)) []byte {
	t.Helper()
	ms := NewMockState(t)
	if mode != modeSeq {
		ms.SetConcurrentCommitment(true)
	}

	b1 := NewUpdateBuilder().Balance(a, 100).Nonce(a, 1).Balance(sib, 500)
	for s := range nSlots {
		b1.Storage(a, hex.EncodeToString(slotHashBytes(s)), "0badc0de")
	}
	k1, u1 := b1.Build()
	_, blob := processModeBatchState(t, ms, mode, workers, k1, u1, nil)

	clean := NewUpdateBuilder()
	for s := range nSlots {
		clean.DeleteStorage(a, hex.EncodeToString(slotHashBytes(s)))
	}
	ck, cu := clean.Build()
	require.NoError(t, ms.applyPlainUpdates(ck, cu))

	b2 := NewUpdateBuilder().Delete(a)
	k2, u2 := b2.Build()
	_, blob2 := processModeBatchState(t, ms, mode, workers, k2, u2, blob)
	if pruneBranches {
		deleteBranchesUnderAccount(t, ms, a)
	}

	b3 := NewUpdateBuilder()
	recreate(b3)
	k3, u3 := b3.Build()
	root, _ := processModeBatchState(t, ms, mode, workers, k3, u3, blob2)
	return root
}

// sdRecreateOracle builds the final state fresh (A recreated, its original slots
// never existed) through the same engine mode.
func sdRecreateOracle(t *testing.T, mode runMode, workers int, sib string, recreate func(*UpdateBuilder)) []byte {
	t.Helper()
	ms := NewMockState(t)
	if mode != modeSeq {
		ms.SetConcurrentCommitment(true)
	}
	ob := NewUpdateBuilder().Balance(sib, 500)
	recreate(ob)
	ok, ou := ob.Build()
	root, _ := processModeBatchState(t, ms, mode, workers, ok, ou, nil)
	return root
}

var sdEngineModes = []struct {
	name string
	mode runMode
}{
	{"seq", modeSeq},
	{"parallel", modeParallel},
}

// With the live-domain GC prefix-delete applied between blocks (the orphaned
// branches gone before the recreate reads them), dropping the per-slot storage
// enumeration reproduces the oracle root across every engine and every recreate
// shape — including the deep-fold regime where self-heal alone is insufficient.
// This is the separate-block regime; the same-block regime is covered by the
// empty-base-signal tests above.
func TestSelfDestructBranchPrune_ReproducesOracle(t *testing.T) {
	t.Parallel()
	a := addrHex(findAddressForNibble(3, 900))
	sib := addrHex(findAddressForNibble(7, 500))

	cases := []struct {
		name     string
		nSlots   int
		recreate func(*UpdateBuilder)
	}{
		{"recreate-fresh-slot", 300, func(ub *UpdateBuilder) {
			ub.Balance(a, 7777).Nonce(a, 9).Storage(a, hex.EncodeToString(slotHashBytes(4242)), "feedface")
		}},
		{"recreate-no-storage", 300, func(ub *UpdateBuilder) {
			ub.Balance(a, 7777).Nonce(a, 9)
		}},
		{"recreate-reuse-old-slot-index", 300, func(ub *UpdateBuilder) {
			ub.Balance(a, 7777).Storage(a, hex.EncodeToString(slotHashBytes(7)), "11")
		}},
		{"recreate-deepfold-slots", 1500, func(ub *UpdateBuilder) {
			ub.Balance(a, 7777).Nonce(a, 9)
			for s := 5000; s < 6600; s++ {
				ub.Storage(a, hex.EncodeToString(slotHashBytes(s)), "cc")
			}
		}},
	}

	for _, c := range cases {
		for _, md := range sdEngineModes {
			t.Run(c.name+"/"+md.name, func(t *testing.T) {
				const workers = 4
				got := sdRecreateLifecycle(t, md.mode, workers, c.nSlots, a, sib, true, c.recreate)
				want := sdRecreateOracle(t, md.mode, workers, sib, c.recreate)
				require.Truef(t, bytes.Equal(got, want),
					"self-destruct + branch prefix-delete (no per-slot enumeration) must reproduce the oracle root\n got=%x\nwant=%x", got, want)
			})
		}
	}
}

// TestSelfDestructBranchTombstone_CarriesRealPrev pins point-3: when a
// self-destruct tombstones a persisted storage branch, deleteStorageSubtreeBranches
// must hand the real branch bytes to PutBranch as prev, not an empty prev. The
// domain diff keys an unwind on that prev — an empty prev records "no previous
// value", so an unwind across the destruct would leave an empty tombstone instead
// of restoring the branch (the []byte{} arm of DomainRoTx.unwind).
func TestSelfDestructBranchTombstone_CarriesRealPrev(t *testing.T) {
	t.Parallel()
	a := addrHex(findAddressForNibble(3, 900))
	sib := addrHex(findAddressForNibble(7, 500))
	const nOld = 1500
	const workers = 4
	recreate := func(ub *UpdateBuilder) {
		ub.Balance(a, 7777).Nonce(a, 9)
		for s := 5000; s < 6600; s++ {
			ub.Storage(a, hex.EncodeToString(slotHashBytes(s)), "cc")
		}
	}

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	b1 := NewUpdateBuilder().Balance(a, 100).Nonce(a, 1).Balance(sib, 500)
	for s := range nOld {
		b1.Storage(a, hex.EncodeToString(slotHashBytes(s)), "0badc0de")
	}
	k1, u1 := b1.Build()
	_, blob := parUpdateBatch(t, ms, workers, k1, u1, nil, "")

	// Snapshot the persisted branches under A before the destruct.
	addrBytes, err := hex.DecodeString(a)
	require.NoError(t, err)
	accPrefix := KeyToHexNibbleHash(addrBytes)
	original := map[string][]byte{}
	for k, v := range ms.cm {
		if bytes.HasPrefix(nibbles.CompactToHex([]byte(k)), accPrefix) {
			original[k] = bytes.Clone(v)
		}
	}
	require.NotEmpty(t, original, "A must have persisted storage branches to tombstone")

	clean := NewUpdateBuilder()
	for s := range nOld {
		clean.DeleteStorage(a, hex.EncodeToString(slotHashBytes(s)))
	}
	ck, cu := clean.Build()
	require.NoError(t, ms.applyPlainUpdates(ck, cu))

	// Same-block recreate carrying the SD-reset marker — the path that tombstones
	// A's persisted storage branches via deleteStorageSubtreeBranches.
	ms.putLog = nil
	ms.recordPuts.Store(true)
	b2 := NewUpdateBuilder()
	recreate(b2)
	k2, u2 := b2.Build()
	_, _ = parUpdateBatch(t, ms, workers, k2, u2, blob, a)
	ms.recordPuts.Store(false)

	tombstones := 0
	for _, p := range ms.putLog {
		if BranchData(p.data).ChildCount() != 0 {
			continue // a real branch write (the recreate), not a tombstone
		}
		orig, ok := original[string(p.prefix)]
		if !ok {
			continue // a tombstone at a prefix that held no pre-destruct branch
		}
		tombstones++
		require.NotEmpty(t, p.prev, "tombstone must carry the real branch as prev, not an empty prev")
		require.Equal(t, orig, p.prev, "tombstone prev must equal the pre-destruct branch so unwind restores it")
	}
	require.Positive(t, tombstones, "at least one persisted branch under A must be tombstoned with its real prev")
}

// TestSelfDestructBranchTombstone_ReusedBranchBuffer is CarriesRealPrev under a
// buffer-reusing Branch — the production TrieContext shape, where ctx.Branch returns
// a reused per-context buffer. deleteStorageSubtreeBranches reads a branch, recurses
// into children (each overwriting that buffer), then records the branch as the
// tombstone's prev; without bytes.Clone it records a child's bytes. Uses the seq
// engine (one context, one goroutine), matching TrieContext's per-goroutine
// ownership — a shared buffer across parallel workers would just data-race instead.
func TestSelfDestructBranchTombstone_ReusedBranchBuffer(t *testing.T) {
	t.Parallel()
	a := addrHex(findAddressForNibble(3, 900))
	sib := addrHex(findAddressForNibble(7, 500))
	const nOld = 1500
	recreate := func(ub *UpdateBuilder) {
		ub.Balance(a, 7777).Nonce(a, 9)
		for s := 5000; s < 6600; s++ {
			ub.Storage(a, hex.EncodeToString(slotHashBytes(s)), "cc")
		}
	}

	ms := NewMockState(t)
	ms.reuseBranchBuf.Store(true)
	b1 := NewUpdateBuilder().Balance(a, 100).Nonce(a, 1).Balance(sib, 500)
	for s := range nOld {
		b1.Storage(a, hex.EncodeToString(slotHashBytes(s)), "0badc0de")
	}
	k1, u1 := b1.Build()
	_, blob := seqUpdateBatch(t, ms, k1, u1, nil, "")

	addrBytes, err := hex.DecodeString(a)
	require.NoError(t, err)
	accPrefix := KeyToHexNibbleHash(addrBytes)
	original := map[string][]byte{}
	for k, v := range ms.cm {
		if bytes.HasPrefix(nibbles.CompactToHex([]byte(k)), accPrefix) {
			original[k] = bytes.Clone(v)
		}
	}
	require.NotEmpty(t, original, "A must have persisted storage branches to tombstone")

	clean := NewUpdateBuilder()
	for s := range nOld {
		clean.DeleteStorage(a, hex.EncodeToString(slotHashBytes(s)))
	}
	ck, cu := clean.Build()
	require.NoError(t, ms.applyPlainUpdates(ck, cu))

	ms.putLog = nil
	ms.recordPuts.Store(true)
	b2 := NewUpdateBuilder()
	recreate(b2)
	k2, u2 := b2.Build()
	_, _ = seqUpdateBatch(t, ms, k2, u2, blob, a)
	ms.recordPuts.Store(false)

	tombstones := 0
	for _, p := range ms.putLog {
		if BranchData(p.data).ChildCount() != 0 {
			continue // a real branch write (the recreate), not a tombstone
		}
		orig, ok := original[string(p.prefix)]
		if !ok {
			continue // a tombstone at a prefix that held no pre-destruct branch
		}
		tombstones++
		require.Equal(t, orig, p.prev,
			"tombstone prev must be this branch's own bytes, not a child's left in the reused read buffer")
	}
	require.Positive(t, tombstones, "the nested subtree must produce tombstones to check")
}

// seqUpdateBatch folds one batch through a seq HexPatriciaHashed via the
// ModeUpdate path (TouchPlainKeyDirect — the commitment calculator's path, which
// carries the Update, including the transient SD-reset marker, through to the
// trie). sdReset names the account whose update gets the marker (empty = none).
func seqUpdateBatch(t *testing.T, ms *MockState, keys [][]byte, upds []Update, blob []byte, sdReset string) ([]byte, []byte) {
	t.Helper()
	require.NoError(t, ms.applyPlainUpdates(keys, upds))
	tr := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer tr.Release()
	require.NoError(t, tr.SetState(blob))
	ut := NewUpdates(ModeUpdate, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()
	var sd []byte
	if sdReset != "" {
		sd, _ = hex.DecodeString(sdReset)
	}
	for i, k := range keys {
		u := upds[i]
		if sd != nil && bytes.Equal(k, sd) {
			u.DeleteStorageSubtree = true
		}
		ut.TouchPlainKeyDirect(string(k), &u)
	}
	root := processRoot(t, tr, ut)
	out, err := tr.EncodeCurrentState(nil)
	require.NoError(t, err)
	return root, out
}

// parUpdateBatch folds one batch through a ParallelPatriciaHashed via the
// ModeParallel path (TouchPlainKeyDirect carries the Update — including the SD-reset
// marker — into the prefix trie). Mirrors processModeBatchState's modeParallel but
// feeds Direct so the marker rides, matching the production parallel calculator.
func parUpdateBatch(t *testing.T, ms *MockState, workers int, keys [][]byte, upds []Update, blob []byte, sdReset string) ([]byte, []byte) {
	t.Helper()
	require.NoError(t, ms.applyPlainUpdates(keys, upds))
	tr := newParTrie(t, ms, workers)
	defer tr.Release()
	require.NoError(t, tr.RootTrie().SetState(blob))
	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()
	var sd []byte
	if sdReset != "" {
		sd, _ = hex.DecodeString(sdReset)
	}
	for i, k := range keys {
		u := upds[i]
		if sd != nil && bytes.Equal(k, sd) {
			u.DeleteStorageSubtree = true
		}
		ut.TouchPlainKeyDirect(string(k), &u)
	}
	root := processRoot(t, tr, ut)
	out, err := tr.RootTrie().EncodeCurrentState(nil)
	require.NoError(t, err)
	return root, out
}

// The empty-base signal on the PARALLEL engine, where a deep-fold recreate
// diverges without it (unlike seq): same-block self-destruct-then-recreate with
// the SD-reset marker must reproduce the oracle, with the old branches still
// present in the store (frozen-snapshot faithful — the deep-fold worker reads a
// pre-block snapshot a mid-Process delete could not affect).
func TestSelfDestructTrieReset_SameBlockRecreate_Parallel(t *testing.T) {
	t.Parallel()
	a := addrHex(findAddressForNibble(3, 900))
	sib := addrHex(findAddressForNibble(7, 500))
	const nOld = 1500
	const workers = 4
	recreate := func(ub *UpdateBuilder) {
		ub.Balance(a, 7777).Nonce(a, 9)
		for s := 5000; s < 6600; s++ {
			ub.Storage(a, hex.EncodeToString(slotHashBytes(s)), "cc")
		}
	}

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	b1 := NewUpdateBuilder().Balance(a, 100).Nonce(a, 1).Balance(sib, 500)
	for s := range nOld {
		b1.Storage(a, hex.EncodeToString(slotHashBytes(s)), "0badc0de")
	}
	k1, u1 := b1.Build()
	_, blob := parUpdateBatch(t, ms, workers, k1, u1, nil, "")

	clean := NewUpdateBuilder()
	for s := range nOld {
		clean.DeleteStorage(a, hex.EncodeToString(slotHashBytes(s)))
	}
	ck, cu := clean.Build()
	require.NoError(t, ms.applyPlainUpdates(ck, cu))

	b2 := NewUpdateBuilder()
	recreate(b2)
	k2, u2 := b2.Build()
	got, _ := parUpdateBatch(t, ms, workers, k2, u2, blob, a)

	oms := NewMockState(t)
	oms.SetConcurrentCommitment(true)
	ob := NewUpdateBuilder().Balance(sib, 500)
	recreate(ob)
	ok, ou := ob.Build()
	want, _ := parUpdateBatch(t, oms, workers, ok, ou, nil, "")

	require.Truef(t, bytes.Equal(got, want),
		"parallel same-block SD+recreate via trie-level reset must reproduce the oracle\n got=%x\nwant=%x", got, want)
}

// The empty-base signal: a same-block self-destruct-then-recreate (account ends
// ALIVE, so no account-key delete is emitted) reproduces the oracle root when the
// account update carries the SD-reset marker. The trie treats the storage base as
// empty and rebuilds from the recreate's touched slots — no persisted-subtree read
// and no store mutation. Seq engine (ModeUpdate, the calculator's path); the old
// branches remain in the store, so this proves the signal, not a delete.
func TestSelfDestructTrieReset_SameBlockRecreate_Seq(t *testing.T) {
	t.Parallel()
	a := addrHex(findAddressForNibble(3, 900))
	sib := addrHex(findAddressForNibble(7, 500))
	const nOld = 1500
	recreate := func(ub *UpdateBuilder) {
		ub.Balance(a, 7777).Nonce(a, 9)
		for s := 5000; s < 6600; s++ {
			ub.Storage(a, hex.EncodeToString(slotHashBytes(s)), "cc")
		}
	}

	ms := NewMockState(t)
	b1 := NewUpdateBuilder().Balance(a, 100).Nonce(a, 1).Balance(sib, 500)
	for s := range nOld {
		b1.Storage(a, hex.EncodeToString(slotHashBytes(s)), "0badc0de")
	}
	k1, u1 := b1.Build()
	_, blob := seqUpdateBatch(t, ms, k1, u1, nil, "")

	// SD-marker state cleanup: old slots gone from state.
	clean := NewUpdateBuilder()
	for s := range nOld {
		clean.DeleteStorage(a, hex.EncodeToString(slotHashBytes(s)))
	}
	ck, cu := clean.Build()
	require.NoError(t, ms.applyPlainUpdates(ck, cu))

	// Same-block recreate, account update carries the SD-reset marker.
	b2 := NewUpdateBuilder()
	recreate(b2)
	k2, u2 := b2.Build()
	got, _ := seqUpdateBatch(t, ms, k2, u2, blob, a)

	oms := NewMockState(t)
	ob := NewUpdateBuilder().Balance(sib, 500)
	recreate(ob)
	ok, ou := ob.Build()
	want, _ := seqUpdateBatch(t, oms, ok, ou, nil, "")

	require.Truef(t, bytes.Equal(got, want),
		"same-block SD+recreate via trie-level reset must reproduce the oracle\n got=%x\nwant=%x", got, want)
}

// Seq self-heals a deep-fold recreate from the account-key delete alone (no
// prune, no per-slot enumeration): a freshly-recreated account rebuilds its
// subtree from only the touched slots. The parallel/streaming deep-fold instead
// needs the branch prefix-delete (TestSelfDestructBranchPrune_ReproducesOracle).
func TestSelfDestructSeqSelfHeals_DeepFoldRecreate(t *testing.T) {
	t.Parallel()
	a := addrHex(findAddressForNibble(3, 900))
	sib := addrHex(findAddressForNibble(7, 500))
	recreate := func(ub *UpdateBuilder) {
		ub.Balance(a, 7777).Nonce(a, 9)
		for s := 5000; s < 6600; s++ {
			ub.Storage(a, hex.EncodeToString(slotHashBytes(s)), "cc")
		}
	}
	noPrune := sdRecreateLifecycle(t, modeSeq, 0, 1500, a, sib, false, recreate)
	want := sdRecreateOracle(t, modeSeq, 0, sib, recreate)
	require.True(t, bytes.Equal(noPrune, want),
		"seq self-heals: account-key delete alone reproduces the oracle")
}

// An account with a single slot is one combined account+storage leaf, so the
// SD-reset marker has no storage branch to drop: the reset must also cut the
// leaf's hashed extension, which still runs into the old slot's key.
func TestSelfDestructTrieReset_SingleSlotRecreate_Seq(t *testing.T) {
	t.Parallel()
	a := addrHex(findAddressForNibble(3, 900))
	sib := addrHex(findAddressForNibble(7, 500))
	recreate := func(ub *UpdateBuilder) {
		ub.Balance(a, 0).Nonce(a, 1).Storage(a, hex.EncodeToString(slotHashBytes(5000)), "01")
	}

	ms := NewMockState(t)
	b1 := NewUpdateBuilder().Nonce(a, 0).Balance(sib, 500).Storage(a, hex.EncodeToString(slotHashBytes(0)), "01")
	k1, u1 := b1.Build()
	_, blob := seqUpdateBatch(t, ms, k1, u1, nil, "")

	clean := NewUpdateBuilder().DeleteStorage(a, hex.EncodeToString(slotHashBytes(0)))
	ck, cu := clean.Build()
	require.NoError(t, ms.applyPlainUpdates(ck, cu))

	b2 := NewUpdateBuilder()
	recreate(b2)
	k2, u2 := b2.Build()
	got, _ := seqUpdateBatch(t, ms, k2, u2, blob, a)

	oms := NewMockState(t)
	ob := NewUpdateBuilder().Balance(sib, 500)
	recreate(ob)
	ok, ou := ob.Build()
	want, _ := seqUpdateBatch(t, oms, ok, ou, nil, "")

	require.Equal(t, want, got)
}

func TestSelfDestructTrieReset_SingleSlotRecreate_Parallel(t *testing.T) {
	t.Parallel()
	a := addrHex(findAddressForNibble(3, 900))
	sib := addrHex(findAddressForNibble(7, 500))
	const workers = 4
	recreate := func(ub *UpdateBuilder) {
		ub.Balance(a, 0).Nonce(a, 1).Storage(a, hex.EncodeToString(slotHashBytes(5000)), "01")
	}

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	b1 := NewUpdateBuilder().Nonce(a, 0).Balance(sib, 500).Storage(a, hex.EncodeToString(slotHashBytes(0)), "01")
	k1, u1 := b1.Build()
	_, blob := parUpdateBatch(t, ms, workers, k1, u1, nil, "")

	clean := NewUpdateBuilder().DeleteStorage(a, hex.EncodeToString(slotHashBytes(0)))
	ck, cu := clean.Build()
	require.NoError(t, ms.applyPlainUpdates(ck, cu))

	b2 := NewUpdateBuilder()
	recreate(b2)
	k2, u2 := b2.Build()
	got, _ := parUpdateBatch(t, ms, workers, k2, u2, blob, a)

	oms := NewMockState(t)
	oms.SetConcurrentCommitment(true)
	ob := NewUpdateBuilder().Balance(sib, 500)
	recreate(ob)
	ok, ou := ob.Build()
	want, _ := parUpdateBatch(t, oms, workers, ok, ou, nil, "")

	require.Equal(t, want, got)
}

// A same-block recreate's rebuilt branches must survive the flush on the parallel
// engine: a forked child checks its branch in before the parent's tombstone at the
// same prefix, so a later block would read the tombstone (or the old cells) instead.
func TestSelfDestructTrieReset_SameBlockRecreate_NextBlock_Parallel(t *testing.T) {
	t.Parallel()
	a := addrHex(findAddressForNibble(3, 900))
	sib := addrHex(findAddressForNibble(7, 500))
	const nOld, workers = 1500, 4
	recreate := func(ub *UpdateBuilder) {
		ub.Balance(a, 7777).Nonce(a, 9)
		for s := 5000; s < 6600; s++ {
			ub.Storage(a, hex.EncodeToString(slotHashBytes(s)), "cc")
		}
	}
	next := func(ub *UpdateBuilder) {
		for s := 5000; s < 6600; s += 7 {
			ub.Storage(a, hex.EncodeToString(slotHashBytes(s)), "dd")
		}
	}

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	b1 := NewUpdateBuilder().Balance(a, 100).Nonce(a, 1).Balance(sib, 500)
	for s := range nOld {
		b1.Storage(a, hex.EncodeToString(slotHashBytes(s)), "0badc0de")
	}
	k1, u1 := b1.Build()
	_, blob := parUpdateBatch(t, ms, workers, k1, u1, nil, "")
	clean := NewUpdateBuilder()
	for s := range nOld {
		clean.DeleteStorage(a, hex.EncodeToString(slotHashBytes(s)))
	}
	ck, cu := clean.Build()
	require.NoError(t, ms.applyPlainUpdates(ck, cu))
	b2 := NewUpdateBuilder()
	recreate(b2)
	k2, u2 := b2.Build()
	_, blob = parUpdateBatch(t, ms, workers, k2, u2, blob, a)
	b3 := NewUpdateBuilder()
	next(b3)
	k3, u3 := b3.Build()
	got, _ := parUpdateBatch(t, ms, workers, k3, u3, blob, "")

	oms := NewMockState(t)
	oms.SetConcurrentCommitment(true)
	ob := NewUpdateBuilder().Balance(sib, 500)
	recreate(ob)
	ok, ou := ob.Build()
	_, oblob := parUpdateBatch(t, oms, workers, ok, ou, nil, "")
	o3 := NewUpdateBuilder()
	next(o3)
	ok3, ou3 := o3.Build()
	want, _ := parUpdateBatch(t, oms, workers, ok3, ou3, oblob, "")

	require.Equal(t, want, got)
}

// The default trie defers branch writes, so the SD reset's tombstone and the rebuilt
// branch at the same prefix are both queued in one Process; the next block must read
// the rebuilt one.
func TestSelfDestructTrieReset_SameBlockRecreate_NextBlock_Seq(t *testing.T) {
	t.Parallel()
	a := addrHex(findAddressForNibble(3, 900))
	sib := addrHex(findAddressForNibble(7, 500))
	const nOld = 1500
	recreate := func(ub *UpdateBuilder) {
		ub.Balance(a, 7777).Nonce(a, 9)
		for s := 5000; s < 6600; s++ {
			ub.Storage(a, hex.EncodeToString(slotHashBytes(s)), "cc")
		}
	}
	next := func(ub *UpdateBuilder) {
		for s := 5000; s < 6600; s += 7 {
			ub.Storage(a, hex.EncodeToString(slotHashBytes(s)), "dd")
		}
	}

	ms := NewMockState(t)
	b1 := NewUpdateBuilder().Balance(a, 100).Nonce(a, 1).Balance(sib, 500)
	for s := range nOld {
		b1.Storage(a, hex.EncodeToString(slotHashBytes(s)), "0badc0de")
	}
	k1, u1 := b1.Build()
	_, blob := seqUpdateBatch(t, ms, k1, u1, nil, "")
	clean := NewUpdateBuilder()
	for s := range nOld {
		clean.DeleteStorage(a, hex.EncodeToString(slotHashBytes(s)))
	}
	ck, cu := clean.Build()
	require.NoError(t, ms.applyPlainUpdates(ck, cu))
	b2 := NewUpdateBuilder()
	recreate(b2)
	k2, u2 := b2.Build()
	_, blob = seqUpdateBatch(t, ms, k2, u2, blob, a)
	b3 := NewUpdateBuilder()
	next(b3)
	k3, u3 := b3.Build()
	got, _ := seqUpdateBatch(t, ms, k3, u3, blob, "")

	oms := NewMockState(t)
	ob := NewUpdateBuilder().Balance(sib, 500)
	recreate(ob)
	ok, ou := ob.Build()
	_, oblob := seqUpdateBatch(t, oms, ok, ou, nil, "")
	o3 := NewUpdateBuilder()
	next(o3)
	ok3, ou3 := o3.Build()
	want, _ := seqUpdateBatch(t, oms, ok3, ou3, oblob, "")

	require.Equal(t, want, got)
}

// When every slot hash shares a leading nibble the storage root is an extension and
// the first branch sits at hashedKey+extension. The SD reset must still tombstone it
// with its real prev, and a recreate that rebuilds a branch there must keep that
// prev, so an unwind across the destruct restores the old branch.
func TestSelfDestructBranchTombstone_ExtensionRoot(t *testing.T) {
	t.Parallel()
	a := addrHex(findAddressForNibble(3, 900))
	sib := addrHex(findAddressForNibble(7, 500))
	sharedNibble := func(n int, from int) []int {
		var out []int
		for s := from; len(out) < n; s++ {
			if crypto.Keccak256(slotHashBytes(s))[0]>>4 == 0x5 {
				out = append(out, s)
			}
		}
		return out
	}
	oldSlots, newSlots := sharedNibble(40, 0), sharedNibble(40, 100_000)

	ms := NewMockState(t)
	b1 := NewUpdateBuilder().Balance(a, 100).Nonce(a, 1).Balance(sib, 500)
	for _, s := range oldSlots {
		b1.Storage(a, hex.EncodeToString(slotHashBytes(s)), "0badc0de")
	}
	k1, u1 := b1.Build()
	_, blob := seqUpdateBatch(t, ms, k1, u1, nil, "")

	addrBytes, err := hex.DecodeString(a)
	require.NoError(t, err)
	accPrefix := KeyToHexNibbleHash(addrBytes)
	original := map[string][]byte{}
	for k, v := range ms.cm {
		if hex := nibbles.CompactToHex([]byte(k)); bytes.HasPrefix(hex, accPrefix) && len(hex) > len(accPrefix) {
			original[k] = bytes.Clone(v)
		}
	}
	require.NotEmpty(t, original, "A must have a persisted storage branch below its extension")
	_, atAccount := original[string(nibbles.HexToCompact(accPrefix))]
	require.False(t, atAccount, "precondition: the storage root is an extension, no branch at the account key")

	clean := NewUpdateBuilder()
	for _, s := range oldSlots {
		clean.DeleteStorage(a, hex.EncodeToString(slotHashBytes(s)))
	}
	ck, cu := clean.Build()
	require.NoError(t, ms.applyPlainUpdates(ck, cu))

	ms.putLog = nil
	ms.recordPuts.Store(true)
	b2 := NewUpdateBuilder().Balance(a, 7777).Nonce(a, 9)
	for _, s := range newSlots {
		b2.Storage(a, hex.EncodeToString(slotHashBytes(s)), "cc")
	}
	k2, u2 := b2.Build()
	_, _ = seqUpdateBatch(t, ms, k2, u2, blob, a)
	ms.recordPuts.Store(false)

	firstPrev := map[string][]byte{}
	for _, p := range ms.putLog {
		if _, seen := firstPrev[string(p.prefix)]; !seen {
			firstPrev[string(p.prefix)] = p.prev
		}
	}
	for k, orig := range original {
		prev, written := firstPrev[k]
		require.True(t, written, "old branch %x must be tombstoned or rebuilt", []byte(k))
		require.Equal(t, orig, prev, "the first write at %x must carry the old branch as prev", []byte(k))
	}
}
