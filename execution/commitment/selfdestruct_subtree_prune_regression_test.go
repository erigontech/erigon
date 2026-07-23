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

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// These tests pin the "delete-all-below-the-account" invariant for self-destruct:
// instead of enumerating and deleting each of a destroyed account's storage slots
// (the current commitment-calculator behaviour), the account-key delete plus a
// single prefix-delete of the persisted commitment branches under the account's
// hashed prefix must reproduce the trie root.
//
// The decisive case is recreation of the destroyed address. Two regimes emerge:
//
//   - shallow storage (below deepStorageThreshold): the root self-heals from the
//     account-key delete alone — a freshly-recreated account rebuilds its subtree
//     from only the touched slots, and orphaned branches are never mounted.
//   - deep-fold storage (above deepStorageThreshold): the parallel/streaming deep
//     -fold worker mounts the account's storage subtree from persisted branches,
//     so orphaned branches left by the account-key delete leak stale slots. Here
//     the branch prefix-delete is load-bearing.
//
// DeleteBranchPrefix models the commitment-branch prefix-delete the calculator
// performs on self-destruct: drop every persisted commitment branch at or below
// the account's 64-nibble hashed prefix (its storage subtree). It is the test
// context's stand-in for the production TrieContext.DeleteBranchPrefix.
func (ms *MockState) DeleteBranchPrefix(hashedAccountPrefix []byte) error {
	if ms.concurrent.Load() {
		ms.mu.Lock()
		defer ms.mu.Unlock()
	}
	for k := range ms.cm {
		nib := nibbles.CompactToHex([]byte(k))
		if len(nib) >= len(hashedAccountPrefix) && bytes.HasPrefix(nib, hashedAccountPrefix) {
			delete(ms.cm, k)
		}
	}
	return nil
}

// pruneBranchesUnderAccount deletes the storage-subtree commitment branches of an
// account via the context capability, keyed by its 64-nibble hashed prefix.
func pruneBranchesUnderAccount(t *testing.T, ms *MockState, accountHex string) {
	t.Helper()
	addrBytes, err := hex.DecodeString(accountHex)
	require.NoError(t, err)
	require.NoError(t, ms.DeleteBranchPrefix(KeyToHexNibbleHash(addrBytes)))
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
		pruneBranchesUnderAccount(t, ms, a)
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
	{"streaming", modeStreaming},
	{"streaming-scheduled", modeStreamingScheduled},
	{"streaming-public", modeStreamingPublic},
}

// With the branch prefix-delete, dropping the per-slot storage enumeration
// reproduces the oracle root across every engine and every recreate shape —
// including the deep-fold regime where self-heal alone is insufficient.
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

// The trie-level SD reset on the PARALLEL engine, where a deep-fold recreate
// diverges without the prune (unlike seq): same-block self-destruct-then-recreate
// with the SD-reset marker must reproduce the oracle.
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

// The trie-level SD reset: a same-block self-destruct-then-recreate (account ends
// ALIVE, so no account-key delete is emitted) reproduces the oracle root when the
// account update carries the SD-reset marker. The trie prunes the old storage
// subtree during Process — synchronously, before the recreate's slots descend —
// so it rebuilds from empty storage. This is the case the pre-Process external
// prune could not handle. Seq engine first (ModeUpdate, the calculator's path).
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
