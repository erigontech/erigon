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
// pruneBranchesUnderAccount models the prefix-delete the calculator must perform
// on self-destruct: drop every persisted commitment branch at or below the
// account's 64-nibble hashed prefix.
func pruneBranchesUnderAccount(ms *MockState, accountHex string) {
	addrBytes, err := hex.DecodeString(accountHex)
	if err != nil {
		panic(err)
	}
	accountHash := KeyToHexNibbleHash(addrBytes)
	for k := range ms.cm {
		nib := nibbles.CompactToHex([]byte(k))
		if len(nib) >= 64 && bytes.HasPrefix(nib, accountHash) {
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
		pruneBranchesUnderAccount(ms, a)
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

// Guard: the branch prefix-delete is load-bearing on the deep-fold path. Deleting
// only the account key (relying on self-heal) diverges from the oracle under the
// parallel/streaming deep-fold, because the deep-fold mount rebuilds the subtree
// from orphaned persisted branches. This pins WHY the prune cannot be dropped.
func TestSelfDestructNoPrune_DeepFoldDiverges(t *testing.T) {
	t.Parallel()
	a := addrHex(findAddressForNibble(3, 900))
	sib := addrHex(findAddressForNibble(7, 500))
	recreate := func(ub *UpdateBuilder) {
		ub.Balance(a, 7777).Nonce(a, 9)
		for s := 5000; s < 6600; s++ {
			ub.Storage(a, hex.EncodeToString(slotHashBytes(s)), "cc")
		}
	}

	for _, md := range sdEngineModes {
		t.Run(md.name, func(t *testing.T) {
			const workers = 4
			noPrune := sdRecreateLifecycle(t, md.mode, workers, 1500, a, sib, false, recreate)
			want := sdRecreateOracle(t, md.mode, workers, sib, recreate)
			if md.mode == modeSeq {
				require.True(t, bytes.Equal(noPrune, want),
					"seq self-heals: account-key delete alone reproduces the oracle")
			} else {
				require.Falsef(t, bytes.Equal(noPrune, want),
					"%s deep-fold must diverge without the branch prefix-delete — if this now matches, self-heal covers deep-fold and the guard can be relaxed", md.name)
			}
		})
	}
}
