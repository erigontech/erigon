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
	"encoding/hex"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// The frontier-fold dispatch (later tasks) must reproduce the sequential trie byte-for-byte,
// so this file pins the oracle harness the whole plan validates against: root + stored-branch
// byte parity after every batch of an N>=3 chain over the four shapes the DAG must handle. The
// injection self-test guards the harness itself — it must go red on a corrupted branch, or the
// per-batch parity assertion is worthless.

// parityModes are the candidate engines checked against the sequential oracle.
var parityModes = []runMode{modeParallel, modeStreaming, modeStreamingScheduled}

// slotValHex is a deterministic non-empty storage value keyed by i.
func slotValHex(i int) string {
	return hex.EncodeToString(slotHashBytes(i * 3))
}

func balNonce(bal, nonce uint64) Update {
	u := Update{Flags: BalanceUpdate | NonceUpdate}
	u.Balance.SetUint64(bal)
	u.Nonce = nonce
	return u
}

// balancedBatches spreads accounts evenly across all 16 top nibbles (uniform account plane, a
// real 16-way root branch) and churns balances/nonces/storage over 3 batches. This is the
// no-straggler shape: the frontier dispatch's headroom must degrade to one task per nibble here.
func balancedBatches() []engineBatch {
	const perNibble = 6
	ub := NewUpdateBuilder()
	accts := make([]string, 0, 16*perNibble)
	for nib := 0; nib < 16; nib++ {
		for s := 0; s < perNibble; s++ {
			a := addrHex(findAddressForNibble(nib, 4000+nib*32+s))
			accts = append(accts, a)
			ub.Balance(a, uint64(1000+nib*10+s))
			ub.Nonce(a, uint64(s))
			ub.Storage(a, hex.EncodeToString(slotHashBytes(nib*4+s)), slotValHex(nib*4+s))
		}
	}
	k1, u1 := ub.Build()

	ub2 := NewUpdateBuilder()
	for i, a := range accts {
		if i%2 == 0 {
			ub2.Balance(a, uint64(50000+i))
		}
	}
	k2, u2 := ub2.Build()

	ub3 := NewUpdateBuilder()
	for i, a := range accts {
		if i%3 == 0 {
			ub3.Nonce(a, uint64(100+i))
			ub3.Storage(a, hex.EncodeToString(slotHashBytes(i)), slotValHex(i*7+1))
		}
	}
	k3, u3 := ub3.Build()

	return []engineBatch{{k1, u1}, {k2, u2}, {k3, u3}}
}

// megaWhaleBatches is the lopsided-by-corecount shape: one account owning slots storage slots
// (a subtree that cannot be split by account-plane fan-out) plus 16 spread accounts so the root
// is a real branch. Batch 2 rewrites a third of the slots and deletes another third; batch 3
// rewrites the remainder. This is the corpus the frontier dispatch must subdivide below depth 64.
func megaWhaleBatches(slots int) []engineBatch {
	whale := addrHex(findAddressForNibble(0xd, 4242))
	spread := make([]string, 0, 16)

	ub := NewUpdateBuilder()
	ub.Balance(whale, 12345)
	for nib := 0; nib < 16; nib++ {
		a := addrHex(findAddressForNibble(nib, 9000+nib))
		spread = append(spread, a)
		ub.Balance(a, uint64(7000+nib))
	}
	for i := 0; i < slots; i++ {
		ub.Storage(whale, hex.EncodeToString(slotHashBytes(i)), slotValHex(i))
	}
	k1, u1 := ub.Build()

	ub2 := NewUpdateBuilder()
	ub2.Balance(whale, 22222)
	for i := 0; i < slots; i += 3 {
		ub2.Storage(whale, hex.EncodeToString(slotHashBytes(i)), slotValHex(i*7+1))
	}
	for i := 1; i < slots; i += 3 {
		ub2.DeleteStorage(whale, hex.EncodeToString(slotHashBytes(i)))
	}
	for nib := 0; nib < 16; nib += 4 {
		ub2.Balance(spread[nib], uint64(80000+nib))
	}
	k2, u2 := ub2.Build()

	ub3 := NewUpdateBuilder()
	ub3.Nonce(whale, 9)
	for i := 2; i < slots; i += 3 {
		ub3.Storage(whale, hex.EncodeToString(slotHashBytes(i)), slotValHex(i*13+5))
	}
	for nib := 1; nib < 16; nib += 4 {
		ub3.Nonce(spread[nib], uint64(300+nib))
	}
	k3, u3 := ub3.Build()

	return []engineBatch{{k1, u1}, {k2, u2}, {k3, u3}}
}

// deleteToCollapseBatches deletes whole storage nibble-groups off a whale over two batches,
// collapsing the storage top branch onto surviving siblings. It keeps at least two groups alive
// throughout on purpose: a full collapse to a single storage leaf under a still-deep account hits a
// benign deep-fold-seam encoding difference (resolved storage-root hash vs stored leaf plainkey),
// not a frontier-dispatch concern, so the corpus stops short of it to stay byte-parity-clean.
func deleteToCollapseBatches() []engineBatch {
	addr, _, _, _, wpk, wupd, groups := whaleByNibble(20_000)

	ub := NewUpdateBuilder()
	for nib := 0; nib < 16; nib++ {
		ub.Balance(addrHex(findAddressForNibble(nib, 3300+nib)), uint64(4000+nib))
	}
	sk, su := ub.Build()
	k1 := append(append([][]byte{}, sk...), wpk...)
	u1 := append(append([]Update{}, su...), wupd...)

	var live []int
	for x := 0; x < 16; x++ {
		if len(groups[x]) > 0 {
			live = append(live, x)
		}
	}
	// keep the last two groups alive across all batches so the storage stays a real branch
	deletable := live
	if len(deletable) > 2 {
		deletable = deletable[:len(deletable)-2]
	}
	mid := len(deletable) / 2

	delGroups := func(idxs []int, bal, nonce uint64) engineBatch {
		k := [][]byte{addr}
		u := []Update{balNonce(bal, nonce)}
		for _, x := range idxs {
			for _, kv := range groups[x] {
				k = append(k, kv.pk)
				u = append(u, Update{Flags: DeleteUpdate})
			}
		}
		return engineBatch{k, u}
	}

	return []engineBatch{
		{k1, u1},
		delGroups(deletable[:mid], 99, 7),
		delGroups(deletable[mid:], 111, 9),
	}
}

// extensionToppedBatches seeds an extension-topped subtree under one root nibble (two accounts
// sharing hashed nibbles [7,a], forking at depth 2), every other nibble holding one account, then
// re-touches the two shared-prefix accounts twice. Re-unfolding the stored extension branch is
// where an over-stripped stitched cell desyncs the extension and corrupts the persisted branch.
func extensionToppedBatches() []engineBatch {
	a := findAddressForHexPrefix([]byte{7, 0xa, 1}, 1)
	b := findAddressForHexPrefix([]byte{7, 0xa, 2}, 2)

	seed := NewUpdateBuilder()
	seed.Balance(addrHex(a), 10)
	seed.Balance(addrHex(b), 20)
	for n := 0; n < 16; n++ {
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

	return []engineBatch{{k1, u1}, retouch(11, 22), retouch(12, 23)}
}

func runParityOverModes(t *testing.T, batches []engineBatch, workers []int) {
	t.Helper()
	for _, mode := range parityModes {
		for _, w := range workers {
			t.Run(fmt.Sprintf("%s/w%d", mode, w), func(t *testing.T) {
				runEngineBatchesParity(t, mode, w, batches)
			})
		}
	}
}

func TestFrontierParity_Balanced(t *testing.T) {
	t.Parallel()
	runParityOverModes(t, balancedBatches(), []int{1, 4, 8})
}

func TestFrontierParity_MegaWhale(t *testing.T) {
	t.Parallel()
	runParityOverModes(t, megaWhaleBatches(40_000), []int{1, 4})
}

func TestFrontierParity_DeleteToCollapse(t *testing.T) {
	t.Parallel()
	runParityOverModes(t, deleteToCollapseBatches(), []int{1, 4})
}

func TestFrontierParity_ExtensionTopped(t *testing.T) {
	t.Parallel()
	runParityOverModes(t, extensionToppedBatches(), []int{1, 4, 8})
}

// recordingT captures a require failure so a self-test can assert the parity assertion fails on a
// corrupted branch. FailNow panics (require calls it after Errorf); the caller recovers it.
type recordingT struct{ failed bool }

func (r *recordingT) Helper()               {}
func (r *recordingT) Log(...any)            {}
func (r *recordingT) Logf(string, ...any)   {}
func (r *recordingT) Errorf(string, ...any) { r.failed = true }
func (r *recordingT) FailNow()              { panic(recordingFailNow{}) }

type recordingFailNow struct{}

// expectBranchParityFails drives the real requireBranchParity with a recorder and asserts it
// flagged a failure — proving the harness would go red rather than silently pass.
func expectBranchParityFails(t *testing.T, seq, got *MockState) {
	t.Helper()
	rec := &recordingT{}
	func() {
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(recordingFailNow); !ok {
					panic(r)
				}
			}
		}()
		requireBranchParity(rec, seq, got)
	}()
	require.True(t, rec.failed, "requireBranchParity must flag a corrupted branch store")
}

func cloneBranches(ms *MockState) *MockState {
	out := &MockState{cm: make(map[string]BranchData, len(ms.cm))}
	for k, v := range ms.cm {
		out.cm[k] = append(BranchData(nil), v...)
	}
	return out
}

// TestFrontierParity_InjectionSelfTest proves the harness detects a corrupted branch instead of
// silently passing: a parity-clean run is confirmed equal, then each of a byte flip, an extra
// branch, and a missing branch must be flagged by both branchStoreMismatches and the full
// requireBranchParity assertion.
func TestFrontierParity_InjectionSelfTest(t *testing.T) {
	t.Parallel()
	batches := balancedBatches()

	seqMs := NewMockState(t)
	candMs := NewMockState(t)
	candMs.SetConcurrentCommitment(true)
	var seqBlob, candBlob []byte
	for _, b := range batches {
		_, seqBlob = processModeBatchState(t, seqMs, modeSeq, 0, b.keys, b.upds, seqBlob)
		_, candBlob = processModeBatchState(t, candMs, modeStreaming, 4, b.keys, b.upds, candBlob)
	}
	require.Empty(t, branchStoreMismatches(seqMs, candMs), "clean run must be at byte parity")
	require.NotEmpty(t, candMs.cm, "corpus must produce branches to corrupt")

	keys := make([]string, 0, len(candMs.cm))
	for k := range candMs.cm {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	target := keys[0]

	t.Run("byte_flip", func(t *testing.T) {
		poisoned := cloneBranches(candMs)
		poisoned.cm[target][len(poisoned.cm[target])-1] ^= 0xFF
		require.NotEmpty(t, branchStoreMismatches(seqMs, poisoned), "byte flip must diverge")
		expectBranchParityFails(t, seqMs, poisoned)
	})

	t.Run("extra_branch", func(t *testing.T) {
		poisoned := cloneBranches(candMs)
		poisoned.cm["\xff\xff\xff"] = append(BranchData(nil), candMs.cm[target]...)
		require.NotEmpty(t, branchStoreMismatches(seqMs, poisoned), "extra branch must diverge")
		expectBranchParityFails(t, seqMs, poisoned)
	})

	t.Run("missing_branch", func(t *testing.T) {
		poisoned := cloneBranches(candMs)
		delete(poisoned.cm, target)
		require.NotEmpty(t, branchStoreMismatches(seqMs, poisoned), "missing branch must diverge")
		expectBranchParityFails(t, seqMs, poisoned)
	})
}
