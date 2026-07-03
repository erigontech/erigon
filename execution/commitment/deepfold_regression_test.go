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
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// whaleSurvivorCorpus builds a whale (batch 1) whose touched storage in batch 2 collapses
// to a single surviving first-nibble child. keepWholeNibble leaves one first nibble entirely
// untouched on disk (a multi-slot branch survivor); otherwise every slot but one is deleted
// (a single leaf survivor). Batch 2 still crosses the deep-fold threshold so the account folds
// concurrently.
func whaleSurvivorCorpus(keepWholeNibble bool) (pk [][]byte, upds []Update, k2 [][]byte, u2 []Update) {
	var addr []byte
	var groups [16][]storKV
	addr, _, _, _, pk, upds, groups = whaleByNibble(30_000)

	surv := -1
	for x := 0; x < 16; x++ {
		if len(groups[x]) >= 2 {
			surv = x
			break
		}
	}

	k2 = [][]byte{addr}
	u2 = []Update{{Flags: BalanceUpdate | NonceUpdate}}
	u2[0].Balance.SetUint64(99)
	u2[0].Nonce = 7
	for x := 0; x < 16; x++ {
		for i, kv := range groups[x] {
			if x == surv && (keepWholeNibble || i == 0) {
				continue // keep the survivor(s)
			}
			k2 = append(k2, kv.pk)
			u2 = append(u2, Update{Flags: DeleteUpdate})
		}
	}
	return pk, upds, k2, u2
}

// A deep-folded account whose target cell was reused from a prior storage leaf: setAccountStorageRoot
// injects the subtree root and must shed the stale storage identity, otherwise computeCellHash
// recomputes the storage root from the stale slot and ignores the injected root.
func TestDeepFold_InjectedRootClearsStaleStorage(t *testing.T) {
	t.Parallel()
	hph := NewHexPatriciaHashed(length.Addr, NewMockState(t), DefaultTrieConfig())

	staleAddr := common.HexToAddress("0x00000000000000000000000000000000deadbeef")
	staleLoc := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	addStorageToCell(&hph.root, staleAddr, staleLoc, []byte{0xAA, 0xBB, 0xCC, 0xDD})
	require.NotZero(t, hph.root.storageAddrLen, "precondition: root holds a stale storage addr")
	require.True(t, hph.root.loaded.storage(), "precondition: root is flagged storage-loaded")

	acct := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	setAccountStorageRoot(hph, KeyToHexNibbleHash(acct[:]), common.HexToHash("0x1234"))

	require.Zerof(t, hph.root.storageAddrLen,
		"injecting a storage root must clear the stale storage plain key (got len=%d)", hph.root.storageAddrLen)
	require.Falsef(t, hph.root.loaded.storage(),
		"injecting a storage root must clear the stale storage-loaded flag")
}

// The account leaf hash must be driven by the storage root injected via setAccountStorageRoot,
// regardless of whether the target cell was freshly stamped or reused from a prior storage leaf.
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
	setAccountStorageRoot(clean, accHashed, injectedSR)
	cleanHash, err := clean.computeCellHash(&clean.root, 0, nil)
	require.NoError(t, err)

	staleAddr := common.HexToAddress("0x00000000000000000000000000000000deadbeef")
	staleLoc := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	stale := NewHexPatriciaHashed(length.Addr, NewMockState(t), DefaultTrieConfig())
	addStorageToCell(&stale.root, staleAddr, staleLoc, []byte{0xAA, 0xBB, 0xCC, 0xDD})
	stale.updateCell(acct[:], accHashed, &accUpd)
	setAccountStorageRoot(stale, accHashed, injectedSR)
	staleHash, err := stale.computeCellHash(&stale.root, 0, nil)
	require.NoError(t, err)

	require.Equal(t, cleanHash, staleHash,
		"account hash must be driven by the injected storage root, not a stale storage slot")
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
	for i, b := range batches {
		roots[i] = processModeBatch(t, ms, mode, workers, b.keys, b.upds)
	}
	return roots, ms
}

// Re-touching an extension-topped top-nibble subtree over non-empty disk state: an over-strip of
// the stitched cell desyncs the extension and corrupts the persisted branch, which surfaces as a
// divergent root on the next block. Checks root and stored-branch parity across all engines.
func TestStreaming_ExtensionToppedMountSplit(t *testing.T) {
	t.Parallel()

	// a and b share hashed nibbles [7,a] and fork at depth 2, so the subtree under root nibble 7
	// is extension-topped ([a] -> branch{1,2}). Every other root nibble holds one account, so the
	// root is a real branch and nibble 7 contains only {a,b}.
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

	batches := []engineBatch{
		{k1, u1},        // seed disk
		retouch(11, 22), // mount split over the extension-topped subtree
		retouch(12, 23), // re-unfolds the subtree's stored branch
	}

	seqRoots, seqMs := runEngineBatches(t, modeSeq, 0, batches)
	for _, tc := range []struct {
		name string
		mode runMode
	}{
		{"parallel", modeParallel},
		{"streaming", modeStreaming},
		{"streaming_scheduled", modeStreamingScheduled},
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

// A whale whose touched storage collapses to a single surviving first-nibble branch. The deep
// fold must yield the extension-node storage root over that survivor without prepending the
// 64-nibble account prefix (which would overflow cell.extension and panic).
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

// The same collapse down to a single surviving storage leaf: the deep fold must compute the
// leaf hash as the storage root rather than emitting the leaf cell's zero hash.
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

// Guards the default (sequential) path against the deep-fold storage reset leaking into it: a
// persistent trie (as a real node carries across blocks) with a singleton account re-touched
// account-only in block 2 must keep its storage slot. Its root must equal a fresh trie built
// with the new account value plus the same slot. Cross-engine parity is blind to this because a
// shared updateCell bug drops the slot in every engine identically.
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
	got = common.Copy(got)
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
