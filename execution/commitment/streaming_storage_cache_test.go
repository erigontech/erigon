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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// touchedGroups converts the deep-fold whaleByNibble storKV groups into the
// touchedKey groups the streaming cache folds, keeping per-entry update pointers
// stable.
func touchedGroups(groups *[16][]storKV) *[16][]touchedKey {
	var out [16][]touchedKey
	for x := range 16 {
		g := groups[x]
		out[x] = make([]touchedKey, len(g))
		for i := range g {
			out[x][i] = touchedKey{hk: g[i].hk, pk: g[i].pk, upd: &g[i].upd}
		}
	}
	return &out
}

func extraSlotsTouched(addr []byte, x byte, n int, seed int64) []storKV {
	return extraSlotsInNibble(addr, x, n, seed)
}

// sequentialWhaleRoot is the real-engine oracle: HexPatriciaHashed.Process over
// the full plain-key set.
func sequentialWhaleRoot(t testing.TB, ms *MockState, pk [][]byte, upds []Update) []byte {
	seq := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer seq.Release()
	seqUpd := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk, upds)
	root, err := seq.Process(context.Background(), seqUpd, "", nil, WarmupConfig{})
	require.NoError(t, err)
	seqUpd.Close()
	return root
}

func newWhaleWorker(ms *MockState) storageWorkerFactory {
	return func() (*HexPatriciaHashed, func(), func() bool) {
		w := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
		return w, func() { w.Release() }, nil
	}
}

// TestNestedCache_CachedRefoldParity proves that an incremental cache re-fold
// (one nibble dirtied, slots added) yields the same account root as the real
// sequential engine over the final state.
func TestNestedCache_CachedRefoldParity(t *testing.T) {
	addr, accHash, accNib, accUpd, pk, upds, groups := whaleByNibble(50_000)

	ms := NewMockState(t)
	require.NoError(t, ms.applyPlainUpdates(pk, upds))

	cache := &accountStorageCache{prefix: append([]byte(nil), accHash[:64]...)}
	factory := newWhaleWorker(ms)
	tg := touchedGroups(&groups)

	// Initial fold: fresh cache, every present nibble folds once.
	_, folded0, _, err := foldStorageRootCached(factory, cache, accNib, tg, nil, 0)
	require.NoError(t, err)
	require.Equal(t, bitsCount(cache.present), folded0, "initial fold must fold every present nibble")

	// Incremental block: add slots to ONE nibble, dirty only that nibble.
	const tx = byte(0)
	extra := extraSlotsTouched(addr, tx, 64, 9)
	extraPk := make([][]byte, len(extra))
	extraUpds := make([]Update, len(extra))
	for i := range extra {
		extraPk[i] = extra[i].pk
		extraUpds[i] = extra[i].upd
	}
	require.NoError(t, ms.applyPlainUpdates(extraPk, extraUpds))
	groups[tx] = append(append([]storKV{}, groups[tx]...), extra...)
	tg = touchedGroups(&groups)
	cache.perNibble[tx].dirty = true

	sr, folded1, _, err := foldStorageRootCached(factory, cache, accNib, tg, nil, 0)
	require.NoError(t, err)
	require.Equal(t, 1, folded1, "incremental fold must re-fold only the dirtied nibble")

	// Oracle: real engine over the final state (original + extra slots).
	allPk := append(append([][]byte{}, pk...), extraPk...)
	allUpds := append(append([]Update{}, upds...), extraUpds...)
	want := sequentialWhaleRoot(t, ms, allPk, allUpds)

	// Account root assembled from the cached children must match the oracle.
	wa := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	got, err := assembleAccountRoot(wa, addr, accHash, accNib, &accUpd, &cache.children, cache.present)
	wa.Release()
	require.NoError(t, err)
	require.Equal(t, want, got, "cached incremental account root != sequential")

	// The storageRoot foldStorageRootCached returns, injected into the account
	// leaf, must reproduce the same root.
	gotSR := accountRootViaStorageRoot(t, ms, addr, accHash, accNib, &accUpd, sr)
	require.Equal(t, want, gotSR, "account root from injected storageRoot != sequential")
}

// accountRootViaStorageRoot folds a lone account leaf with the storageRoot
// injected (the setAccountStorageRoot mechanism dfsDeepLocal will use).
func accountRootViaStorageRoot(t testing.TB, ms *MockState, addr, accHash []byte, accNib int, accUpd *Update, sr cell) []byte {
	w := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer w.Release()
	copy(w.currentKey[:], accHash[:64])
	w.currentKeyLen = 64
	w.depths[0] = 64
	w.activeRows = 1
	var ac cell
	ac.accountAddrLen = int16(len(addr))
	copy(ac.accountAddr[:], addr)
	ac.CodeHash = sr.CodeHash
	ac.setFromUpdate(accUpd)
	ac.hash = sr.hash
	ac.hashLen = 32
	ac.stateHashLen = 0
	w.grid[0][accNib] = ac
	w.touchMap[0] = uint16(1) << accNib
	w.afterMap[0] = uint16(1) << accNib
	for w.activeRows > 0 {
		require.NoError(t, w.fold())
	}
	r, err := w.RootHash()
	require.NoError(t, err)
	return r
}

// synthStorageTouches builds n distinct synthetic hashed storage keys for one
// account (prefix = 64 copies of accNib) whose first-storage-nibble cycles
// through nibs, so a caller controls the nibble span and slot count without
// keccak. Task-3 routing only inspects nibbles; it never folds, so plainKey is a
// dummy stable slice supplied by the caller.
func synthStorageTouches(accNib byte, nibs []byte, n int) [][]byte {
	out := make([][]byte, n)
	for i := range n {
		hk := make([]byte, storageKeyNibbles)
		for j := range accountKeyNibbles {
			hk[j] = accNib
		}
		hk[accountKeyNibbles] = nibs[i%len(nibs)]
		v := uint64(i)
		for j := accountKeyNibbles + 1; j < storageKeyNibbles; j++ {
			hk[j] = byte(v & 0xf)
			v >>= 4
		}
		out[i] = hk
	}
	return out
}

// accPrefixString is the caches/accTouch map key for an account whose hashed
// prefix is 64 copies of accNib (the synthStorageTouches layout).
func accPrefixString(accNib byte) string {
	b := make([]byte, accountKeyNibbles)
	for i := range b {
		b[i] = accNib
	}
	return string(b)
}

func newCacheCommitter(t *testing.T) *StreamingCommitter {
	t.Helper()
	ms := NewMockState(t)
	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	t.Cleanup(sc.Release)
	return sc
}

func splitKeyCountGen(sc *StreamingCommitter, nib byte) (keyCount, gen uint64, ok bool) {
	sc.trieMu.RLock()
	defer sc.trieMu.RUnlock()
	s := sc.splits[nib]
	if s == nil {
		return 0, 0, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keyCount, s.gen, true
}

func cacheNibbleOf(sc *StreamingCommitter, prefix string, nib byte) (cacheNibble, bool) {
	sc.trieMu.RLock()
	defer sc.trieMu.RUnlock()
	c := sc.caches[prefix]
	if c == nil {
		return cacheNibble{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.perNibble[nib], true
}

// TestNestedCache_PromotionAndRouting drives storage touches of one account past
// the deep walk's promotion condition (>10k slots spanning ≥2 nibbles) and
// asserts: the account is promoted to caches; post-promotion storage touches
// route to the right per-nibble slot; and they no longer bump the owning
// top-nibble split's keyCount/gen.
func TestNestedCache_PromotionAndRouting(t *testing.T) {
	sc := newCacheCommitter(t)
	const accNib = byte(0x3)
	prefix := accPrefixString(accNib)
	pk := make([]byte, length.Addr+length.Hash)

	keys := synthStorageTouches(accNib, []byte{1, 2, 4, 8}, deepStorageThreshold+1)
	for _, hk := range keys {
		sc.TouchKey(hk, pk, nil)
	}

	sc.trieMu.RLock()
	_, promoted := sc.caches[prefix]
	_, stillTracked := sc.accTouch[prefix]
	sc.trieMu.RUnlock()
	require.True(t, promoted, "account must promote once >10k slots span ≥2 nibbles")
	require.False(t, stillTracked, "promotion must drop the pre-promotion tracker")

	kc0, gen0, ok := splitKeyCountGen(sc, accNib)
	require.True(t, ok, "pre-promotion touches must have bumped the split")

	before, _ := cacheNibbleOf(sc, prefix, 5)
	require.False(t, before.dirty, "nibble 5 is untouched before routing")
	for _, hk := range synthStorageTouches(accNib, []byte{5}, 50) {
		sc.TouchKey(hk, pk, nil)
	}
	after, ok := cacheNibbleOf(sc, prefix, 5)
	require.True(t, ok)
	require.True(t, after.dirty, "routed nibble must be marked dirty")
	untouched, _ := cacheNibbleOf(sc, prefix, 6)
	require.False(t, untouched.dirty, "an unrelated nibble must stay clean")

	kc1, gen1, _ := splitKeyCountGen(sc, accNib)
	require.Equal(t, kc0, kc1, "cached storage touch must not bump the split keyCount")
	require.Equal(t, gen0, gen1, "cached storage touch must not bump the split gen")
}

// TestNestedCache_SingleNibbleNotPromoted asserts the 10k-slots-in-one-nibble
// case mirrors the deep walk: count exceeds the threshold but the storage spans
// only one first-storage-nibble, so the account is NOT promoted and keeps
// streaming through its top-nibble split.
func TestNestedCache_SingleNibbleNotPromoted(t *testing.T) {
	sc := newCacheCommitter(t)
	const accNib = byte(0x7)
	prefix := accPrefixString(accNib)
	pk := make([]byte, length.Addr+length.Hash)

	const n = deepStorageThreshold + 1000
	for _, hk := range synthStorageTouches(accNib, []byte{0xa}, n) {
		sc.TouchKey(hk, pk, nil)
	}

	sc.trieMu.RLock()
	_, promoted := sc.caches[prefix]
	track := sc.accTouch[prefix]
	sc.trieMu.RUnlock()
	require.False(t, promoted, "single-nibble storage must not promote (deep walk would not deep-fold it)")
	require.NotNil(t, track)
	require.False(t, track.capped)

	kc, _, ok := splitKeyCountGen(sc, accNib)
	require.True(t, ok)
	require.Equal(t, uint64(n), kc, "a non-cached account's storage must stream through its split")
}

// TestNestedCache_OverCapFallback asserts that once the cache cap is reached a
// newly-qualifying account is denied a cache (no LRU eviction) and falls back to
// streaming its storage through the split.
func TestNestedCache_OverCapFallback(t *testing.T) {
	sc := newCacheCommitter(t)
	sc.nestedCap = 1
	pk := make([]byte, length.Addr+length.Hash)

	for _, hk := range synthStorageTouches(0x1, []byte{1, 2}, deepStorageThreshold+1) {
		sc.TouchKey(hk, pk, nil)
	}
	prefixB := accPrefixString(0x2)
	for _, hk := range synthStorageTouches(0x2, []byte{3, 4}, deepStorageThreshold+1) {
		sc.TouchKey(hk, pk, nil)
	}

	sc.trieMu.RLock()
	defer sc.trieMu.RUnlock()
	require.Len(t, sc.caches, 1, "cap must hold the cached-account count at the limit")
	require.Nil(t, sc.caches[prefixB], "over-cap account must not be cached")
	require.True(t, sc.accTouch[prefixB].capped, "over-cap account must be marked capped (cache-free)")
}

// TestNestedCache_Lifecycle asserts endBlock clears the per-block tracker but
// keeps promoted caches for cross-block reuse, while Reset clears both.
func TestNestedCache_Lifecycle(t *testing.T) {
	sc := newCacheCommitter(t)
	const accNib = byte(0x9)
	prefix := accPrefixString(accNib)
	pk := make([]byte, length.Addr+length.Hash)

	for _, hk := range synthStorageTouches(accNib, []byte{1, 2}, deepStorageThreshold+1) {
		sc.TouchKey(hk, pk, nil)
	}
	// A second, sub-threshold account leaves a live tracker.
	for _, hk := range synthStorageTouches(0xb, []byte{1, 2}, 100) {
		sc.TouchKey(hk, pk, nil)
	}

	sc.endBlock()
	sc.trieMu.RLock()
	_, keptCache := sc.caches[prefix]
	trackLen := len(sc.accTouch)
	sc.trieMu.RUnlock()
	require.True(t, keptCache, "endBlock must keep promoted caches for the next block")
	require.Zero(t, trackLen, "endBlock must clear the per-block tracker")

	sc.Reset()
	sc.trieMu.RLock()
	cacheLen := len(sc.caches)
	sc.trieMu.RUnlock()
	require.Zero(t, cacheLen, "Reset must clear caches")
}

// TestNestedCache_InvalidateOnRestore asserts a state restore drops the
// cross-block nested caches: clean cached subcells reflect the pre-restore
// block, so reusing them after the root is moved to a different block would
// re-fold from stale cells and compute a wrong root.
func TestNestedCache_InvalidateOnRestore(t *testing.T) {
	sc := newCacheCommitter(t)
	const accNib = byte(0x9)
	prefix := accPrefixString(accNib)
	pk := make([]byte, length.Addr+length.Hash)

	for _, hk := range synthStorageTouches(accNib, []byte{1, 2}, deepStorageThreshold+1) {
		sc.TouchKey(hk, pk, nil)
	}
	sc.endBlock() // promoted cache survives the block boundary

	sc.trieMu.RLock()
	_, cached := sc.caches[prefix]
	sc.trieMu.RUnlock()
	require.True(t, cached, "whale must be cached before the restore")

	sc.InvalidateCaches()

	sc.trieMu.RLock()
	cacheLen := len(sc.caches)
	trackLen := len(sc.accTouch)
	sc.trieMu.RUnlock()
	require.Zero(t, cacheLen, "restore must drop the cross-block caches")
	require.Zero(t, trackLen, "restore must drop the promotion trackers")
	require.Nil(t, sc.cacheFor([]byte(prefix)), "cacheFor must miss after a restore")
}

func bitsCount(b uint16) int {
	n := 0
	for b != 0 {
		n++
		b &= b - 1
	}
	return n
}

// TestNestedCache_ProcessWhaleParity drives a >10k-storage whale through the
// streaming committer's Process and asserts the cached deep fold reproduces the
// sequential root + every stored branch, and that the whale actually folded
// through the nested cache.
func TestNestedCache_ProcessWhaleParity(t *testing.T) {
	t.Parallel()
	_, _, _, _, pk, upds, _ := whaleByNibble(15_000)

	seqRoot, seqMs := sequentialRoot(t, pk, upds)

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	require.NoError(t, ms.applyPlainUpdates(pk, upds))
	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer sc.Release()
	sc.SetNumWorkers(4)
	for i := range pk {
		sc.TouchKey(KeyToHexNibbleHash(pk[i]), pk[i], nil)
	}
	root, err := sc.Process(context.Background())
	require.NoError(t, err)

	require.Equal(t, seqRoot, root, "cached-whale streaming root != sequential")
	requireBranchParity(t, seqMs, ms)
	require.NotZero(t, sc.DeepLocalFolds(), "the whale must fold through the deep walk")
	require.NotZero(t, sc.NibbleFolds(), "the whale storage must fold through the nested cache")
}

// TestNestedCache_DisabledWhaleParity is the kill-switch baseline the apples-to-
// apples bench relies on: with the nested cache off, the same whale still folds
// to the correct root through the cache-free deep walk, and the cache machinery
// (promotion, per-nibble re-fold) stays entirely dormant.
func TestNestedCache_DisabledWhaleParity(t *testing.T) {
	t.Parallel()
	_, _, _, _, pk, upds, _ := whaleByNibble(15_000)

	seqRoot, seqMs := sequentialRoot(t, pk, upds)

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	require.NoError(t, ms.applyPlainUpdates(pk, upds))
	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer sc.Release()
	sc.SetNumWorkers(4)
	sc.SetNestedCache(false)
	for i := range pk {
		sc.TouchKey(KeyToHexNibbleHash(pk[i]), pk[i], nil)
	}
	root, err := sc.Process(context.Background())
	require.NoError(t, err)

	require.Equal(t, seqRoot, root, "cache-off streaming root != sequential")
	requireBranchParity(t, seqMs, ms)
	require.NotZero(t, sc.DeepLocalFolds(), "the whale must still fold through the deep walk")
	require.Zero(t, sc.NibbleFolds(), "no per-nibble cache re-fold may run with the cache off")
	sc.trieMu.RLock()
	cacheLen := len(sc.caches)
	sc.trieMu.RUnlock()
	require.Zero(t, cacheLen, "no account may be promoted with the cache off")
}

// TestNestedCache_IncrementalBlockParity mass-writes a whale (batch 1), then a
// second block touches a sparse subset of its storage in two first-storage
// nibbles (batch 2) — the account itself untouched. Reusing the same committer
// (the cache survives endBlock) the final root + every branch must match the
// sequential two-batch run, and batch 2 must re-fold exactly the two changed
// nibbles.
func TestNestedCache_IncrementalBlockParity(t *testing.T) {
	ctx := context.Background()
	addr, _, _, _, pk1, upds1, _ := whaleByNibble(15_000)

	var pk2 [][]byte
	var upds2 []Update
	for _, nib := range []byte{0x3, 0xc} {
		for _, e := range extraSlotsInNibble(addr, nib, 40, int64(nib)+1) {
			pk2 = append(pk2, e.pk)
			upds2 = append(upds2, e.upd)
		}
	}

	seqMs := NewMockState(t)
	require.NoError(t, seqMs.applyPlainUpdates(pk1, upds1))
	seq := NewHexPatriciaHashed(length.Addr, seqMs, DefaultTrieConfig())
	defer seq.Release()
	u1 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk1, upds1)
	_, err := seq.Process(ctx, u1, "", nil, WarmupConfig{})
	require.NoError(t, err)
	u1.Close()
	require.NoError(t, seqMs.applyPlainUpdates(pk2, upds2))
	u2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk2, upds2)
	seqRoot, err := seq.Process(ctx, u2, "", nil, WarmupConfig{})
	require.NoError(t, err)
	u2.Close()

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer sc.Release()
	sc.SetNumWorkers(4)

	require.NoError(t, ms.applyPlainUpdates(pk1, upds1))
	for i := range pk1 {
		sc.TouchKey(KeyToHexNibbleHash(pk1[i]), pk1[i], nil)
	}
	_, err = sc.Process(ctx)
	require.NoError(t, err)

	before := sc.NibbleFolds()
	require.NoError(t, ms.applyPlainUpdates(pk2, upds2))
	for i := range pk2 {
		sc.TouchKey(KeyToHexNibbleHash(pk2[i]), pk2[i], nil)
	}
	root2, err := sc.Process(ctx)
	require.NoError(t, err)

	require.Equal(t, seqRoot, root2, "incremental cached-whale root != sequential")
	requireBranchParity(t, seqMs, ms)
	require.Equal(t, uint64(2), sc.NibbleFolds()-before, "batch 2 must re-fold exactly the two touched nibbles")
}

// TestNestedCache_SingleNibbleCompressedParity regresses the compressed deep-walk
// path: a cached whale whose block-2 touch is a single slot in a single
// first-storage-nibble compresses the storage prefix trie past depth 64, so
// dfsDeepLocal enters at a storage leaf with path>64. The cache must supply the
// storageRoot without the leaf-follow leaking a stray storage cell into the
// account-level worker, so the incremental root matches the sequential oracle.
func TestNestedCache_SingleNibbleCompressedParity(t *testing.T) {
	ctx := context.Background()
	addr, _, _, _, pk1, upds1, _ := whaleByNibble(15_000)

	var pk2 [][]byte
	var upds2 []Update
	for _, e := range extraSlotsInNibble(addr, 0x7, 1, 99) {
		pk2 = append(pk2, e.pk)
		upds2 = append(upds2, e.upd)
	}

	seqMs := NewMockState(t)
	require.NoError(t, seqMs.applyPlainUpdates(pk1, upds1))
	seq := NewHexPatriciaHashed(length.Addr, seqMs, DefaultTrieConfig())
	defer seq.Release()
	u1 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk1, upds1)
	_, err := seq.Process(ctx, u1, "", nil, WarmupConfig{})
	require.NoError(t, err)
	u1.Close()
	require.NoError(t, seqMs.applyPlainUpdates(pk2, upds2))
	u2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk2, upds2)
	seqRoot, err := seq.Process(ctx, u2, "", nil, WarmupConfig{})
	require.NoError(t, err)
	u2.Close()

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer sc.Release()
	sc.SetNumWorkers(4)

	require.NoError(t, ms.applyPlainUpdates(pk1, upds1))
	for i := range pk1 {
		sc.TouchKey(KeyToHexNibbleHash(pk1[i]), pk1[i], nil)
	}
	_, err = sc.Process(ctx)
	require.NoError(t, err)

	require.NoError(t, ms.applyPlainUpdates(pk2, upds2))
	for i := range pk2 {
		sc.TouchKey(KeyToHexNibbleHash(pk2[i]), pk2[i], nil)
	}
	root2, err := sc.Process(ctx)
	require.NoError(t, err)

	require.Equal(t, seqRoot, root2, "single-nibble compressed cached-whale root != sequential")
}

// TestNestedCache_WhaleNibbleCollapse builds a >10k-storage whale (batch 1) so its
// storage caches, then batch 2 deletes every slot in one first-storage-nibble,
// collapsing that nibble's subtree. The cache must drop the emptied nibble from
// the storage branch and re-fold the touched nibble from its accumulated slot set
// (deletes applied) rather than reusing the stale batch-1 cell, so the final root
// matches the sequential two-block oracle.
//
// Only ROOT parity is asserted, not full branch-store parity: a cross-block delete
// that collapses a big account's storage subtree cannot be reproduced
// byte-for-byte in the deep-fold path — it rebuilds each nibble from its keys and
// never reads the on-disk subtree, so it cannot emit the deletion tombstones a
// sequential fold writes for the orphaned (now-unreachable) deep sub-branches. The
// cache-free deep baselines (ModeParallel, cache-off streaming) fare worse: they
// diverge on the ROOT itself for this corpus, so the cache's rebuild-from-keys is
// the only path that gets the collapsed root right.
func TestNestedCache_WhaleNibbleCollapse(t *testing.T) {
	ctx := context.Background()
	_, _, _, _, pk1, upds1, groups := whaleByNibble(15_000)

	// Pick the most-populated first-storage-nibble and delete every slot in it.
	delNib := 0
	for x := 1; x < 16; x++ {
		if len(groups[x]) > len(groups[delNib]) {
			delNib = x
		}
	}
	require.Greater(t, len(groups[delNib]), 0, "chosen nibble must carry slots")
	var pk2 [][]byte
	var upds2 []Update
	for _, e := range groups[delNib] {
		var u Update
		u.Flags = DeleteUpdate
		pk2 = append(pk2, e.pk)
		upds2 = append(upds2, u)
	}

	seqMs := NewMockState(t)
	require.NoError(t, seqMs.applyPlainUpdates(pk1, upds1))
	seq := NewHexPatriciaHashed(length.Addr, seqMs, DefaultTrieConfig())
	defer seq.Release()
	u1 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk1, upds1)
	_, err := seq.Process(ctx, u1, "", nil, WarmupConfig{})
	require.NoError(t, err)
	u1.Close()
	require.NoError(t, seqMs.applyPlainUpdates(pk2, upds2))
	u2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk2, upds2)
	seqRoot, err := seq.Process(ctx, u2, "", nil, WarmupConfig{})
	require.NoError(t, err)
	u2.Close()

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer sc.Release()
	sc.SetNumWorkers(4)

	require.NoError(t, ms.applyPlainUpdates(pk1, upds1))
	for i := range pk1 {
		sc.TouchKey(KeyToHexNibbleHash(pk1[i]), pk1[i], nil)
	}
	_, err = sc.Process(ctx)
	require.NoError(t, err)

	before := sc.DeepLocalFolds()
	require.NoError(t, ms.applyPlainUpdates(pk2, upds2))
	for i := range pk2 {
		sc.TouchKey(KeyToHexNibbleHash(pk2[i]), pk2[i], nil)
	}
	root2, err := sc.Process(ctx)
	require.NoError(t, err)

	require.Equal(t, seqRoot, root2, "collapsed cached-whale nibble root != sequential")
	require.Greater(t, sc.DeepLocalFolds(), before, "the collapse must route through the cache deep walk")
}

// TestNestedCache_OnlyDirtyNibblesRefold asserts the per-nibble fold count: after
// the initial fold, touching a single nibble re-folds exactly that nibble and
// reuses the cached cells for the rest.
func TestNestedCache_OnlyDirtyNibblesRefold(t *testing.T) {
	addr, accHash, accNib, _, pk, upds, groups := whaleByNibble(50_000)
	ms := NewMockState(t)
	require.NoError(t, ms.applyPlainUpdates(pk, upds))

	cache := &accountStorageCache{prefix: append([]byte(nil), accHash[:64]...)}
	factory := newWhaleWorker(ms)
	tg := touchedGroups(&groups)

	_, folded0, _, err := foldStorageRootCached(factory, cache, accNib, tg, nil, 0)
	require.NoError(t, err)
	require.Greater(t, folded0, 1, "whale storage must span more than one nibble")

	// No nibble dirtied: a second fold reuses every cached cell.
	_, foldedNoop, _, err := foldStorageRootCached(factory, cache, accNib, tg, nil, 0)
	require.NoError(t, err)
	require.Equal(t, 0, foldedNoop, "clean re-fold must reuse all cached cells")

	// Dirty two nibbles → exactly two re-fold.
	dirtied := 0
	for x := 0; x < 16 && dirtied < 2; x++ {
		if cache.present&(uint16(1)<<x) != 0 {
			cache.perNibble[x].dirty = true
			dirtied++
		}
	}
	_, folded2, _, err := foldStorageRootCached(factory, cache, accNib, tg, nil, 0)
	require.NoError(t, err)
	require.Equal(t, 2, folded2, "only the two dirtied nibbles must re-fold")
	_ = addr
}

// TestNestedCache_SchedulerWhaleParity runs the background scheduler while many
// goroutines concurrently touch a whale-bearing corpus, then asserts the drained
// Process root + every stored branch match sequential. The whale promotes a cache
// mid-block, so the background fold of its split must route through the cache-aware
// deep walk rather than re-streaming the storage. Run under -race.
func TestNestedCache_SchedulerWhaleParity(t *testing.T) {
	t.Parallel()
	pk, upds := buildBigAccountCorpus(15_000)
	seqRoot, seqMs := sequentialRoot(t, pk, upds)

	for _, w := range []int{4, 8} {
		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		require.NoError(t, ms.applyPlainUpdates(pk, upds))

		sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		sc.SetNumWorkers(w)
		sc.SetEagerFold(1) // fold below the production floor so the whale's split eagerly background-folds
		require.NoError(t, sc.StartScheduler(context.Background()))

		const goroutines = 4
		var wg sync.WaitGroup
		for g := range goroutines {
			wg.Add(1)
			go func(start int) {
				defer wg.Done()
				for i := start; i < len(pk); i += goroutines {
					sc.TouchKey(KeyToHexNibbleHash(pk[i]), pk[i], nil)
				}
			}(g)
		}
		wg.Wait()

		root, err := sc.Process(context.Background())
		require.NoError(t, err)
		require.Equalf(t, seqRoot, root, "scheduler whale(workers=%d) root != sequential", w)
		requireBranchParity(t, seqMs, ms)
		sc.Release()
	}
}

// TestNestedCache_BgDeepFoldParity establishes a whale cache in block 1, then in
// block 2 (scheduler running) touches the whale's account leaf — forcing its split
// to background-fold through the cache-aware deep walk while the cache exists —
// plus a sparse storage subset. The final root + branches must match the
// sequential two-block run, and the background deep fold must actually fire,
// proving the background and Process paths share the cache as the single
// storageRoot source.
func TestNestedCache_BgDeepFoldParity(t *testing.T) {
	ctx := context.Background()
	addr, _, _, _, pk1, upds1, _ := whaleByNibble(15_000)
	a := hex.EncodeToString(addr)

	ub := NewUpdateBuilder()
	ub.Balance(a, 99_999)
	apk, aupd := ub.Build()
	pk2 := [][]byte{apk[0]}
	upds2 := []Update{aupd[0]}
	for _, nib := range []byte{0x3, 0xc} {
		for _, e := range extraSlotsInNibble(addr, nib, 40, int64(nib)+1) {
			pk2 = append(pk2, e.pk)
			upds2 = append(upds2, e.upd)
		}
	}

	seqMs := NewMockState(t)
	require.NoError(t, seqMs.applyPlainUpdates(pk1, upds1))
	seq := NewHexPatriciaHashed(length.Addr, seqMs, DefaultTrieConfig())
	defer seq.Release()
	u1 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk1, upds1)
	_, err := seq.Process(ctx, u1, "", nil, WarmupConfig{})
	require.NoError(t, err)
	u1.Close()
	require.NoError(t, seqMs.applyPlainUpdates(pk2, upds2))
	u2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk2, upds2)
	seqRoot, err := seq.Process(ctx, u2, "", nil, WarmupConfig{})
	require.NoError(t, err)
	u2.Close()

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer sc.Release()
	sc.SetNumWorkers(4)

	require.NoError(t, ms.applyPlainUpdates(pk1, upds1))
	for i := range pk1 {
		sc.TouchKey(KeyToHexNibbleHash(pk1[i]), pk1[i], nil)
	}
	_, err = sc.Process(ctx)
	require.NoError(t, err)

	require.NoError(t, ms.applyPlainUpdates(pk2, upds2))
	sc.SetEagerFold(1)
	require.NoError(t, sc.StartScheduler(ctx))
	for i := range pk2 {
		sc.TouchKey(KeyToHexNibbleHash(pk2[i]), pk2[i], nil)
	}
	waitSchedulerIdle(t, sc)
	root2, err := sc.Process(ctx)
	require.NoError(t, err)

	require.Equal(t, seqRoot, root2, "background cached-whale root != sequential")
	requireBranchParity(t, seqMs, ms)
	require.Positive(t, sc.BgDeepFolds(), "the whale split must background-fold through the cache")
}
