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
	return func() (*HexPatriciaHashed, func()) {
		w := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
		return w, func() { w.Release() }
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
	_, folded0, err := foldStorageRootCached(factory, cache, accNib, tg)
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

	sr, folded1, err := foldStorageRootCached(factory, cache, accNib, tg)
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

func bitsCount(b uint16) int {
	n := 0
	for b != 0 {
		n++
		b &= b - 1
	}
	return n
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

	_, folded0, err := foldStorageRootCached(factory, cache, accNib, tg)
	require.NoError(t, err)
	require.Greater(t, folded0, 1, "whale storage must span more than one nibble")

	// No nibble dirtied: a second fold reuses every cached cell.
	_, foldedNoop, err := foldStorageRootCached(factory, cache, accNib, tg)
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
	_, folded2, err := foldStorageRootCached(factory, cache, accNib, tg)
	require.NoError(t, err)
	require.Equal(t, 2, folded2, "only the two dirtied nibbles must re-fold")
	_ = addr
}
