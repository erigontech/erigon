package commitment

// Prototype (measurement-only): nested cached storage sub-cells. A big-storage
// account caches its 16 depth-65 storage-child cells; an incremental block that
// touches one storage nibble re-folds ONLY that nibble's subtree and re-aggregates
// the account from the cached cells — vs the current whole-account re-fold. Reuses
// the committed deep-fold helpers (foldChildAt / whaleByNibble / storKV).

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
)

// foldAllChildren folds every present storage nibble to its depth-65 child cell
// (the whole-account fold the flat path does every time).
func foldAllChildren(tb testing.TB, ms *MockState, accNib int, groups *[16][]storKV) ([16]cell, uint16) {
	var children [16]cell
	var present uint16
	for x := 0; x < 16; x++ {
		if len(groups[x]) == 0 {
			continue
		}
		w := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
		c, err := foldChildAt(w, accNib, groups[x])
		w.Release()
		require.NoError(tb, err)
		children[x] = c
		present |= uint16(1) << x
	}
	return children, present
}

// assembleAccountFromChildren stitches cached storage child cells into the account
// leaf and returns the root (the cheap top aggregation: ~present-count keccaks).
func assembleAccountFromChildren(tb testing.TB, ms *MockState, addr, accHash []byte, accNib int, accUpd Update, children *[16]cell, present uint16) []byte {
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
	for x := 0; x < 16; x++ {
		if present&(uint16(1)<<x) != 0 {
			asm.grid[1][x] = children[x]
		}
	}
	asm.touchMap[1] = present
	asm.afterMap[1] = present
	for asm.activeRows > 0 {
		require.NoError(tb, asm.fold())
	}
	r, err := asm.RootHash()
	require.NoError(tb, err)
	return r
}

// extraSlotsInNibble brute-forces `n` storKV whose hashed-storage nibble == x,
// modeling new storage writes landing in one already-folded sub-split.
func extraSlotsInNibble(addr []byte, x byte, n int, seed int64) []storKV {
	rnd := rand.New(rand.NewSource(seed))
	out := make([]storKV, 0, n)
	for len(out) < n {
		loc := make([]byte, length.Hash)
		rnd.Read(loc)
		key := append(append(make([]byte, 0, length.Addr+length.Hash), addr...), loc...)
		hk := KeyToHexNibbleHash(key)
		if hk[64] != x {
			continue
		}
		var u Update
		u.Flags = StorageUpdate
		u.StorageLen = 2
		u.Storage[0], u.Storage[1] = byte(len(out)+1), 0x9
		out = append(out, storKV{hk: hk, pk: key, upd: u})
	}
	return out
}

func TestNestedStorage_IncrementalParity(t *testing.T) {
	addr, accHash, accNib, accUpd, pk, upds, groups := whaleByNibble(200_000)
	ms := NewMockState(t)
	require.NoError(t, ms.applyPlainUpdates(pk, upds))

	// initial: fold all nibbles, cache cells, compute R0.
	cached, present := foldAllChildren(t, ms, accNib, &groups)
	_ = assembleAccountFromChildren(t, ms, addr, accHash, accNib, accUpd, &cached, present)

	// incremental block: add slots to ONE nibble.
	const tx = byte(0)
	extra := extraSlotsInNibble(addr, tx, 50, 9)
	groups[tx] = append(append([]storKV{}, groups[tx]...), extra...)

	// full re-fold (what the flat path does): re-fold all 16, assemble.
	fullChildren, fullPresent := foldAllChildren(t, ms, accNib, &groups)
	rFull := assembleAccountFromChildren(t, ms, addr, accHash, accNib, accUpd, &fullChildren, fullPresent)

	// incremental: re-fold only the touched nibble, reuse the other cached cells.
	wx := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	cx, err := foldChildAt(wx, accNib, groups[tx])
	wx.Release()
	require.NoError(t, err)
	inc := cached
	inc[tx] = cx
	present |= uint16(1) << tx
	rInc := assembleAccountFromChildren(t, ms, addr, accHash, accNib, accUpd, &inc, present)

	require.Equal(t, rFull, rInc, "nested incremental root != full re-fold root")
	t.Logf("incremental nested fold matches full re-fold: %x", rFull)
}

// Benchmark_NestedStorageRefold: an incremental block touching one storage nibble
// of a big account — full whole-account re-fold vs nested incremental (one nibble
// + cached-cell aggregation).
func Benchmark_NestedStorageRefold(b *testing.B) {
	addr, accHash, accNib, accUpd, pk, upds, groups := whaleByNibble(750_000)
	ms := NewMockState(b)
	require.NoError(b, ms.applyPlainUpdates(pk, upds))
	cached, present := foldAllChildren(b, ms, accNib, &groups)
	const tx = byte(0)
	groups[tx] = append(append([]storKV{}, groups[tx]...), extraSlotsInNibble(addr, tx, 50, 9)...)

	b.Run("FullRefold", func(b *testing.B) {
		for b.Loop() {
			ch, pr := foldAllChildren(b, ms, accNib, &groups)
			_ = assembleAccountFromChildren(b, ms, addr, accHash, accNib, accUpd, &ch, pr)
		}
	})
	b.Run("Incremental-1nibble", func(b *testing.B) {
		for b.Loop() {
			wx := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
			cx, err := foldChildAt(wx, accNib, groups[tx])
			wx.Release()
			require.NoError(b, err)
			inc := cached
			inc[tx] = cx
			_ = assembleAccountFromChildren(b, ms, addr, accHash, accNib, accUpd, &inc, present)
		}
	})
}
