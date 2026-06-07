package commitment

import (
	"context"
	"encoding/hex"
	"math/bits"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
)

// foldSubWorkerTo folds w down until only mountRow+1 rows remain and returns the
// cell at grid[mountRow][col] — the generalised foldMounted (which is hardwired
// to mountRow==0). For a storage sub-worker mountRow==1 returns the subtree cell
// at depth 65 under storage nibble col.
func foldSubWorkerTo(t *testing.T, w *HexPatriciaHashed, mountRow, col int) cell {
	for w.activeRows > mountRow+1 {
		require.NoError(t, w.fold())
	}
	require.GreaterOrEqual(t, w.activeRows, mountRow+1, "sub-worker collapsed below mount row")
	return w.grid[mountRow][col]
}

// buildDenseWhale: one account + `slots` random storage slots (dense — with many
// slots every depth-64 nibble branches, sidestepping the single-child case).
func buildDenseWhale(slots int) (addr []byte, accUpd Update, pk [][]byte, upds []Update) {
	rnd := rand.New(rand.NewSource(424242))
	addr = make([]byte, length.Addr)
	rnd.Read(addr)
	a := hex.EncodeToString(addr)
	ub := NewUpdateBuilder()
	ub.Balance(a, 12345)
	for i := 0; i < slots; i++ {
		loc := make([]byte, length.Hash)
		rnd.Read(loc)
		val := make([]byte, 32)
		rnd.Read(val)
		ub.Storage(a, hex.EncodeToString(loc), hex.EncodeToString(val))
	}
	pk, upds = ub.Build()
	for i, k := range pk {
		if len(k) == length.Addr {
			accUpd = upds[i]
		}
	}
	return addr, accUpd, pk, upds
}

func TestDeepConcurrent_DenseStorageParity(t *testing.T) {
	addr, accUpd, pk, upds := buildDenseWhale(2048)

	// ---- sequential oracle ----
	seqMs := NewMockState(t)
	require.NoError(t, seqMs.applyPlainUpdates(pk, upds))
	seq := NewHexPatriciaHashed(length.Addr, seqMs, DefaultTrieConfig())
	seqUpd := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk, upds)
	seqRoot, err := seq.Process(context.Background(), seqUpd, "", nil, WarmupConfig{})
	require.NoError(t, err)
	seqUpd.Close()

	// ---- partition storage plain keys by first storage nibble (hashed) ----
	accHash := KeyToHexNibbleHash(addr) // 64 nibbles
	accNib := int(accHash[63])
	var groups [16][][]byte
	for _, k := range pk {
		if len(k) == length.Addr {
			continue
		}
		h := KeyToHexNibbleHash(k) // 128 nibbles: accHash(64)+storHash(64)
		x := int(h[64])
		groups[x] = append(groups[x], k)
	}

	ms := NewMockState(t)
	require.NoError(t, ms.applyPlainUpdates(pk, upds))

	// ---- per-nibble sub-workers (serial here; concurrency is orthogonal once correct) ----
	var children [16]cell
	var present uint16
	for x := 0; x < 16; x++ {
		if len(groups[x]) == 0 {
			continue
		}
		require.Greater(t, len(groups[x]), 1, "nibble %x has a single child (edge case not handled yet)", x)
		w := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
		for _, k := range groups[x] {
			hk := KeyToHexNibbleHash(k)
			require.NoError(t, w.followAndUpdate(hk, k, nil))
		}
		c := foldSubWorkerTo(t, w, 0, accNib)
		// the cell sits at depth 64 with extension leading with x; strip x to
		// position it at the depth-65 storage branch slot.
		if c.hashedExtLen > 0 {
			c.hashedExtLen--
			copy(c.hashedExtension[:], c.hashedExtension[1:])
		}
		if c.extLen > 0 {
			c.extLen--
			copy(c.extension[:], c.extension[1:])
		}
		children[x] = c
		present |= uint16(1) << x
	}

	// ---- assemble: account leaf (row 0, depth 64) + storage branch (row 1, depth 65) ----
	asm := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
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
		if present&(uint16(1)<<x) == 0 {
			continue
		}
		asm.grid[1][x] = children[x]
	}
	asm.touchMap[1] = present
	asm.afterMap[1] = present

	for asm.activeRows > 0 {
		require.NoError(t, asm.fold())
	}
	conRoot, err := asm.RootHash()
	require.NoError(t, err)

	t.Logf("present nibbles=%d (%016b)", bits.OnesCount16(present), present)
	t.Logf("seq=%x", seqRoot)
	t.Logf("con=%x", conRoot)
	require.Equal(t, seqRoot, conRoot, "concurrent storage-fold root != sequential root")
}
