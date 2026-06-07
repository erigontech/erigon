package commitment

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
)

// findHashForNibbles brute-forces a key whose keccak hash begins with prefix.
// counterOff is the byte offset of the search counter within key.
func findHashForNibbles(t testing.TB, key []byte, counterOff int, prefix []byte, seed, salt uint64) {
	t.Helper()
	counter := seed*salt + 1
	for range 1 << 26 {
		binary.BigEndian.PutUint64(key[counterOff:counterOff+8], counter)
		h := crypto.Keccak256(key)
		ok := true
		for j, n := range prefix {
			hn := h[j>>1] >> 4
			if j&1 == 1 {
				hn = h[j>>1] & 0x0f
			}
			if hn != n {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		counter++
	}
	t.Fatalf("findHashForNibbles(%x): not found in 2^26 tries", prefix)
}

// findAddrForNibbles returns a 20-byte address whose keccak hash starts with prefix.
func findAddrForNibbles(t testing.TB, prefix []byte, seed uint64) []byte {
	addr := make([]byte, 20)
	findHashForNibbles(t, addr, 8, prefix, seed, 1_000_003)
	return addr
}

// findSlotForNibbles returns a 32-byte storage slot whose keccak hash starts with prefix.
func findSlotForNibbles(t testing.TB, prefix []byte, seed uint64) []byte {
	slot := make([]byte, 32)
	findHashForNibbles(t, slot, 16, prefix, seed, 2_000_003)
	return slot
}

var (
	storageTopN byte = 4
	storageSubN byte = 2
)

// genWideNested builds a trie wide at the top that forks at depths 1..4: 8 top
// nibbles × 4 × 2, several keys per leaf group, so with MinSplitKeys=2 there are
// nested split-points at every level — the structure random keys never produce.
func genWideNested(t testing.TB) (keys [][]byte, upds []Update) {
	t.Helper()
	seed := uint64(1)
	add := func(prefix []byte) {
		var u Update
		u.Flags = BalanceUpdate | NonceUpdate
		u.Balance.SetUint64(seed * 7)
		u.Nonce = seed
		keys = append(keys, findAddrForNibbles(t, prefix, seed))
		upds = append(upds, u)
		seed++
	}
	for top := byte(0); top < 8; top++ {
		for s := byte(0); s < 4; s++ {
			for u := byte(0); u < 2; u++ {
				add([]byte{top, s, u})
				add([]byte{top, s, u})
				add([]byte{top, s, u, 0x0})
				add([]byte{top, s, u, 0x1})
			}
		}
	}
	return keys, upds
}

// genAccountsWithNestedStorage builds nAccounts accounts, each with an account
// update plus storage slots whose keccak(slot) forks at depths 1..4 — exercising
// deep extensions and account-terminator nodes below depth 64.
func genAccountsWithNestedStorage(t testing.TB, nAccounts int) (keys [][]byte, upds []Update) {
	t.Helper()
	seed := uint64(1000)
	for a := 0; a < nAccounts; a++ {
		addr := findAddrForNibbles(t, []byte{byte(a & 0x7)}, seed)
		seed++
		var au Update
		au.Flags = BalanceUpdate | NonceUpdate | CodeUpdate
		au.Balance.SetUint64(seed)
		au.Nonce = seed
		au.CodeHash = [32]byte{byte(a), 0x11, 0x22}
		keys = append(keys, append([]byte{}, addr...))
		upds = append(upds, au)

		addSlot := func(prefix []byte) {
			slot := findSlotForNibbles(t, prefix, seed)
			seed++
			key := append(append(make([]byte, 0, length.Addr+length.Hash), addr...), slot...)
			var u Update
			u.Flags = StorageUpdate
			u.StorageLen = 4
			binary.BigEndian.PutUint32(u.Storage[:4], uint32(seed))
			keys = append(keys, key)
			upds = append(upds, u)
		}
		for top := byte(0); top < storageTopN; top++ {
			for s := byte(0); s < storageSubN; s++ {
				addSlot([]byte{top, s})
				addSlot([]byte{top, s})
				addSlot([]byte{top, s, 0x0})
				addSlot([]byte{top, s, 0x1})
			}
		}
	}
	return keys, upds
}

// genRandomAccountsStorage builds nAcc accounts with uncontrolled (real keccak)
// hash distribution, each with a few storage slots — the production-like shape.
func genRandomAccountsStorage(nAcc int) (keys [][]byte, upds []Update) {
	for a := 0; a < nAcc; a++ {
		var addr [20]byte
		binary.BigEndian.PutUint64(addr[0:8], uint64(a)*2654435761+1)
		binary.BigEndian.PutUint64(addr[12:20], uint64(a)*40503+7)
		var au Update
		au.Flags = BalanceUpdate | NonceUpdate | CodeUpdate
		au.Balance.SetUint64(uint64(a) + 1000)
		au.Nonce = uint64(a)
		au.CodeHash = [32]byte{byte(a), 0x11}
		keys = append(keys, append([]byte{}, addr[:]...))
		upds = append(upds, au)
		for s := 0; s < 3+a%6; s++ {
			var slot [32]byte
			binary.BigEndian.PutUint64(slot[0:8], uint64(a)*7919+uint64(s))
			binary.BigEndian.PutUint64(slot[24:32], uint64(s)*131+1)
			key := append(append(make([]byte, 0, length.Addr+length.Hash), addr[:]...), slot[:]...)
			var u Update
			u.Flags = StorageUpdate
			u.StorageLen = 4
			binary.BigEndian.PutUint32(u.Storage[:4], uint32(a*100+s))
			keys = append(keys, key)
			upds = append(upds, u)
		}
	}
	return keys, upds
}

// sparseBatch2 selects every modN-th key as an incremental batch: a balance/nonce
// update for accounts, a storage update for storage keys, and — when deletes is
// set — a delete for every other selected key.
func sparseBatch2(keys [][]byte, modN int, deletes bool) (k2 [][]byte, u2 []Update) {
	for i := range keys {
		if i%modN != 0 {
			continue
		}
		var u Update
		switch {
		case deletes && i%2 == 0:
			u.Flags = DeleteUpdate
		case len(keys[i]) == length.Addr:
			u.Flags = BalanceUpdate | NonceUpdate
			u.Balance.SetUint64(uint64(i)*13 + 1)
			u.Nonce = uint64(i) + 7
		default:
			u.Flags = StorageUpdate
			u.StorageLen = 4
			binary.BigEndian.PutUint32(u.Storage[:4], uint32(i)*97+3)
		}
		k2 = append(k2, keys[i])
		u2 = append(u2, u)
	}
	return k2, u2
}

// runMode selects which commitment engine runIncremental drives.
type runMode int

const (
	modeSeq runMode = iota
	modeParallel
	modeStreaming
)

// runIncremental applies two batches to one MockState (batch-1 branches become
// DB state for batch-2) and returns the final root and the MockState.
func runIncremental(t *testing.T, mode runMode, workers int, k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update) ([]byte, *MockState) {
	t.Helper()
	ctx := context.Background()
	ms := NewMockState(t)
	if mode != modeSeq {
		ms.SetConcurrentCommitment(true)
	}
	process := func(keys [][]byte, upds []Update) []byte {
		require.NoError(t, ms.applyPlainUpdates(keys, upds))
		switch mode {
		case modeParallel:
			tr := NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
			defer tr.Release()
			tr.SetNumWorkers(workers)
			tr.SetMinSplitKeys(parallelEquivMinSplitKeys)
			tr.ResetContext(ms)
			ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
			defer ut.Close()
			for i, k := range keys {
				ks := string(k)
				ut.TouchPlainKey(ks, nil, func(c *KeyUpdate, _ []byte) {
					c.plainKey = ks
					c.hashedKey = KeyToHexNibbleHash(k)
					c.update = &upds[i]
				})
			}
			r, err := tr.Process(ctx, ut, "", nil, WarmupConfig{})
			require.NoError(t, err)
			return r
		case modeStreaming:
			sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
			defer sc.Release()
			sc.SetNumWorkers(workers)
			for _, k := range keys {
				sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
			}
			r, err := sc.Process(ctx)
			require.NoError(t, err)
			return r
		default:
			tr := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
			defer tr.Release()
			ut := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, keys, upds)
			defer ut.Close()
			r, err := tr.Process(ctx, ut, "", nil, WarmupConfig{})
			require.NoError(t, err)
			return r
		}
	}
	process(k1, u1)
	return process(k2, u2), ms
}

// requireIncrementalEquiv asserts the parallel AND streaming roots match the
// sequential root after the same two batches, dumping divergent branches on
// mismatch.
func requireIncrementalEquiv(t *testing.T, k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update, workers int) {
	t.Helper()
	seqRoot, seqMs := runIncremental(t, modeSeq, 0, k1, u1, k2, u2)

	parRoot, parMs := runIncremental(t, modeParallel, workers, k1, u1, k2, u2)
	if !bytes.Equal(seqRoot, parRoot) {
		branchDiff(t, seqMs, parMs)
	}
	require.Equalf(t, seqRoot, parRoot, "parallel(workers=%d) vs sequential root mismatch", workers)

	strRoot, strMs := runIncremental(t, modeStreaming, workers, k1, u1, k2, u2)
	if !bytes.Equal(seqRoot, strRoot) {
		branchDiff(t, seqMs, strMs)
	}
	require.Equalf(t, seqRoot, strRoot, "streaming(workers=%d) vs sequential root mismatch", workers)
}

// branchDiff logs every stored branch prefix whose encoding differs between the
// sequential and parallel runs, decoding both afterMaps so dropped nibbles show.
func branchDiff(t *testing.T, seq, par *MockState) {
	t.Helper()
	seen := map[string]struct{}{}
	for k := range seq.cm {
		seen[k] = struct{}{}
	}
	for k := range par.cm {
		seen[k] = struct{}{}
	}
	n := 0
	for k := range seen {
		sb, sok := seq.cm[k]
		pb, pok := par.cm[k]
		if sok && pok && bytes.Equal(sb, pb) {
			continue
		}
		var sa, pa uint16
		if sok {
			_, sa, _, _ = BranchData(sb).decodeCells()
		}
		if pok {
			_, pa, _, _ = BranchData(pb).decodeCells()
		}
		t.Logf("DIVERGENT prefix=%x depth=%d seq[%v %016b] par[%v %016b] dropped=%016b", []byte(k), len(k), sok, sa, pok, pa, sa&^pa)
		if n++; n > 20 {
			t.Log("... (more)")
			break
		}
	}
	t.Logf("total divergent branches: %d", n)
}

// TestVerifyParallel_WideNested: single batch over a deeply-nested wide trie.
func TestVerifyParallel_WideNested(t *testing.T) {
	t.Parallel()
	keys, upds := genWideNested(t)
	require.NotEmpty(t, assertEquivalentRootWorkers(t, keys, upds, parallelEquivMinSplitKeys, 8))
}

// TestVerifyParallel_WideNestedIncremental: batch 1 commits the whole wide-nested
// trie, batch 2 touches a sparse subset so split-points have untouched DB-only
// siblings.
func TestVerifyParallel_WideNestedIncremental(t *testing.T) {
	t.Parallel()
	keys, upds := genWideNested(t)
	k2, u2 := sparseBatch2(keys, 3, false)
	requireIncrementalEquiv(t, keys, upds, k2, u2, 8)
}

// TestVerifyParallel_StorageMinimal: one storage slot per account (storageTopN/
// SubN=1) so each top nibble holds a single account+storage leaf with a shared
// extension — the shape that broke the deposit/mount extension trim.
func TestVerifyParallel_StorageMinimal(t *testing.T) {
	oldTop, oldSub := storageTopN, storageSubN
	storageTopN, storageSubN = 1, 1
	defer func() { storageTopN, storageSubN = oldTop, oldSub }()
	keys, upds := genAccountsWithNestedStorage(t, 4)
	k2, u2 := sparseBatch2(keys, 3, false)
	requireIncrementalEquiv(t, keys, upds, k2, u2, 8)
}

// TestVerifyParallel_StorageBranchEquiv asserts that after a single batch the
// parallel run's stored branches (not just the root) match the sequential ones —
// catching wrong branch metadata that a matching root would hide.
func TestVerifyParallel_StorageBranchEquiv(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	keys, upds := genAccountsWithNestedStorage(t, 4)

	seqMs := NewMockState(t)
	require.NoError(t, seqMs.applyPlainUpdates(keys, upds))
	seqTrie := NewHexPatriciaHashed(length.Addr, seqMs, DefaultTrieConfig())
	defer seqTrie.Release()
	seqUpds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, keys, upds)
	defer seqUpds.Close()
	seqRoot, err := seqTrie.Process(ctx, seqUpds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	parMs := NewMockState(t)
	parMs.SetConcurrentCommitment(true)
	require.NoError(t, parMs.applyPlainUpdates(keys, upds))
	parTrie := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr, DefaultTrieConfig())
	defer parTrie.Release()
	parTrie.SetNumWorkers(8)
	parTrie.SetMinSplitKeys(parallelEquivMinSplitKeys)
	parTrie.ResetContext(parMs)
	parUpds := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer parUpds.Close()
	for i, k := range keys {
		ks := string(k)
		parUpds.TouchPlainKey(ks, nil, func(c *KeyUpdate, _ []byte) {
			c.plainKey = ks
			c.hashedKey = KeyToHexNibbleHash(k)
			c.update = &upds[i]
		})
	}
	parRoot, err := parTrie.Process(ctx, parUpds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	require.Equal(t, seqRoot, parRoot, "single-batch root must match")
	if len(seqMs.cm) != len(parMs.cm) {
		branchDiff(t, seqMs, parMs)
	}
	require.Equal(t, len(seqMs.cm), len(parMs.cm), "branch count must match")
	for k, sb := range seqMs.cm {
		require.Equalf(t, []byte(sb), []byte(parMs.cm[k]), "branch at prefix %x must match", []byte(k))
	}
}

// TestVerifyParallel_StorageWideNestedIncremental: accounts+storage committed,
// then a sparse subset touched.
func TestVerifyParallel_StorageWideNestedIncremental(t *testing.T) {
	t.Parallel()
	keys, upds := genAccountsWithNestedStorage(t, 4)
	k2, u2 := sparseBatch2(keys, 4, false)
	requireIncrementalEquiv(t, keys, upds, k2, u2, 8)
}

// TestVerifyParallel_RandomStorageIncremental: production-like distribution,
// incremental, across worker counts.
func TestVerifyParallel_RandomStorageIncremental(t *testing.T) {
	t.Parallel()
	keys, upds := genRandomAccountsStorage(256)
	k2, u2 := sparseBatch2(keys, 3, false)
	for _, w := range []int{1, 2, 4, 8} {
		requireIncrementalEquiv(t, keys, upds, k2, u2, w)
	}
}

// TestVerifyParallel_StorageIncrementalDeletes: incremental batch mixing updates
// and deletes (storage zeroing / account removal), across worker counts.
func TestVerifyParallel_StorageIncrementalDeletes(t *testing.T) {
	t.Parallel()
	keys, upds := genRandomAccountsStorage(256)
	k2, u2 := sparseBatch2(keys, 3, true)
	for _, w := range []int{1, 2, 4, 8} {
		requireIncrementalEquiv(t, keys, upds, k2, u2, w)
	}
}
