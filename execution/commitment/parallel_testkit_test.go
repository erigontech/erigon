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
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

type whaleOpts struct {
	seed             int64
	smallBefore      int
	smallBeforeSlots int
	bigSlots         int
	extraWhales      []int
	smallAfter       int
	smallAfterSlots  int
	tailAccounts     int
}

// addRandomSlot appends one random storage slot to account a, drawing loc then val.
func addRandomSlot(ub *UpdateBuilder, rnd *rand.Rand, a string) {
	loc := make([]byte, length.Hash)
	rnd.Read(loc)
	val := make([]byte, 32)
	rnd.Read(val)
	ub.Storage(a, hex.EncodeToString(loc), hex.EncodeToString(val))
}

// addRandomAccount appends a random-address account (balance rnd.Uint64()+1) with slots storage slots.
func addRandomAccount(ub *UpdateBuilder, rnd *rand.Rand, slots int) {
	addr := make([]byte, length.Addr)
	rnd.Read(addr)
	a := hex.EncodeToString(addr)
	ub.Balance(a, rnd.Uint64()+1)
	for range slots {
		addRandomSlot(ub, rnd, a)
	}
}

// addNibbleAccount appends an account pinned to top nibble (balance rnd.Uint64()) with slots storage slots.
func addNibbleAccount(ub *UpdateBuilder, rnd *rand.Rand, nibble, seed, slots int) {
	a := hex.EncodeToString(findAddressForNibble(nibble, seed))
	ub.Balance(a, rnd.Uint64())
	for range slots {
		addRandomSlot(ub, rnd, a)
	}
}

func buildWhaleCorpus(opts whaleOpts) (pk [][]byte, upds []Update) {
	rnd := rand.New(rand.NewSource(opts.seed))
	ub := NewUpdateBuilder()
	for range opts.smallBefore {
		addRandomAccount(ub, rnd, opts.smallBeforeSlots)
	}
	if opts.bigSlots > 0 {
		addRandomAccount(ub, rnd, opts.bigSlots)
	}
	for _, w := range opts.extraWhales {
		addRandomAccount(ub, rnd, w)
	}
	for range opts.smallAfter {
		addRandomAccount(ub, rnd, opts.smallAfterSlots)
	}
	for range opts.tailAccounts {
		addRandomAccount(ub, rnd, 1)
	}
	return ub.Build()
}

func bigAccountWhale(bigSlots int) whaleOpts {
	return whaleOpts{seed: 771, smallBefore: 8, smallBeforeSlots: 3, bigSlots: bigSlots, smallAfter: 8, smallAfterSlots: 2}
}

// Within-account storage stress; single-level mount cannot parallelise it.
func whale1M() whaleOpts {
	return whaleOpts{seed: 919273, bigSlots: 750_000, extraWhales: []int{150_000, 5_000}, tailAccounts: 95_000}
}

type runMode int

const (
	modeSeq runMode = iota
	modeParallel
	modeStreaming
	modeStreamingScheduled
	modeStreamingPublic
)

func newSeqTrie(t *testing.T, ms *MockState) *HexPatriciaHashed {
	t.Helper()
	return NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
}

func newParTrie(t *testing.T, ms *MockState, workers int) *ParallelPatriciaHashed {
	t.Helper()
	tr := NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	tr.SetNumWorkers(workers)
	tr.ResetContext(ms)
	return tr
}

// Requires ms.SetConcurrentCommitment(true) already set.
func newStreamCommitter(t *testing.T, ms *MockState, workers int, scheduler bool) *StreamingCommitter {
	t.Helper()
	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	sc.SetNumWorkers(workers)
	if scheduler {
		require.NoError(t, sc.StartScheduler(context.Background()))
	}
	return sc
}

// newStreamingFixture builds a concurrent MockState with keys/upds applied and a StreamingCommitter
// wired to it. Pass scheduler=true to start the background scheduler before returning.
func newStreamingFixture(t *testing.T, keys [][]byte, upds []Update, workers int, scheduler ...bool) (*StreamingCommitter, *MockState) {
	t.Helper()
	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	require.NoError(t, ms.applyPlainUpdates(keys, upds))
	sc := newStreamCommitter(t, ms, workers, len(scheduler) > 0 && scheduler[0])
	return sc, ms
}

func touchAll(sc *StreamingCommitter, keys [][]byte) {
	for _, k := range keys {
		sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
	}
}

func processRoot(t *testing.T, trie Trie, ut *Updates) []byte {
	t.Helper()
	root, err := trie.Process(context.Background(), ut, "", nil, WarmupConfig{})
	require.NoError(t, err)
	return common.Copy(root)
}

func processModeBatch(t *testing.T, ms *MockState, mode runMode, workers int, keys [][]byte, upds []Update) []byte {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, ms.applyPlainUpdates(keys, upds))

	switch mode {
	case modeParallel:
		tr := newParTrie(t, ms, workers)
		defer tr.Release()
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
		return processRoot(t, tr, ut)
	case modeStreaming, modeStreamingScheduled:
		sc := newStreamCommitter(t, ms, workers, mode == modeStreamingScheduled)
		defer sc.Release()
		for _, k := range keys {
			sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
		}
		r, err := sc.Process(ctx)
		require.NoError(t, err)
		return common.Copy(r)
	case modeStreamingPublic:
		cfg := DefaultTrieConfig()
		cfg.Variant = VariantStreamingHexPatricia
		trie, ut := InitializeTrieAndUpdates(ModeDirect, t.TempDir(), cfg)
		defer ut.Close()
		defer trie.Release()
		pt := trie.(*ParallelPatriciaHashed)
		pt.SetNumWorkers(workers)
		pt.SetTrieContextFactory(mockTrieCtxFactory(ms))
		pt.ResetContext(ms)
		for _, key := range keys {
			ut.TouchPlainKey(string(key), nil, ut.TouchAccount)
		}
		return processRoot(t, trie, ut)
	default:
		tr := newSeqTrie(t, ms)
		defer tr.Release()
		ut := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, keys, upds)
		defer ut.Close()
		return processRoot(t, tr, ut)
	}
}

func engineRoot(t *testing.T, mode runMode, workers int, keys [][]byte, upds []Update) ([]byte, *MockState) {
	t.Helper()
	ms := NewMockState(t)
	if mode != modeSeq {
		ms.SetConcurrentCommitment(true)
	}
	return processModeBatch(t, ms, mode, workers, keys, upds), ms
}

// Folds two batches into one MockState so batch-1 branches become on-disk state for batch-2.
func incrementalRoot(t *testing.T, mode runMode, workers int, k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update) ([]byte, *MockState) {
	t.Helper()
	ms := NewMockState(t)
	if mode != modeSeq {
		ms.SetConcurrentCommitment(true)
	}
	processModeBatch(t, ms, mode, workers, k1, u1)
	return processModeBatch(t, ms, mode, workers, k2, u2), ms
}

func requireRootParity(t *testing.T, keys [][]byte, upds []Update, workers int) []byte {
	t.Helper()
	seqRoot, _ := engineRoot(t, modeSeq, 0, keys, upds)
	parRoot, _ := engineRoot(t, modeParallel, workers, keys, upds)
	require.Equal(t, seqRoot, parRoot,
		"sequential and parallel root hashes must match (numWorkers=%d)", workers)
	return seqRoot
}

func requireAllEnginesParity(t *testing.T, k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update, workers int) {
	t.Helper()
	seqRoot, seqMs := incrementalRoot(t, modeSeq, 0, k1, u1, k2, u2)

	parRoot, parMs := incrementalRoot(t, modeParallel, workers, k1, u1, k2, u2)
	if !bytes.Equal(seqRoot, parRoot) {
		branchDiff(t, seqMs, parMs)
	}
	require.Equalf(t, seqRoot, parRoot, "parallel(workers=%d) vs sequential root mismatch", workers)

	strRoot, strMs := incrementalRoot(t, modeStreaming, workers, k1, u1, k2, u2)
	if !bytes.Equal(seqRoot, strRoot) {
		branchDiff(t, seqMs, strMs)
	}
	require.Equalf(t, seqRoot, strRoot, "streaming(workers=%d) vs sequential root mismatch", workers)

	schRoot, schMs := incrementalRoot(t, modeStreamingScheduled, workers, k1, u1, k2, u2)
	if !bytes.Equal(seqRoot, schRoot) {
		branchDiff(t, seqMs, schMs)
	}
	require.Equalf(t, seqRoot, schRoot, "streaming-scheduled(workers=%d) vs sequential root mismatch", workers)
}

func requireBranchParity(t *testing.T, seq, got *MockState) {
	t.Helper()
	mism := 0
	seen := map[string]struct{}{}
	for k := range seq.cm {
		seen[k] = struct{}{}
	}
	for k := range got.cm {
		seen[k] = struct{}{}
	}
	for k := range seen {
		sb, sok := seq.cm[k]
		pb, pok := got.cm[k]
		if !sok || !pok || !bytes.Equal(sb, pb) {
			mism++
		}
	}
	if mism != 0 {
		branchDiff(t, seq, got)
	}
	require.Equal(t, len(seq.cm), len(got.cm), "branch count must match")
	require.Zero(t, mism, "stored branch metadata differs between streaming and sequential")
}

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

func snapshotBranches(ms *MockState) map[string][]byte {
	snap := make(map[string][]byte, len(ms.cm))
	for k, v := range ms.cm {
		snap[k] = append([]byte(nil), v...)
	}
	return snap
}

func requireBranchesUnchanged(t *testing.T, snap map[string][]byte, ms *MockState) {
	t.Helper()
	require.Equalf(t, len(snap), len(ms.cm), "a mid-block re-fold changed the stored branch count")
	for k, v := range ms.cm {
		require.Truef(t, bytes.Equal(snap[k], v), "a mid-block re-fold wrote branch %x", []byte(k))
	}
}

func nibs(vals ...byte) []byte {
	out := make([]byte, len(vals))
	copy(out, vals)
	return out
}

func nibbleAddr(targetNibble, seed int) []byte {
	return findAddressForNibble(targetNibble, seed)
}

func slotHashBytes(i int) []byte {
	var out [32]byte
	binary.BigEndian.PutUint64(out[24:], uint64(i)+1)
	return out[:]
}

func buildMixedCorpus(seed int64, nKeys int) ([][]byte, []Update) {
	rnd := rand.New(rand.NewSource(seed))
	ub := NewUpdateBuilder()
	n := 0
	for n < nKeys {
		addr := make([]byte, length.Addr)
		rnd.Read(addr)
		a := hex.EncodeToString(addr)
		ub.Balance(a, rnd.Uint64()+1)
		n++
		for s := 0; s < rnd.Intn(5) && n < nKeys; s++ {
			addRandomSlot(ub, rnd, a)
			n++
		}
	}
	return ub.Build()
}

func build100KAccountsCorpus(b testing.TB) ([][]byte, []Update) {
	b.Helper()
	rnd := rand.New(rand.NewSource(133777))
	ub := NewUpdateBuilder()
	for range 100_000 {
		addr := make([]byte, length.Addr)
		rnd.Read(addr)
		ub.Balance(hex.EncodeToString(addr), rnd.Uint64())
	}
	return ub.Build()
}

func build500KStorageHeavyCorpus(b testing.TB) ([][]byte, []Update) {
	b.Helper()
	rnd := rand.New(rand.NewSource(244888))
	ub := NewUpdateBuilder()

	addrs := make([]string, 1000)
	for i := range addrs {
		addr := make([]byte, length.Addr)
		rnd.Read(addr)
		addrs[i] = hex.EncodeToString(addr)
		ub.Balance(addrs[i], rnd.Uint64())
	}

	const slotsPerAccount = 499
	for _, addr := range addrs {
		for range slotsPerAccount {
			addRandomSlot(ub, rnd, addr)
		}
	}
	return ub.Build()
}

// witnessSlot returns the deterministic j-th storage slot value used by the witness corpora.
func witnessSlot(j int) []byte {
	return common.FromHex(fmt.Sprintf("%064x", j+1))
}

// buildWitnessCorpus builds accts accounts (balance i+1) each with slots sequential
// storage slots, processes them into (ms, hph) and returns the account plain keys.
func buildWitnessCorpus(tb testing.TB, ms *MockState, hph *HexPatriciaHashed, accts, slots int) [][]byte {
	tb.Helper()
	builder := NewUpdateBuilder()
	addrs := make([][]byte, 0, accts)
	for i := 0; i < accts; i++ {
		a, _ := generateKeyWithHashedPrefix(nil, length.Addr)
		addrs = append(addrs, a)
		builder.Balance(common.Bytes2Hex(a), uint64(i+1))
		for j := 0; j < slots; j++ {
			slot := witnessSlot(j)
			builder.Storage(common.Bytes2Hex(a), common.Bytes2Hex(slot), common.Bytes2Hex(slot))
		}
	}
	plainKeys, updates := builder.Build()
	processBatch(tb, ms, hph, plainKeys, updates)
	return addrs
}

// touchAccountsSlots touches each account and its first `slots` storage slots into u
// (slots == 0 touches accounts only).
func touchAccountsSlots(u *Updates, addrs [][]byte, slots int) {
	for _, a := range addrs {
		u.TouchPlainKey(string(a), nil, u.TouchAccount)
		for j := 0; j < slots; j++ {
			u.TouchPlainKey(string(storageKey(a, witnessSlot(j))), nil, u.TouchStorage)
		}
	}
}
