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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// whaleOpts describes a deterministic account/storage corpus: smallBefore small
// accounts, an optional bigSlots whale, extraWhales, smallAfter small accounts,
// then tailAccounts single-slot accounts. fixedBalance != 0 uses a constant
// balance (no RNG draw); otherwise each balance is rnd.Uint64()+1.
type whaleOpts struct {
	seed             int64
	smallBefore      int
	smallBeforeSlots int
	bigSlots         int
	extraWhales      []int
	smallAfter       int
	smallAfterSlots  int
	tailAccounts     int
	fixedBalance     uint64
}

// buildWhaleCorpus generates the account/storage corpus described by opts. The
// RNG draw order (addr, balance, then per-slot loc+val) is identical for every
// account so a given (seed, layout) reproduces a fixed set of keys and updates.
func buildWhaleCorpus(opts whaleOpts) (pk [][]byte, upds []Update) {
	rnd := rand.New(rand.NewSource(opts.seed))
	ub := NewUpdateBuilder()
	addAcc := func(slots int) {
		addr := make([]byte, length.Addr)
		rnd.Read(addr)
		a := hex.EncodeToString(addr)
		if opts.fixedBalance != 0 {
			ub.Balance(a, opts.fixedBalance)
		} else {
			ub.Balance(a, rnd.Uint64()+1)
		}
		for range slots {
			loc := make([]byte, length.Hash)
			rnd.Read(loc)
			val := make([]byte, 32)
			rnd.Read(val)
			ub.Storage(a, hex.EncodeToString(loc), hex.EncodeToString(val))
		}
	}
	for range opts.smallBefore {
		addAcc(opts.smallBeforeSlots)
	}
	if opts.bigSlots > 0 {
		addAcc(opts.bigSlots)
	}
	for _, w := range opts.extraWhales {
		addAcc(w)
	}
	for range opts.smallAfter {
		addAcc(opts.smallAfterSlots)
	}
	for range opts.tailAccounts {
		addAcc(1)
	}
	return ub.Build()
}

// bigAccountWhale: several small accounts surrounding one account with bigSlots
// storage entries (> deepStorageThreshold) that triggers the deep fan-out.
func bigAccountWhale(bigSlots int) whaleOpts {
	return whaleOpts{seed: 771, smallBefore: 8, smallBeforeSlots: 3, bigSlots: bigSlots, smallAfter: 8, smallAfterSlots: 2}
}

// whale1M: three whale accounts (750k/150k/5k storage slots) plus a 95k
// single-slot tail — ~1M storage keys. Stresses within-account storage, which
// single-level mount cannot parallelise (the 750k whale runs on one worker).
func whale1M() whaleOpts {
	return whaleOpts{seed: 919273, bigSlots: 750_000, extraWhales: []int{150_000, 5_000}, tailAccounts: 95_000}
}

// runMode selects which commitment engine the engineRoot/incrementalRoot
// dispatch drives.
type runMode int

const (
	modeSeq runMode = iota
	modeParallel
	modeStreaming
	modeStreamingScheduled
	modeStreamingUpdates
	modeStreamingPublic
)

// newSeqTrie builds the sequential HexPatriciaHashed over ms. Caller defers Release.
func newSeqTrie(t *testing.T, ms *MockState) *HexPatriciaHashed {
	t.Helper()
	return NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
}

// newParTrie builds a ParallelPatriciaHashed over ms with workers set and the
// context reset. workers <= 0 falls back to NumCPU on the engine side. Caller
// defers Release.
func newParTrie(t *testing.T, ms *MockState, workers int) *ParallelPatriciaHashed {
	t.Helper()
	tr := NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	tr.SetNumWorkers(workers)
	tr.ResetContext(ms)
	return tr
}

// newStreamCommitter builds a StreamingCommitter over ms (ms must already
// SetConcurrentCommitment(true)). When scheduler is true the background
// scheduler is started. Caller defers Release.
func newStreamCommitter(t *testing.T, ms *MockState, workers int, scheduler bool) *StreamingCommitter {
	t.Helper()
	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	sc.SetNumWorkers(workers)
	if scheduler {
		require.NoError(t, sc.StartScheduler(context.Background()))
	}
	return sc
}

// processRoot runs trie.Process over ut and returns a copy of the root.
func processRoot(t *testing.T, trie Trie, ut *Updates) []byte {
	t.Helper()
	root, err := trie.Process(context.Background(), ut, "", nil, WarmupConfig{})
	require.NoError(t, err)
	return common.Copy(root)
}

// processBatch applies one batch to ms and folds it through the engine selected
// by mode, returning the resulting root.
func processBatch(t *testing.T, ms *MockState, mode runMode, workers int, keys [][]byte, upds []Update) []byte {
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
	case modeStreamingUpdates:
		sc := newStreamCommitter(t, ms, workers, false)
		defer sc.Release()
		ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
		defer ut.Close()
		ut.SetStreamingCommitter(sc)
		require.True(t, ut.Streaming())
		for i, key := range keys {
			ut.TouchPlainKeyDirect(string(key), &upds[i])
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

// engineRoot folds a single batch through the engine selected by mode and
// returns the root plus the MockState (carrying committed branches).
func engineRoot(t *testing.T, mode runMode, workers int, keys [][]byte, upds []Update) ([]byte, *MockState) {
	t.Helper()
	ms := NewMockState(t)
	if mode != modeSeq {
		ms.SetConcurrentCommitment(true)
	}
	return processBatch(t, ms, mode, workers, keys, upds), ms
}

// incrementalRoot applies two batches to one MockState (batch-1 branches become
// DB state for batch-2) and returns the final root and the MockState.
func incrementalRoot(t *testing.T, mode runMode, workers int, k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update) ([]byte, *MockState) {
	t.Helper()
	ms := NewMockState(t)
	if mode != modeSeq {
		ms.SetConcurrentCommitment(true)
	}
	processBatch(t, ms, mode, workers, k1, u1)
	return processBatch(t, ms, mode, workers, k2, u2), ms
}

// requireRootParity drives the sequential and parallel engines over one batch and
// asserts their roots match, returning the (shared) sequential root.
func requireRootParity(t *testing.T, keys [][]byte, upds []Update, workers int) []byte {
	t.Helper()
	seqRoot, _ := engineRoot(t, modeSeq, 0, keys, upds)
	parRoot, _ := engineRoot(t, modeParallel, workers, keys, upds)
	require.Equal(t, seqRoot, parRoot,
		"sequential and parallel root hashes must match (numWorkers=%d)", workers)
	return seqRoot
}

// requireAllEnginesParity asserts the parallel, streaming and scheduled-streaming
// roots all match the sequential root after the same two batches, dumping
// divergent branches on mismatch.
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

// requireBranchParity asserts the two MockStates hold byte-identical branches.
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

// snapshotBranches deep-copies a MockState's stored branches so a later
// comparison can detect any mid-block write.
func snapshotBranches(ms *MockState) map[string][]byte {
	snap := make(map[string][]byte, len(ms.cm))
	for k, v := range ms.cm {
		snap[k] = append([]byte(nil), v...)
	}
	return snap
}

// requireBranchesUnchanged asserts ms holds exactly the snapshot branches — used
// to prove a mid-block re-fold deferred everything and wrote nothing.
func requireBranchesUnchanged(t *testing.T, snap map[string][]byte, ms *MockState) {
	t.Helper()
	require.Equalf(t, len(snap), len(ms.cm), "a mid-block re-fold changed the stored branch count")
	for k, v := range ms.cm {
		require.Truef(t, bytes.Equal(snap[k], v), "a mid-block re-fold wrote branch %x", []byte(k))
	}
}

// nibs returns a copy of the given nibble values as a slice.
func nibs(vals ...byte) []byte {
	out := make([]byte, len(vals))
	copy(out, vals)
	return out
}

// nibbleAddr returns an address whose hashed first nibble matches targetNibble
// and whose seed differentiates it from other addresses in the same nibble.
func nibbleAddr(targetNibble, seed int) []byte {
	return findAddressForNibble(targetNibble, seed)
}

// slotHashBytes returns a deterministic 32-byte storage slot identifier derived
// from i; each i produces a distinct slot.
func slotHashBytes(i int) []byte {
	var out [32]byte
	binary.BigEndian.PutUint64(out[24:], uint64(i)+1)
	return out[:]
}
