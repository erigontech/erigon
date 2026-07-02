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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

type recordingStreamSink struct{ touches int }

func (r *recordingStreamSink) TouchKey(hashedKey, plainKey []byte, update *Update) {
	r.touches++
}

func TestUpdates_NewEmpty_PreservesStreaming(t *testing.T) {
	u := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	sink := &recordingStreamSink{}
	u.SetStreamingCommitter(sink)

	rotated := u.NewEmpty()
	require.True(t, rotated.Streaming(), "NewEmpty dropped the streaming funnel")

	addr := make([]byte, length.Addr)
	addr[0] = 0xab
	rotated.TouchPlainKeyDirect(string(addr), &Update{Flags: BalanceUpdate})
	require.NotZero(t, sink.touches, "rotated buffer did not forward touch to the streamer")
}

func streamingRoot(t *testing.T, workers int, keys [][]byte, upds []Update, idxOrder []int) ([]byte, *MockState) {
	t.Helper()
	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	require.NoError(t, ms.applyPlainUpdates(keys, upds))

	sc := newStreamCommitter(t, ms, workers, false)
	defer sc.Release()
	for _, i := range idxOrder {
		sc.TouchKey(KeyToHexNibbleHash(keys[i]), keys[i], nil)
	}
	root, err := sc.Process(context.Background())
	require.NoError(t, err)
	return root, ms
}

func sequentialRoot(t *testing.T, keys [][]byte, upds []Update) ([]byte, *MockState) {
	t.Helper()
	return engineRoot(t, modeSeq, 0, keys, upds)
}

func TestStreaming_RandomOrderParity(t *testing.T) {
	t.Parallel()
	keys, upds := buildMixedCorpus(99, 6000)

	idx := make([]int, len(keys))
	for i := range idx {
		idx[i] = i
	}
	rnd := rand.New(rand.NewSource(0xBEEF))
	rnd.Shuffle(len(idx), func(i, j int) { idx[i], idx[j] = idx[j], idx[i] })

	seqRoot, seqMs := sequentialRoot(t, keys, upds)
	for _, w := range []int{1, 4, 8} {
		strRoot, strMs := streamingRoot(t, w, keys, upds, idx)
		require.Equalf(t, seqRoot, strRoot, "streaming(workers=%d) root != sequential", w)
		requireBranchParity(t, seqMs, strMs)
	}
}

func TestStreaming_DeepBranchParity(t *testing.T) {
	t.Parallel()
	keys, upds := buildWhaleCorpus(bigAccountWhale(15_000))

	idx := make([]int, len(keys))
	for i := range idx {
		idx[i] = i
	}
	rnd := rand.New(rand.NewSource(0xD00D))
	rnd.Shuffle(len(idx), func(i, j int) { idx[i], idx[j] = idx[j], idx[i] })

	seqRoot, seqMs := sequentialRoot(t, keys, upds)
	for _, w := range []int{1, 4, 8} {
		strRoot, strMs := streamingRoot(t, w, keys, upds, idx)
		require.Equalf(t, seqRoot, strRoot, "streaming(workers=%d) deep root != sequential", w)
		requireBranchParity(t, seqMs, strMs)
	}
}

// Corpus must be collapse-free: repeated re-folds over a non-empty pre-image only stay parity-clean without collapses.
func TestStreaming_NonEmptyPrevRefold(t *testing.T) {
	t.Parallel()
	const workers = 4
	ctx := context.Background()
	k1, u1 := genRandomAccountsStorage(400)
	k2, u2 := sparseBatch2(k1, 3, false)

	seqRoot, seqMs := runIncremental(t, modeSeq, 0, k1, u1, k2, u2)

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)

	require.NoError(t, ms.applyPlainUpdates(k1, u1))
	sc1 := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	sc1.SetNumWorkers(workers)
	for _, k := range k1 {
		sc1.TouchKey(KeyToHexNibbleHash(k), k, nil)
	}
	_, err := sc1.Process(ctx)
	require.NoError(t, err)
	sc1.Release()
	require.NotEmpty(t, ms.cm)
	snap := snapshotBranches(ms)

	require.NoError(t, ms.applyPlainUpdates(k2, u2))
	sc2 := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer sc2.Release()
	sc2.SetNumWorkers(workers)
	for _, k := range k2 {
		sc2.TouchKey(KeyToHexNibbleHash(k), k, nil)
	}
	for range 4 {
		require.NoError(t, sc2.foldDirtySplits(ctx))
		requireBranchesUnchanged(t, snap, ms)
	}
	root2, err := sc2.Process(ctx)
	require.NoError(t, err)
	require.Equal(t, seqRoot, root2, "streaming block-2 root after re-folds != sequential")
	requireBranchParity(t, seqMs, ms)
}

// touched nibbles get hash bytes seeded from seed so each update's cells are source-distinguishable in dedup assertions.
func makeBranch(prefix []byte, afterMap uint16, touched []int, seed byte, prev []byte) *DeferredBranchUpdate {
	var cells [16]cellEncodeData
	var tm uint16
	for _, n := range touched {
		tm |= uint16(1) << uint(n)
		cells[n].hashLen = 32
		for b := range cells[n].hash {
			cells[n].hash[b] = seed + byte(n)
		}
	}
	return getDeferredUpdate(prefix, tm, tm, afterMap, &cells, prev)
}

// Settle condition is idle, not all-clean: the coalescing gate may legitimately leave a split dirty.
func waitSchedulerIdle(t *testing.T, sc *StreamingCommitter) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	stable := 0
	for {
		queued := 0
		sc.trieMu.RLock()
		for _, s := range sc.splits {
			s.mu.Lock()
			if s.queued {
				queued++
			}
			s.mu.Unlock()
		}
		sc.trieMu.RUnlock()
		if queued == 0 && len(sc.dirtyCh) == 0 && sc.inFlight.Load() == 0 {
			if stable++; stable >= 5 {
				return
			}
		} else {
			stable = 0
		}
		if time.Now().After(deadline) {
			t.Fatalf("scheduler did not go idle: queued=%d dirtyCh=%d", queued, len(sc.dirtyCh))
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStreaming_SchedulerConcurrentParity(t *testing.T) {
	t.Parallel()
	keys, upds := buildMixedCorpus(77, 4000)
	seqRoot, seqMs := sequentialRoot(t, keys, upds)

	for _, w := range []int{1, 4, 8} {
		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		require.NoError(t, ms.applyPlainUpdates(keys, upds))

		sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		sc.SetNumWorkers(w)
		require.NoError(t, sc.StartScheduler(context.Background()))

		const goroutines = 4
		var wg sync.WaitGroup
		for g := range goroutines {
			wg.Add(1)
			go func(start int) {
				defer wg.Done()
				for i := start; i < len(keys); i += goroutines {
					sc.TouchKey(KeyToHexNibbleHash(keys[i]), keys[i], nil)
				}
			}(g)
		}
		wg.Wait()

		root, err := sc.Process(context.Background())
		require.NoError(t, err)
		require.Equalf(t, seqRoot, root, "scheduler(workers=%d) root != sequential", w)
		requireBranchParity(t, seqMs, ms)
		sc.Release()
	}
}

func TestStreaming_StorageMidAccountFold(t *testing.T) {
	t.Parallel()

	t.Run("retouch_after_fold", func(t *testing.T) {
		keys, upds := buildMixedCorpus(33, 2500)
		seqRoot, seqMs := sequentialRoot(t, keys, upds)

		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		require.NoError(t, ms.applyPlainUpdates(keys, upds))

		sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		defer sc.Release()
		sc.SetNumWorkers(4)
		sc.SetEagerFold(1) // below production floor: forces the gate path
		require.NoError(t, sc.StartScheduler(context.Background()))

		const hold = 8
		for i := 0; i < len(keys)-hold; i++ {
			sc.TouchKey(KeyToHexNibbleHash(keys[i]), keys[i], nil)
		}
		waitSchedulerIdle(t, sc)

		for i := len(keys) - hold; i < len(keys); i++ {
			sc.TouchKey(KeyToHexNibbleHash(keys[i]), keys[i], nil)
		}
		waitSchedulerIdle(t, sc)

		root, err := sc.Process(context.Background())
		require.NoError(t, err)
		require.Equal(t, seqRoot, root, "streaming re-touch root != sequential")
		requireBranchParity(t, seqMs, ms)
	})

	t.Run("mid_account_fold", func(t *testing.T) {
		keys, upds := genRandomAccountsStorage(300)

		withheld := -1
		for i, k := range keys {
			if len(k) > length.Addr {
				withheld = i
				break
			}
		}
		require.GreaterOrEqual(t, withheld, 0, "corpus must contain a storage key")
		targetNib := KeyToHexNibbleHash(keys[withheld])[0]

		seqRoot, seqMs := sequentialRoot(t, keys, upds)

		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		require.NoError(t, ms.applyPlainUpdates(keys, upds))

		sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		defer sc.Release()
		sc.SetNumWorkers(4)
		sc.SetEagerFold(1) // below production floor so the target split folds and foldGate fires

		var once sync.Once
		sc.SetFoldGate(func(nib byte) {
			if nib != targetNib {
				return
			}
			once.Do(func() {
				sc.TouchKey(KeyToHexNibbleHash(keys[withheld]), keys[withheld], nil)
			})
		})

		require.NoError(t, sc.StartScheduler(context.Background()))
		for i, k := range keys {
			if i == withheld {
				continue
			}
			sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
		}
		waitSchedulerIdle(t, sc)

		root, err := sc.Process(context.Background())
		require.NoError(t, err)
		require.Equal(t, seqRoot, root, "streaming storage-mid-fold root != sequential")
		requireBranchParity(t, seqMs, ms)
		require.Positive(t, sc.RefoldCount(), "expected a mid-fold re-fold to be triggered")
	})
}

func TestStreaming_SchedulerCollapseParity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	k1, u1 := genRandomAccountsStorage(400)
	k2, u2 := sparseBatch2(k1, 3, true)
	seqRoot, seqMs := runIncremental(t, modeSeq, 0, k1, u1, k2, u2)

	for _, w := range []int{1, 4} {
		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)

		require.NoError(t, ms.applyPlainUpdates(k1, u1))
		sc1 := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		sc1.SetNumWorkers(w)
		for _, k := range k1 {
			sc1.TouchKey(KeyToHexNibbleHash(k), k, nil)
		}
		_, err := sc1.Process(ctx)
		require.NoError(t, err)
		sc1.Release()
		snap := snapshotBranches(ms)

		require.NoError(t, ms.applyPlainUpdates(k2, u2))
		sc2 := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		sc2.SetNumWorkers(w)
		require.NoError(t, sc2.StartScheduler(ctx))
		for _, k := range k2 {
			sc2.TouchKey(KeyToHexNibbleHash(k), k, nil)
		}
		sc2.Stop()
		requireBranchesUnchanged(t, snap, ms)

		root, err := sc2.Process(ctx)
		require.NoError(t, err)
		sc2.Release()
		require.Equalf(t, seqRoot, root, "scheduler collapse(workers=%d) root != sequential", w)
		requireBranchParity(t, seqMs, ms)
	}
}

func TestStreaming_SplitMergeCollisionDedup(t *testing.T) {
	t.Parallel()
	prefix := []byte{0x0a, 0x03}
	all := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	storePrev := func(ms *MockState) []byte {
		_, err := ApplyDeferredBranchUpdates(
			[]*DeferredBranchUpdate{makeBranch(prefix, 0xFFFF, all, 0x10, nil)},
			1, ms.PutBranch)
		require.NoError(t, err)
		return append([]byte(nil), ms.cm[string(prefix)]...)
	}

	guardMs := NewMockState(t)
	gp := storePrev(guardMs)
	require.NoError(t, applyDeferredGuarded(guardMs, []*DeferredBranchUpdate{
		makeBranch(prefix, 0xFFFF, []int{0, 1, 2, 3}, 0x40, gp),
		makeBranch(prefix, 0xFFFF, []int{4, 5, 6, 7}, 0x80, gp),
	}, 4))
	_, _, grow, err := BranchData(guardMs.cm[string(prefix)]).decodeCells()
	require.NoError(t, err)
	require.Equal(t, byte(0x40+0), grow[0].hash[0], "guard kept the split set's low-half cell")
	require.Equal(t, byte(0x80+4), grow[4].hash[0], "guard kept the merge set's high-half cell")
	require.Equal(t, byte(0x10+8), grow[8].hash[0], "guard kept prev's untouched cell")

	bareMs := NewMockState(t)
	bp := storePrev(bareMs)
	_, err = ApplyDeferredBranchUpdates([]*DeferredBranchUpdate{
		makeBranch(prefix, 0xFFFF, []int{0, 1, 2, 3}, 0x40, bp),
		makeBranch(prefix, 0xFFFF, []int{4, 5, 6, 7}, 0x80, bp),
	}, 4, bareMs.PutBranch)
	require.NoError(t, err)
	_, _, brow, err := BranchData(bareMs.cm[string(prefix)]).decodeCells()
	require.NoError(t, err)
	require.Equal(t, byte(0x10+0), brow[0].hash[0], "bare apply dropped the split set's low half (clobbered by prev)")
	require.Equal(t, byte(0x80+4), brow[4].hash[0], "bare apply kept the merge set's high half")
}

// Models the production write-only apply context: PutBranch buffers writes that Branch cannot read until drained.
type drainContext struct {
	*MockState
	pending map[string][]byte
}

func newDrainContext(ms *MockState) *drainContext {
	return &drainContext{MockState: ms, pending: make(map[string][]byte)}
}

func (d *drainContext) PutBranch(prefix, data, _ []byte) error {
	d.pending[string(prefix)] = common.Copy(data)
	return nil
}

func (d *drainContext) drain() error {
	for k, v := range d.pending {
		if err := d.MockState.PutBranch([]byte(k), v, nil); err != nil {
			return err
		}
	}
	return nil
}

func TestStreaming_SplitMergeCollisionDedup_WriteOnlyCtx(t *testing.T) {
	t.Parallel()
	prefix := []byte{0x0a, 0x03}
	all := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	ms := NewMockState(t)
	_, err := ApplyDeferredBranchUpdates(
		[]*DeferredBranchUpdate{makeBranch(prefix, 0xFFFF, all, 0x10, nil)},
		1, ms.PutBranch)
	require.NoError(t, err)
	prev := append([]byte(nil), ms.cm[string(prefix)]...)

	dctx := newDrainContext(ms)
	require.NoError(t, applyDeferredGuarded(dctx, []*DeferredBranchUpdate{
		makeBranch(prefix, 0xFFFF, []int{0, 1, 2, 3}, 0x40, prev),
		makeBranch(prefix, 0xFFFF, []int{4, 5, 6, 7}, 0x80, prev),
	}, 4))
	require.NoError(t, dctx.drain())

	_, _, row, err := BranchData(ms.cm[string(prefix)]).decodeCells()
	require.NoError(t, err)
	require.Equal(t, byte(0x40+0), row[0].hash[0], "split set's low-half cell survived write-only apply")
	require.Equal(t, byte(0x80+4), row[4].hash[0], "merge set's high-half cell survived write-only apply")
	require.Equal(t, byte(0x10+8), row[8].hash[0], "prev's untouched cell survived")
}

func foldedSplitCount(sc *StreamingCommitter) int {
	n := 0
	sc.trieMu.RLock()
	for _, s := range sc.splits {
		s.mu.Lock()
		if s.folded {
			n++
		}
		s.mu.Unlock()
	}
	sc.trieMu.RUnlock()
	return n
}

func splitCount(sc *StreamingCommitter) int {
	sc.trieMu.RLock()
	n := len(sc.splits)
	sc.trieMu.RUnlock()
	return n
}

func TestStreaming_FoldEagerPolicy(t *testing.T) {
	t.Parallel()
	keys, upds := buildMixedCorpus(123, 5000)
	seqRoot, seqMs := sequentialRoot(t, keys, upds)

	newCommitter := func() (*StreamingCommitter, *MockState) {
		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		require.NoError(t, ms.applyPlainUpdates(keys, upds))
		sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		sc.SetNumWorkers(4)
		sc.SetEagerFold(1) // below production floor: forces the eager path
		return sc, ms
	}

	t.Run("lazy_fall_through", func(t *testing.T) {
		sc, ms := newCommitter()
		defer sc.Release()
		for _, k := range keys {
			sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
		}
		require.Zero(t, foldedSplitCount(sc), "no scheduler: nothing folds before Process")
		root, err := sc.Process(context.Background())
		require.NoError(t, err)
		require.Equal(t, seqRoot, root, "lazy fall-through root != sequential")
		requireBranchParity(t, seqMs, ms)
	})

	t.Run("eager_drained", func(t *testing.T) {
		sc, ms := newCommitter()
		defer sc.Release()
		require.NoError(t, sc.StartScheduler(context.Background()))
		for _, k := range keys {
			sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
		}
		waitSchedulerIdle(t, sc)
		require.Positive(t, foldedSplitCount(sc), "eager policy must fold splits in the background")
		root, err := sc.Process(context.Background())
		require.NoError(t, err)
		require.Equal(t, seqRoot, root, "eager drained root != sequential")
		requireBranchParity(t, seqMs, ms)
	})

	t.Run("eager_partial_fall_through", func(t *testing.T) {
		sc, ms := newCommitter()
		defer sc.Release()
		require.NoError(t, sc.StartScheduler(context.Background()))
		for _, k := range keys {
			sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
		}
		root, err := sc.Process(context.Background())
		require.NoError(t, err)
		require.Equal(t, seqRoot, root, "eager partial fall-through root != sequential")
		requireBranchParity(t, seqMs, ms)
	})
}

// carried=true touches via TouchPlainKeyDirect (carried *Update); carried=false via TouchPlainKey (nil/ctx-read).
func streamingViaUpdatesRoot(t *testing.T, workers int, keys [][]byte, upds []Update, carried bool) ([]byte, *MockState) {
	t.Helper()
	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	require.NoError(t, ms.applyPlainUpdates(keys, upds))

	sc := newStreamCommitter(t, ms, workers, false)
	defer sc.Release()

	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()
	ut.SetStreamingCommitter(sc)
	require.True(t, ut.Streaming())

	for i, key := range keys {
		if carried {
			ut.TouchPlainKeyDirect(string(key), &upds[i])
		} else {
			ut.TouchPlainKey(string(key), nil, ut.TouchAccount)
		}
	}
	root, err := sc.Process(context.Background())
	require.NoError(t, err)
	return root, ms
}

func TestStreaming_UpdatesFunnelParity(t *testing.T) {
	t.Parallel()
	keys, upds := buildMixedCorpus(7, 6000)
	seqRoot, seqMs := sequentialRoot(t, keys, upds)

	for _, carried := range []bool{false, true} {
		for _, w := range []int{1, 4, 8} {
			root, ms := streamingViaUpdatesRoot(t, w, keys, upds, carried)
			require.Equalf(t, seqRoot, root, "funnel(carried=%v,workers=%d) root != sequential", carried, w)
			requireBranchParity(t, seqMs, ms)
		}
	}
}

func TestStreaming_UpdatesLifetimeRegression(t *testing.T) {
	t.Parallel()
	keys, upds := buildMixedCorpus(31, 4000)
	seqRoot, seqMs := sequentialRoot(t, keys, upds)

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	require.NoError(t, ms.applyPlainUpdates(keys, upds))

	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer sc.Release()
	sc.SetNumWorkers(4)

	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()
	ut.SetStreamingCommitter(sc)

	var held []*Update
	for i, key := range keys {
		kb := append([]byte(nil), key...)
		ub := upds[i]
		ut.TouchPlainKeyDirect(common.ToStringZeroCopy(kb), &ub)
		// Corrupt the caller's key backing after Touch: a non-copying intern would fold the corruption.
		for j := range kb {
			kb[j] ^= 0xFF
		}
		held = append(held, &ub)
	}
	// Clobber every caller-owned Update before folding: a non-copying branch would fold these deletes.
	for _, u := range held {
		*u = Update{Flags: DeleteUpdate}
	}

	root, err := sc.Process(context.Background())
	require.NoError(t, err)
	require.Equal(t, seqRoot, root, "mutating caller buffers after Touch changed the root")
	requireBranchParity(t, seqMs, ms)
}

func TestInitializeTrieAndUpdates_StreamingVariant(t *testing.T) {
	t.Parallel()

	cfg := DefaultTrieConfig()
	cfg.Variant = VariantStreamingHexPatricia
	trie, upd := InitializeTrieAndUpdates(ModeDirect, t.TempDir(), cfg)
	defer upd.Close()
	defer trie.Release()

	require.IsType(t, (*ParallelPatriciaHashed)(nil), trie)
	require.Equal(t, VariantStreamingHexPatricia, trie.Variant())
	pt := trie.(*ParallelPatriciaHashed)
	require.NotNil(t, pt.streaming, "streaming variant must attach a StreamingCommitter")
	require.Equal(t, ModeParallel, upd.Mode())
	require.NotNil(t, upd.parallel)
	require.True(t, upd.Streaming(), "Updates must forward touches to the committer")
}

func streamingViaPublicProcessRoot(t *testing.T, workers int, keys [][]byte, upds []Update) ([]byte, *MockState) {
	t.Helper()
	return engineRoot(t, modeStreamingPublic, workers, keys, upds)
}

func TestStreaming_PublicProcessParity(t *testing.T) {
	t.Parallel()
	keys, upds := buildWhaleCorpus(bigAccountWhale(15_000))
	seqRoot, seqMs := sequentialRoot(t, keys, upds)

	for _, w := range []int{1, 4, 8} {
		root, ms := streamingViaPublicProcessRoot(t, w, keys, upds)
		require.Equalf(t, seqRoot, root, "public Process(workers=%d) root != sequential", w)
		requireBranchParity(t, seqMs, ms)
	}
}

func requireResetClean(t *testing.T, sc *StreamingCommitter) {
	t.Helper()
	require.Empty(t, sc.splits, "Reset left stale split state")
	require.Nil(t, sc.deferredForCaller, "Reset left staged deferred updates")
	require.Nil(t, sc.base, "Reset left the scheduler base alive")
	require.False(t, sc.started.Load(), "Reset left the scheduler running")
	require.NotNil(t, sc.trie, "Reset must keep a usable prefix trie")
	require.Zero(t, sc.trie.root.subtreeCount, "Reset left prefix-trie entries")
}

func TestStreaming_NewSplitMidBlock(t *testing.T) {
	t.Parallel()
	keys, upds := buildMixedCorpus(91, 4000)
	seqRoot, seqMs := sequentialRoot(t, keys, upds)

	var early, late []int
	for i, k := range keys {
		if KeyToHexNibbleHash(k)[0] < 8 {
			early = append(early, i)
		} else {
			late = append(late, i)
		}
	}
	require.NotEmpty(t, early, "corpus must populate the lower top-nibble half")
	require.NotEmpty(t, late, "corpus must populate the upper top-nibble half")

	for _, w := range []int{1, 4, 8} {
		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		require.NoError(t, ms.applyPlainUpdates(keys, upds))

		sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		sc.SetNumWorkers(w)
		sc.SetEagerFold(1) // below production floor: forces background folding
		require.NoError(t, sc.StartScheduler(context.Background()))

		for _, i := range early {
			sc.TouchKey(KeyToHexNibbleHash(keys[i]), keys[i], nil)
		}
		waitSchedulerIdle(t, sc)
		earlySplits := splitCount(sc)
		require.Positive(t, earlySplits, "early touches must create the lower-half splits")

		for _, i := range late {
			sc.TouchKey(KeyToHexNibbleHash(keys[i]), keys[i], nil)
		}
		waitSchedulerIdle(t, sc)
		require.Greater(t, splitCount(sc), earlySplits, "late touches must create brand-new top-nibble splits")

		root, err := sc.Process(context.Background())
		require.NoError(t, err)
		require.Equalf(t, seqRoot, root, "new-split-mid-block(workers=%d) root != sequential", w)
		requireBranchParity(t, seqMs, ms)
		sc.Release()
	}
}

func TestStreaming_MultiBlockReuse(t *testing.T) {
	t.Parallel()

	t.Run("reset", func(t *testing.T) {
		ctx := context.Background()
		k1, u1 := genRandomAccountsStorage(400)
		k2, u2 := sparseBatch2(k1, 3, true)

		seqRoot1, _ := sequentialRoot(t, k1, u1)
		seqRoot2, seqMs := runIncremental(t, modeSeq, 0, k1, u1, k2, u2)

		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		defer sc.Release()
		sc.SetNumWorkers(4)

		require.NoError(t, ms.applyPlainUpdates(k1, u1))
		require.NoError(t, sc.StartScheduler(ctx))
		for _, k := range k1 {
			sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
		}
		root1, err := sc.Process(ctx)
		require.NoError(t, err)
		require.Equal(t, seqRoot1, root1, "block-1 streaming root != sequential")

		sc.Reset()
		requireResetClean(t, sc)

		require.NoError(t, ms.applyPlainUpdates(k2, u2))
		require.NoError(t, sc.StartScheduler(ctx))
		for _, k := range k2 {
			sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
		}
		root2, err := sc.Process(ctx)
		require.NoError(t, err)
		require.Equal(t, seqRoot2, root2, "block-2 streaming root after reset != sequential")
		requireBranchParity(t, seqMs, ms)
	})

	t.Run("no_reset", func(t *testing.T) {
		ctx := context.Background()
		k1, u1 := genRandomAccountsStorage(400)
		k2, u2 := sparseBatch2(k1, 3, true)

		seqRoot1, _ := sequentialRoot(t, k1, u1)
		seqRoot2, seqMs := runIncremental(t, modeSeq, 0, k1, u1, k2, u2)

		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		defer sc.Release()
		sc.SetNumWorkers(4)

		require.NoError(t, ms.applyPlainUpdates(k1, u1))
		for _, k := range k1 {
			sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
		}
		root1, err := sc.Process(ctx)
		require.NoError(t, err)
		require.Equal(t, seqRoot1, root1, "block-1 streaming root != sequential")

		require.Zero(t, sc.trie.root.subtreeCount, "Process left block-1 keys in the prefix trie")
		require.Empty(t, sc.splits, "Process left stale split state")

		require.NoError(t, ms.applyPlainUpdates(k2, u2))
		for _, k := range k2 {
			sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
		}
		root2, err := sc.Process(ctx)
		require.NoError(t, err)
		require.Equal(t, seqRoot2, root2, "block-2 streaming root without reset != sequential")
		requireBranchParity(t, seqMs, ms)
	})

	t.Run("scheduler_then_no_reset", func(t *testing.T) {
		ctx := context.Background()
		k1, u1 := genRandomAccountsStorage(400)
		k2, u2 := sparseBatch2(k1, 3, true)

		seqRoot1, _ := sequentialRoot(t, k1, u1)
		seqRoot2, seqMs := runIncremental(t, modeSeq, 0, k1, u1, k2, u2)

		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		defer sc.Release()
		sc.SetNumWorkers(4)

		require.NoError(t, ms.applyPlainUpdates(k1, u1))
		require.NoError(t, sc.StartScheduler(ctx))
		for _, k := range k1 {
			sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
		}
		root1, err := sc.Process(ctx)
		require.NoError(t, err)
		require.Equal(t, seqRoot1, root1, "block-1 scheduler root != sequential")
		require.Nil(t, sc.base, "Process must release the scheduler base after folding it down")

		require.NoError(t, ms.applyPlainUpdates(k2, u2))
		for _, k := range k2 {
			sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
		}
		root2, err := sc.Process(ctx)
		require.NoError(t, err)
		require.Equal(t, seqRoot2, root2, "block-2 lazy root after scheduler block (no reset) != sequential")
		requireBranchParity(t, seqMs, ms)
	})
}

func TestStreamingCommitterStateRoundTrip(t *testing.T) {
	t.Parallel()

	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 42).
		Balance("a1b2c3d4e5f60718293a4b5c6d7e8f9012345678", 7).
		Balance("ffeeddccbbaa00112233445566778899aabbccdd", 99).
		Build()

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))

	cfg := DefaultTrieConfig()
	cfg.Variant = VariantStreamingHexPatricia
	trie, ut := InitializeTrieAndUpdates(ModeDirect, t.TempDir(), cfg)
	defer ut.Close()
	defer trie.Release()

	pt := trie.(*ParallelPatriciaHashed)
	pt.SetNumWorkers(1)
	pt.SetTrieContextFactory(mockTrieCtxFactory(ms))
	pt.ResetContext(ms)

	for _, key := range plainKeys {
		ut.TouchPlainKey(string(key), nil, ut.TouchAccount)
	}
	published, err := trie.Process(context.Background(), ut, "", nil, WarmupConfig{})
	require.NoError(t, err)
	require.NotEmpty(t, published)

	tmpl := pt.RootTrie()
	require.True(t, tmpl.rootChecked, "streaming template.rootChecked must be promoted from the committer")
	require.True(t, tmpl.rootTouched, "streaming template.rootTouched must be promoted from the committer")
	require.True(t, tmpl.rootPresent, "streaming template.rootPresent must be promoted from the committer")

	encoded, err := tmpl.EncodeCurrentState(nil)
	require.NoError(t, err)
	require.NotEmpty(t, encoded, "EncodeCurrentState must capture template state mirrored from the committer")

	trie2, ut2 := InitializeTrieAndUpdates(ModeDirect, t.TempDir(), cfg)
	defer ut2.Close()
	defer trie2.Release()
	pt2 := trie2.(*ParallelPatriciaHashed)
	pt2.SetTrieContextFactory(mockTrieCtxFactory(ms))
	pt2.ResetContext(ms)
	require.NoError(t, pt2.RootTrie().SetState(encoded))

	restored, err := pt2.RootHash()
	require.NoError(t, err)
	require.Equal(t, published, restored,
		"RootHash after SetState must reproduce the published streaming root")
}

func TestKeyArena_PointerStability(t *testing.T) {
	var arena keyArena

	inputs := make([][]byte, 0, 4096)
	got := make([][]byte, 0, 4096)
	// 4096 small keys roll the arena over at least two chunks.
	for i := range 4096 {
		in := bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 32)
		inputs = append(inputs, in)
		got = append(got, arena.copy(in))
	}
	// Oversized key forces the max(keyArenaChunk, len) allocation path.
	big := bytes.Repeat([]byte{0xAB}, keyArenaChunk+128)
	inputs = append(inputs, big)
	got = append(got, arena.copy(big))

	for i, in := range inputs {
		require.True(t, bytes.Equal(in, got[i]),
			"key %d corrupted: returned slice does not equal its input", i)
		require.Equal(t, len(got[i]), cap(got[i]),
			"key %d not full-cap: a caller append could overwrite the next key", i)
	}

	for i := range got {
		for j := range got[i] {
			got[i][j] = byte(i)
		}
	}
	for i := range got {
		for j := range got[i] {
			require.Equal(t, byte(i), got[i][j],
				"key %d overlaps another arena slice (overwritten at byte %d)", i, j)
		}
	}
}

// Mixes shallow account forks with a whale storage subtree forking below depth 64 to fan out folds across many depths.
func buildMultiDepthCorpus() (keys [][]byte, upds []Update) {
	mk, mu := buildMixedCorpus(0xD15C0DE, 6000)
	_, _, _, _, pk, pu, _ := whaleByNibble(20_000)
	keys = append(keys, mk...)
	keys = append(keys, pk...)
	upds = append(upds, mu...)
	upds = append(upds, pu...)
	return keys, upds
}

func parallelRoot(t *testing.T, workers int, keys [][]byte, upds []Update) ([]byte, *MockState) {
	t.Helper()
	return engineRoot(t, modeParallel, workers, keys, upds)
}

func TestStreaming_MultiDepthSplitParity(t *testing.T) {
	t.Parallel()
	keys, upds := buildMultiDepthCorpus()

	seqRoot, seqMs := sequentialRoot(t, keys, upds)

	parRoot, parMs := parallelRoot(t, 4, keys, upds)
	require.Equal(t, seqRoot, parRoot, "parallel root != sequential")
	requireBranchParity(t, seqMs, parMs)

	for _, w := range []int{1, 4, 8} {
		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		require.NoError(t, ms.applyPlainUpdates(keys, upds))

		sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		sc.SetNumWorkers(w)
		for i := range keys {
			sc.TouchKey(KeyToHexNibbleHash(keys[i]), keys[i], nil)
		}
		root, err := sc.Process(context.Background())
		require.NoError(t, err)

		require.Equalf(t, seqRoot, root, "multi-depth streaming(workers=%d) root != ModeDirect", w)
		require.Equalf(t, parRoot, root, "multi-depth streaming(workers=%d) root != ModeParallel", w)
		requireBranchParity(t, seqMs, ms)
		// First-commit whale: no persisted branch at the account prefix, so it takes the
		// streaming-recursion fallback, not the deep fold; root/branch parity above is the check.
		sc.Release()
	}
}

// Embedding the collapsing whale among many accounts is load-bearing: a single-account trie yields a degenerate collapse root.
func TestStreaming_MultiDepthCollapseParity(t *testing.T) {
	t.Parallel()
	wk1, wu1, wk2, wu2 := whaleCollapseCorpus()
	for _, tc := range []struct {
		name    string
		mixSeed int64
		mixKeys int
	}{
		{"embed_c0ffee_4000", 0xC0FFEE, 4000},
		{"embed_5eed_3000", 0x5EED, 3000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mk, mu := buildMixedCorpus(tc.mixSeed, tc.mixKeys)
			k1 := append(append([][]byte{}, mk...), wk1...)
			u1 := append(append([]Update{}, mu...), wu1...)

			seqRoot, seqMs := runIncremental(t, modeSeq, 0, k1, u1, wk2, wu2)
			for _, w := range []int{1, 4, 8} {
				requireIncrementalEquiv(t, k1, u1, wk2, wu2, w)
				strRoot, strMs := runIncremental(t, modeStreaming, w, k1, u1, wk2, wu2)
				require.Equalf(t, seqRoot, strRoot, "whale storage collapse(workers=%d) root != sequential", w)
				requireBranchParity(t, seqMs, strMs)
			}
		})
	}
}

func TestStreaming_FullCollapseParity(t *testing.T) {
	t.Parallel()
	wk1, wu1, wk2, wu2 := whaleFullCollapseCorpus()
	mk, mu := buildMixedCorpus(0xC0FFEE, 4000)
	k1 := append(append([][]byte{}, mk...), wk1...)
	u1 := append(append([]Update{}, mu...), wu1...)
	for _, w := range []int{1, 4, 8} {
		requireIncrementalEquiv(t, k1, u1, wk2, wu2, w)
	}
}

func TestStreaming_StorageInteriorSplits(t *testing.T) {
	t.Parallel()
	_, _, _, _, pk, upds, _ := whaleByNibble(20_000)

	seqRoot, seqMs := sequentialRoot(t, pk, upds)

	for _, w := range []int{1, 4, 8} {
		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		require.NoError(t, ms.applyPlainUpdates(pk, upds))

		sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		sc.SetNumWorkers(w)
		for i := range pk {
			sc.TouchKey(KeyToHexNibbleHash(pk[i]), pk[i], nil)
		}
		root, err := sc.Process(context.Background())
		require.NoError(t, err)

		require.Equalf(t, seqRoot, root, "whale storage-interior split(workers=%d) root != sequential", w)
		requireBranchParity(t, seqMs, ms)
		// First-commit whale: no persisted branch at the account prefix, so it takes the
		// streaming-recursion fallback, not the deep fold; root/branch parity above is the check.
		sc.Release()
	}
}

// Block 2 leaves a third of slots untouched on disk: the untouched-sibling read forces a mid-fold self-flush.
func whaleCollapseCorpus() (pk [][]byte, upds []Update, k2 [][]byte, u2 []Update) {
	var addr []byte
	addr, _, _, _, pk, upds, _ = whaleByNibble(30_000)

	k2 = [][]byte{addr}
	u2 = []Update{{Flags: BalanceUpdate | NonceUpdate}}
	u2[0].Balance.SetUint64(99)
	u2[0].Nonce = 7
	for i := range pk {
		if len(pk[i]) == length.Addr || i%3 == 2 {
			continue
		}
		var u Update
		if i%3 == 0 {
			u.Flags = DeleteUpdate
		} else {
			u.Flags = StorageUpdate
			u.StorageLen = 4
			binary.BigEndian.PutUint32(u.Storage[:4], uint32(i)*97+3)
		}
		k2 = append(k2, pk[i])
		u2 = append(u2, u)
	}
	return pk, upds, k2, u2
}

// Deleting every storage slot exercises the all-children-collapsed path, which must yield the empty-trie root, not a zero hash.
func whaleFullCollapseCorpus() (pk [][]byte, upds []Update, k2 [][]byte, u2 []Update) {
	var addr []byte
	addr, _, _, _, pk, upds, _ = whaleByNibble(30_000)

	k2 = [][]byte{addr}
	u2 = []Update{{Flags: BalanceUpdate | NonceUpdate}}
	u2[0].Balance.SetUint64(99)
	u2[0].Nonce = 7
	for i := range pk {
		if len(pk[i]) == length.Addr {
			continue
		}
		k2 = append(k2, pk[i])
		u2 = append(u2, Update{Flags: DeleteUpdate})
	}
	return pk, upds, k2, u2
}
