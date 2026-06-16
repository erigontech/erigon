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

// TestUpdates_NewEmpty_PreservesStreaming guards the calculator funnel: the
// commitment calculator rotates its buffer via NewEmpty (committer.go), and the
// rotated buffer must keep forwarding touches to the StreamingCommitter,
// otherwise the committer receives no touches and silently computes the
// unchanged base root.
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

// streamingRoot drives a StreamingCommitter over keys/upds touched in the order
// given by idxOrder, returning its root and MockState (with committed branches).
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

// sequentialRoot drives the sequential HexPatriciaHashed, returning root + state.
func sequentialRoot(t *testing.T, keys [][]byte, upds []Update) ([]byte, *MockState) {
	t.Helper()
	return engineRoot(t, modeSeq, 0, keys, upds)
}

// TestStreaming_RandomOrderParity feeds the mixed corpus through TouchKey in
// randomized (execution) order — order-independence is the premise; the in-order
// prefix-trie walk re-sorts at fold — and asserts root + every stored branch
// match the sequential run.
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

// TestStreaming_DeepBranchParity drives the deep-fan-out corpus (an account whose
// touched storage exceeds deepStorageThreshold, alongside many small accounts)
// through TouchKey in randomized order and asserts root + every stored branch
// match sequential. This exercises foldSplit's big-storage path
// (foldStorageRoot) and the account-leaf storageRoot/CodeHash assembly.
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

// TestStreaming_NonEmptyPrevRefold is the novel-correctness claim: re-folding a
// split repeatedly over a NON-EMPTY on-disk pre-image stays parity-clean because
// the committer flushes nothing mid-block, so every re-fold reads the same prev.
// After each re-fold the store must still equal the post-batch-1 snapshot. A
// from-scratch run cannot falsify this, hence the batch-1 commit first. The
// corpus is collapse-free (sparseBatch2 without deletes) — the regime where the
// invariant holds; see foldDirtySplits for the collapse caveat.
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

// makeBranch builds a hash-only-cell deferred branch update at prefix: afterMap
// declares the full child set, touched lists the nibbles this update supplies
// cells for (seeded so each nibble's hash is distinguishable by source), and prev
// is the on-disk pre-image the update merges onto.
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

// waitSchedulerIdle blocks until the background scheduler has drained its queue
// (no split queued and dirtyCh empty, stable for a short window). Under the
// re-fold coalescing gate a split can remain dirty between doublings, so "all
// clean" is no longer the settle condition — the scheduler folds what the gate
// allows and idles; Process folds the rest. Fails the test if it never idles.
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

// TestStreaming_SchedulerConcurrentParity runs the background scheduler while
// many goroutines touch keys concurrently, then asserts the drained Process root
// and every stored branch match sequential. The corpus is from-scratch
// (collapse-free), so background folds cache authoritative cells and Process only
// merges. Run under -race.
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

// TestStreaming_StorageMidAccountFold covers the two re-fold-before-Process
// regimes. The retouch subtest folds a first wave to completion, then re-touches
// keys landing in already-folded splits, forcing them to re-fold and pick up the
// later touch rather than serve a stale cached cell. The mid_account subtest
// injects a withheld storage-slot touch into the owning account's split via
// foldGate exactly while it folds: the in-flight snapshot misses the slot, its
// gen check fails, the result is discarded (RefoldCount bumps) and the split
// re-folds with the new slot — proving the storageRoot cross-dependency is
// honored. Both must match the sequential root + branches.
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
		sc.SetEagerFold(1) // small corpus: fold below the production floor so the gate path is exercised
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
		sc.SetEagerFold(1) // small corpus: fold below the production floor so the target split folds (foldGate fires)

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

// TestStreaming_SchedulerCollapseParity runs the background scheduler over a
// delete batch that collapses branches on a non-empty pre-image — the regime the
// Task-3 caveat flagged as unsafe for naive re-folds. After draining in-flight
// folds (Stop), the store must be byte-for-byte the post-block-1 snapshot: the
// overlay isolates every self-flush so no branch is written mid-block. Process
// then folds the still-dirty collapsed splits against the real ctx, and the final
// root + branches match sequential — proving the isolation prevents the
// double-apply corruption.
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

// TestStreaming_SplitMergeCollisionDedup models a prefix emitted by two slices
// (a split set and the merge set) over a non-empty pre-image: each supplies a
// different half of the same full child set. The duplicate-prefix flush guard
// re-reads prev so the second update merges onto the first's write — both halves
// survive. Bare ApplyDeferredBranchUpdates does not dedup: the second update
// still carries the block-start prev and its merge clobbers the first half. The
// branch merger treats branch2's afterMap as authoritative, so the clobber is
// silent — exactly the data loss the guard prevents.
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

// drainContext models the production concurrent apply context: PutBranch writes
// into a drain-later collector (last-write-wins on a key) that Branch cannot read
// until drained. This reproduces the read-your-writes gap MockState hides — the
// flush-then-reread guard saw the stale pre-image here and clobbered the first
// colliding update.
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

// TestStreaming_SplitMergeCollisionDedup_WriteOnlyCtx asserts the dedup guard
// survives a write-only apply context (the production collector), where a flushed
// branch is not visible to a subsequent Branch read. Both colliding halves must
// survive; with the prior reread-based guard the second half merged against the
// stale pre-image and clobbered the first.
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

// foldedSplitCount reports how many splits hold an authoritative background-folded
// cell (eager policy reached them before Process).
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

// TestStreaming_FoldEagerPolicy exercises the single shipped fold trigger
// (foldEager, fold-on-dirty) across both regimes it governs: the eager path,
// where every touched split is background-folded before Process (drained), and
// the fall-through path, where splits the scheduler never reached fold at Process
// — both the lazy committer (scheduler never started) and a scheduler stopped
// immediately after touching. Every regime must match the sequential root and
// branches; the drained eager run must actually fold splits in the background.
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
		sc.SetEagerFold(1) // exercise the eager path below the production floor
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

// streamingViaUpdatesRoot drives a StreamingCommitter through the production
// funnel: an Updates buffer in ModeParallel with SetStreamingCommitter, fed via
// the same TouchPlainKey/TouchPlainKeyDirect entry points sdctx.TouchKey and the
// commitment calculator use. carried=true exercises the carried-*Update path
// (TouchPlainKeyDirect); carried=false the nil/ctx-read path (TouchPlainKey).
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

// TestStreaming_UpdatesFunnelParity drives the committer through the public
// Updates.TouchPlainKey* funnel (ModeParallel + SetStreamingCommitter) and
// asserts both the carried-*Update path and the nil/ctx-read path produce
// root + every stored branch == sequential.
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

// TestStreaming_UpdatesLifetimeRegression proves the ModeParallel streaming
// branch copies both the key bytes and the *Update into the stable arena: every
// caller-owned key buffer and Update is clobbered immediately after Touch
// returns, yet the root matches a run that never mutated its inputs.
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
		// Corrupt the caller's key backing right after Touch returns; a
		// non-copying intern would carry the corruption into the fold.
		for j := range kb {
			kb[j] ^= 0xFF
		}
		held = append(held, &ub)
	}
	// Clobber every caller-owned Update before folding; a non-copying branch
	// would fold these DeleteUpdates instead of the real state.
	for _, u := range held {
		*u = Update{Flags: DeleteUpdate}
	}

	root, err := sc.Process(context.Background())
	require.NoError(t, err)
	require.Equal(t, seqRoot, root, "mutating caller buffers after Touch changed the root")
	requireBranchParity(t, seqMs, ms)
}

// TestInitializeTrieAndUpdates_StreamingVariant asserts the streaming variant
// builds a *ParallelPatriciaHashed shell with a streaming committer attached and
// a ModeParallel Updates buffer wired to forward touches to that same committer.
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
	// The streaming committer reuses ModeParallel's intern/prefix-trie machinery.
	require.Equal(t, ModeParallel, upd.Mode())
	require.NotNil(t, upd.parallel)
	require.True(t, upd.Streaming(), "Updates must forward touches to the committer")
}

// streamingViaPublicProcessRoot drives the streaming committer through the
// public Trie.Process path: InitializeTrieAndUpdates builds the shell + wires the
// committer to the Updates buffer, touches flow through TouchPlainKey, and
// trie.Process delegates to processStreaming. This is the production entry point
// (the Trie interface), unlike streamingViaUpdatesRoot which calls sc.Process.
func streamingViaPublicProcessRoot(t *testing.T, workers int, keys [][]byte, upds []Update) ([]byte, *MockState) {
	t.Helper()
	return engineRoot(t, modeStreamingPublic, workers, keys, upds)
}

// TestStreaming_PublicProcessParity is the Task-7 integration check: the
// streaming variant, selected by InitializeTrieAndUpdates and driven through the
// public Trie.Process path, yields root + every stored branch == sequential on
// the big-storage corpus across worker counts.
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

// requireResetClean asserts Reset wiped every per-block field so the committer
// can be reused with no carry-over.
func requireResetClean(t *testing.T, sc *StreamingCommitter) {
	t.Helper()
	require.Empty(t, sc.splits, "Reset left stale split state")
	require.Nil(t, sc.deferredForCaller, "Reset left staged deferred updates")
	require.Nil(t, sc.base, "Reset left the scheduler base alive")
	require.False(t, sc.started.Load(), "Reset left the scheduler running")
	require.NotNil(t, sc.trie, "Reset must keep a usable prefix trie")
	require.Zero(t, sc.trie.root.subtreeCount, "Reset left prefix-trie entries")
}

// TestStreaming_NewSplitMidBlock is the scheduler-specific new-split case: with
// the background pool running, all keys in the lower top-nibble half are touched
// and their folds scheduled first, then keys in the upper half create brand-new
// top-nibble splits that did not exist when the earlier ones were scheduled. Each
// new split is its own per-nibble state, independent of any in-flight fold, so the
// drained Process root + branches match sequential. Background folding is
// best-effort (the coalescing gate may defer a settled split to Process), so the
// scenario is asserted via deterministic split creation, not fold timing.
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
		sc.SetEagerFold(1) // small per-nibble corpus: fold below the production floor
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

// TestStreaming_MultiBlockReuse reuses one committer across two blocks (block 1 a
// from-scratch corpus, block 2 a delete/collapse batch over block 1's DB
// branches) in both rotation regimes. The reset subtest runs the scheduler each
// block with sc.Reset() between, proving Reset wipes all per-block split state.
// The no_reset subtest mirrors production — the calculator rotates only the
// Updates buffer and never calls Reset — so Process must drain its own prefix
// trie + split state at the block boundary (asserted directly on the trie) or
// block 2 re-folds block 1's keys. Both must match the sequential incremental
// root + branches.
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
}

// TestStreamingCommitterStateRoundTrip drives the streaming variant through the
// public Trie.Process path, encodes the resulting trie state via the persistence
// template, restores it into a fresh streaming instance, and asserts the
// restored RootHash matches the originally published value. This is the
// commitmentdb persistence path (encodeCommitmentState -> restorePatriciaState)
// taken on a process restart; without promoting the committer's folded root into
// the template, EncodeCurrentState would serialize a stale/empty root.
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

	// The template's root flags must mirror the committer's terminal state; they
	// are serialized below and any drift surfaces as a restore-then-continue bug.
	tmpl := pt.RootTrie()
	require.True(t, tmpl.rootChecked, "streaming template.rootChecked must be promoted from the committer")
	require.True(t, tmpl.rootTouched, "streaming template.rootTouched must be promoted from the committer")
	require.True(t, tmpl.rootPresent, "streaming template.rootPresent must be promoted from the committer")

	encoded, err := tmpl.EncodeCurrentState(nil)
	require.NoError(t, err)
	require.NotEmpty(t, encoded, "EncodeCurrentState must capture template state mirrored from the committer")

	// Restore into a fresh streaming instance, simulating a process restart.
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

// TestKeyArena_PointerStability verifies the chunked keyArena returns stable,
// independent slices: prior keys survive a new-chunk allocation, each returned
// slice is full-cap so a caller append cannot clobber the next key, and an
// oversize key still copies its full contents.
func TestKeyArena_PointerStability(t *testing.T) {
	var arena keyArena

	inputs := make([][]byte, 0, 4096)
	got := make([][]byte, 0, 4096)
	// Push enough small keys to roll over at least two 64KB chunks.
	for i := range 4096 {
		in := bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 32) // 64 bytes/key
		inputs = append(inputs, in)
		got = append(got, arena.copy(in))
	}
	// A key larger than one chunk forces the max(keyArenaChunk, len) path.
	big := bytes.Repeat([]byte{0xAB}, keyArenaChunk+128)
	inputs = append(inputs, big)
	got = append(got, arena.copy(big))

	for i, in := range inputs {
		require.True(t, bytes.Equal(in, got[i]),
			"key %d corrupted: returned slice does not equal its input", i)
		require.Equal(t, len(got[i]), cap(got[i]),
			"key %d not full-cap: a caller append could overwrite the next key", i)
	}

	// Stamp a unique byte into every returned slice, then confirm none was
	// overwritten by a neighbour sharing the same backing chunk.
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

// buildMultiDepthCorpus mixes account-trie forks (thousands of independent
// accounts → split-points at several shallow account-trie depths) with a whale
// storage subtree whose deep forks split below the account/storage boundary
// (depth > 64). Together they exercise concurrent folding at many depths in one
// batch, which a single-whale or accounts-only corpus cannot.
func buildMultiDepthCorpus() (keys [][]byte, upds []Update) {
	mk, mu := buildMixedCorpus(0xD15C0DE, 6000)
	_, _, _, _, pk, pu, _ := whaleByNibble(20_000)
	keys = append(keys, mk...)
	keys = append(keys, pk...)
	upds = append(upds, mu...)
	upds = append(upds, pu...)
	return keys, upds
}

// parallelRoot drives ModeParallel over a single batch, returning the root and
// the MockState (with committed branches) for parity comparison.
func parallelRoot(t *testing.T, workers int, keys [][]byte, upds []Update) ([]byte, *MockState) {
	t.Helper()
	return engineRoot(t, modeParallel, workers, keys, upds)
}

// TestStreaming_MultiDepthSplitParity is the headline Task-5 parity gate: a
// corpus with split-points at SEVERAL depths must fold via the streaming
// concurrent engine to the SAME root and stored-branch set as sequential
// ModeDirect AND ModeParallel, at every worker count. DeepLocalFolds asserts the
// whale's account@64 boundary still routes through foldStorageRoot (the flat
// per-first-nibble fan-out).
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
		require.NotZerof(t, sc.DeepLocalFolds(), "account@64 must fold through foldStorageRoot (workers=%d)", w)
		sc.Release()
	}
}

// TestStreaming_MultiDepthCollapseParity is the deep-collapse parity gate: a
// whale account whose deep storage collapses (block 2 deletes 1/3 + updates 1/3)
// while EMBEDDED among thousands of other accounts must fold via the streaming
// concurrent engine to the SAME root and byte-identical stored-branch set as
// sequential ModeDirect AND ModeParallel, at every worker count. Embedding is
// load-bearing: a single-account storage trie yields a degenerate
// incremental-collapse root no embedding-insensitive concurrent fold can match.
// Both embedding seeds are swept so a collapse self-flush that dropped a deletion
// (phantom child) or clobbered an untouched on-disk sibling breaks branch parity.
// Surfaces the streaming-mode deep-collapse divergence fixed by
// docs/plans/20260609-streaming-collapse-fold-fix.md.
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

// TestStreaming_FullCollapseParity is the full-deletion variant: block 2 deletes
// EVERY whale storage slot (embedded among other accounts), so the whale becomes
// storage-less. Streaming must yield the empty-trie storageRoot for the account
// (not a zero hash) and match ModeDirect == ModeParallel at every worker count.
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

// TestStreaming_StorageInteriorSplits is the headline Task-3 check: a whale
// account whose storage spans many slots must fold via the flat per-first-nibble
// fan-out (foldStorageRoot, ~16-way below the account/storage boundary) while
// still matching the sequential root and stored branch set. The account folds
// through foldStorageRoot (DeepLocalFolds), never as a split-point (its depth-64
// node carries the account terminator).
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
		require.NotZerof(t, sc.DeepLocalFolds(), "account must fold through foldStorageRoot (workers=%d)", w)
		sc.Release()
	}
}

// whaleCollapseCorpus builds a two-block whale-storage delete corpus that drives
// the concurrent deep-fold path. Block 1 (pk/upds) is a whale whose storage spans
// many slots. Block 2 (k2/u2) touches the account and rewrites a SUBSET of the
// slots — a third deleted, a third updated, a third left untouched on disk — so
// the block-2 fold still crosses deepStorageThreshold (routing through
// foldStorageRoot's split fan-out) while the deletes collapse interior storage
// branches AND untouched on-disk siblings must be preserved across the fold. That
// untouched-sibling read is what makes the engine self-flush mid-fold; the fold
// must write only deferred (applied once at end of block) and stay parity-clean.
func whaleCollapseCorpus() (pk [][]byte, upds []Update, k2 [][]byte, u2 []Update) {
	var addr []byte
	addr, _, _, _, pk, upds, _ = whaleByNibble(30_000)

	k2 = [][]byte{addr}
	u2 = []Update{{Flags: BalanceUpdate | NonceUpdate}}
	u2[0].Balance.SetUint64(99)
	u2[0].Nonce = 7
	for i := range pk {
		if len(pk[i]) == length.Addr || i%3 == 2 {
			continue // leave every third slot untouched on disk
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

// whaleFullCollapseCorpus is whaleCollapseCorpus's full-deletion variant: block 2
// touches the account and DELETES every storage slot, so the whale becomes
// storage-less (storageRoot == emptyRoot). Every first-storage-nibble subtree
// collapses to empty, exercising the all-children-collapsed path that must yield
// the empty-trie root rather than a zero hash.
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
