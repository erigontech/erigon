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

	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer sc.Release()
	sc.SetNumWorkers(workers)
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
	ms := NewMockState(t)
	require.NoError(t, ms.applyPlainUpdates(keys, upds))
	tr := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer tr.Release()
	ut := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, keys, upds)
	defer ut.Close()
	root, err := tr.Process(context.Background(), ut, "", nil, WarmupConfig{})
	require.NoError(t, err)
	return root, ms
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
// (concurrentStorageRoot) and the account-leaf storageRoot/CodeHash assembly.
func TestStreaming_DeepBranchParity(t *testing.T) {
	t.Parallel()
	keys, upds := buildBigAccountCorpus(15_000)

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

// TestStreaming_DeepLocalWalkUsed proves the deep big-storage fold now runs
// through the streaming-local walk (dfsDeepLocal/storageRootLocal) rather than
// parallel_mount.go: the committer's DeepLocalFolds counter must fire and the
// root must still match sequential. This is the Task-2 isolation gate.
func TestStreaming_DeepLocalWalkUsed(t *testing.T) {
	t.Parallel()
	keys, upds := buildBigAccountCorpus(15_000)

	seqRoot, seqMs := sequentialRoot(t, keys, upds)

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	require.NoError(t, ms.applyPlainUpdates(keys, upds))

	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer sc.Release()
	sc.SetNumWorkers(4)
	for i := range keys {
		sc.TouchKey(KeyToHexNibbleHash(keys[i]), keys[i], nil)
	}
	root, err := sc.Process(context.Background())
	require.NoError(t, err)

	require.Equal(t, seqRoot, root, "streaming-local deep root != sequential")
	requireBranchParity(t, seqMs, ms)
	require.NotZero(t, sc.DeepLocalFolds(), "deep account must fold through the streaming-local walk")
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

// TestStreaming_RefoldAfterCollapse is the focused collapse assertion: a streaming
// fold over a delete batch that collapses branches, layered on a non-empty prev,
// must match sequential root + branches. The lazy path folds each split once at
// Process, so the engine's mid-fold self-flush stays correct here; the multi
// re-fold of a collapsed split is a Task-4 concern (see foldDirtySplits).
func TestStreaming_RefoldAfterCollapse(t *testing.T) {
	t.Parallel()
	k1, u1 := genRandomAccountsStorage(400)
	k2, u2 := sparseBatch2(k1, 3, true)
	for _, w := range []int{1, 4} {
		seqRoot, seqMs := runIncremental(t, modeSeq, 0, k1, u1, k2, u2)
		strRoot, strMs := runIncremental(t, modeStreaming, w, k1, u1, k2, u2)
		require.Equalf(t, seqRoot, strRoot, "streaming(workers=%d) collapse root != sequential", w)
		requireBranchParity(t, seqMs, strMs)
	}
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
		if queued == 0 && len(sc.dirtyCh) == 0 {
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

// TestStreaming_RetouchAfterFold folds a first wave of touches to completion,
// then re-touches keys landing in already-folded splits, forcing those splits to
// re-fold before Process. The final root + branches must match sequential — the
// re-fold picks up the later touch rather than serving the stale cached cell.
func TestStreaming_RetouchAfterFold(t *testing.T) {
	t.Parallel()
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
}

// TestStreaming_StorageMidAccountFold injects a withheld storage-slot touch into
// the owning account's split via foldGate, exactly while that split is folding.
// The in-flight fold's snapshot misses the slot, so its gen check fails, the
// result is discarded (RefoldCount bumps) and the split re-folds with the new
// slot — proving the storageRoot cross-dependency is honored. Final root +
// branches match sequential.
func TestStreaming_StorageMidAccountFold(t *testing.T) {
	t.Parallel()
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

	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer sc.Release()
	sc.SetNumWorkers(workers)

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
	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	require.NoError(t, ms.applyPlainUpdates(keys, upds))

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
	root, err := trie.Process(context.Background(), ut, "", nil, WarmupConfig{})
	require.NoError(t, err)
	return root, ms
}

// TestStreaming_PublicProcessParity is the Task-7 integration check: the
// streaming variant, selected by InitializeTrieAndUpdates and driven through the
// public Trie.Process path, yields root + every stored branch == sequential on
// the big-storage corpus across worker counts.
func TestStreaming_PublicProcessParity(t *testing.T) {
	t.Parallel()
	keys, upds := buildBigAccountCorpus(15_000)
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
// and their folds drained first, then keys in the upper half create brand-new
// top-nibble splits that did not exist when the earlier ones folded. Each new
// split is its own per-nibble state, independent of any in-flight fold, so the
// drained Process root + branches match sequential.
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
		require.Positive(t, foldedSplitCount(sc), "early splits must fold before the late ones appear")
		earlyFolded := foldedSplitCount(sc)

		for _, i := range late {
			sc.TouchKey(KeyToHexNibbleHash(keys[i]), keys[i], nil)
		}
		waitSchedulerIdle(t, sc)
		require.Greater(t, foldedSplitCount(sc), earlyFolded, "late touches must create and fold new splits")

		root, err := sc.Process(context.Background())
		require.NoError(t, err)
		require.Equalf(t, seqRoot, root, "new-split-mid-block(workers=%d) root != sequential", w)
		requireBranchParity(t, seqMs, ms)
		sc.Release()
	}
}

// TestStreaming_MultiBlockResetWithScheduler reuses one committer across two
// blocks with Reset between, the scheduler running each block. Block 1 commits a
// from-scratch corpus; Reset must wipe all per-block state; block 2 (a
// delete/collapse batch over block 1's DB branches) must match the sequential
// incremental root and branches — proving Reset leaves no stale split state.
func TestStreaming_MultiBlockResetWithScheduler(t *testing.T) {
	t.Parallel()
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
}

// TestStreaming_MultiBlockNoResetAccumulation reuses one committer across two
// blocks the way production does: the commitment calculator rotates only the
// Updates buffer (NewEmpty) and never calls sc.Reset() between blocks. Process
// must drain its own prefix trie + split state at the block boundary, otherwise
// block 2 re-folds block 1's keys (unbounded growth within a batch). Roots stay
// correct either way, so the accumulation is asserted directly on the trie.
func TestStreaming_MultiBlockNoResetAccumulation(t *testing.T) {
	t.Parallel()
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
