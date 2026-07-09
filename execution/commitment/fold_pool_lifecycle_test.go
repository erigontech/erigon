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
	"encoding/hex"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// These tests pin the fold pool's failure discipline: every error path (a read failing mid-leaf, a
// merge base that cannot seed, a deferred-apply write failing) must fail closed — return the error,
// leave the branch store untouched, recycle every deferred update, and leave the engine reusable —
// and a cancelled Process must unwind cleanly and stay restorable. The pin-scope net proves no task
// reads a context after its own cleanup (the mmap use-after-munmap class). The recycle/pin classes
// are also gated by the package -race run, which surfaces a double-pooled worker as a data race.

// errInjected is the sentinel a failState returns so a test can assert the surfaced error is the
// one it injected and not an unrelated failure.
var errInjected = errors.New("fold pool lifecycle test: injected failure")

type failKind int

const (
	failOff   failKind = iota
	failError          // surface errInjected
	failEmpty          // return an absent (empty) branch — the vanished-branch case
)

// injector fires on the after-th matching call. prefix, when set, restricts matching to that exact
// compact key so a test can target one specific read (e.g. a single merge base seed).
type injector struct {
	after  int
	prefix []byte
	kind   failKind
	calls  int
	fired  bool
}

func (in *injector) trip(key []byte) failKind {
	if in.after <= 0 {
		return failOff
	}
	if in.prefix != nil && !bytes.Equal(key, in.prefix) {
		return failOff
	}
	in.calls++
	if in.calls >= in.after {
		in.fired = true
		return in.kind
	}
	return failOff
}

// failState wraps a MockState with per-operation fault injection shared across every worker context
// the factory hands out. All injector mutations are serialized so the concurrent pool races clean.
type failState struct {
	*MockState
	mu      sync.Mutex
	branch  injector
	account injector
	storage injector
	put     injector
}

func newFailState(ms *MockState) *failState { return &failState{MockState: ms} }

func (fs *failState) factory() TrieContextFactory {
	return func() (PatriciaContext, func()) { return fs, func() {} }
}

func (fs *failState) Branch(prefix []byte) ([]byte, kv.Step, error) {
	fs.mu.Lock()
	k := fs.branch.trip(prefix)
	fs.mu.Unlock()
	switch k {
	case failError:
		return nil, 0, errInjected
	case failEmpty:
		return nil, 0, nil
	}
	return fs.MockState.Branch(prefix)
}

func (fs *failState) Account(plainKey []byte) (*Update, error) {
	fs.mu.Lock()
	k := fs.account.trip(plainKey)
	fs.mu.Unlock()
	if k == failError {
		return nil, errInjected
	}
	return fs.MockState.Account(plainKey)
}

func (fs *failState) Storage(plainKey []byte) (*Update, error) {
	fs.mu.Lock()
	k := fs.storage.trip(plainKey)
	fs.mu.Unlock()
	if k == failError {
		return nil, errInjected
	}
	return fs.MockState.Storage(plainKey)
}

func (fs *failState) PutBranch(prefix, data, prevData []byte) error {
	fs.mu.Lock()
	k := fs.put.trip(prefix)
	fs.mu.Unlock()
	if k == failError {
		return errInjected
	}
	return fs.MockState.PutBranch(prefix, data, prevData)
}

// seqReference folds the batches through the sequential trie into a fresh store, returning the final
// root and the reference branch store the candidate engine must reproduce byte-for-byte.
func seqReference(t *testing.T, batches ...engineBatch) ([]byte, *MockState) {
	t.Helper()
	ms := NewMockState(t)
	tr := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer tr.Release()
	var root []byte
	for _, b := range batches {
		root = processBatch(t, ms, tr, b.keys, b.upds)
	}
	return common.Copy(root), ms
}

// seedBatch1 folds batch1 through the sequential trie into a fresh concurrent store, returning the
// store (with its on-disk branches) and the encoded trie state a streaming batch-2 restores from.
func seedBatch1(t *testing.T, batch1 engineBatch) (*MockState, []byte) {
	t.Helper()
	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	tr := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer tr.Release()
	processBatch(t, ms, tr, batch1.keys, batch1.upds)
	blob, err := tr.EncodeCurrentState(nil)
	require.NoError(t, err)
	return ms, blob
}

// streamAttempt folds one already-plain-applied batch through a StreamingCommitter wired to factory,
// restoring the carried root from blob. It returns the root, the next state blob, and any error; on
// failure both byte slices are nil. The committer and template are released before returning.
func streamAttempt(ctx context.Context, t *testing.T, ms *MockState, factory TrieContextFactory,
	workers int, keys [][]byte, blob []byte) ([]byte, []byte, error) {
	t.Helper()
	sc := NewStreamingCommitter(factory, length.Addr, DefaultTrieConfig())
	defer sc.Release()
	sc.SetNumWorkers(workers)

	tmpl := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer tmpl.Release()
	require.NoError(t, tmpl.SetState(blob))
	sc.SeedRootFrom(tmpl)

	for _, k := range keys {
		sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
	}
	root, err := sc.Process(ctx)
	if err != nil {
		return nil, nil, err
	}
	sc.PromoteRootInto(tmpl)
	next, encErr := tmpl.EncodeCurrentState(nil)
	require.NoError(t, encErr)
	return common.Copy(root), next, nil
}

// TestFoldPoolLifecycle_LeafReadErrorFailsClosed injects an account read failure mid-leaf and
// asserts the streaming Process fails closed: the error surfaces, the branch store is untouched
// (folds never write; only the post-fold apply does), and a fresh attempt reproduces the sequential
// root and branches — proving nothing leaked to corrupt the recycled pool.
func TestFoldPoolLifecycle_LeafReadErrorFailsClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	batches := balancedBatches()
	b1, b2 := batches[0], batches[1]

	seqRoot, seqMs := seqReference(t, b1, b2)

	ms, blob := seedBatch1(t, b1)
	require.NoError(t, ms.applyPlainUpdates(b2.keys, b2.upds))
	before := snapshotBranches(ms)

	fs := newFailState(ms)
	fs.account = injector{after: 1, kind: failError}
	_, _, err := streamAttempt(ctx, t, ms, fs.factory(), 3, b2.keys, blob)
	require.ErrorIs(t, err, errInjected, "the injected account read failure must surface")
	require.True(t, fs.account.fired, "the fault must have fired")
	requireBranchesUnchanged(t, before, ms)

	root, _, err := streamAttempt(ctx, t, ms, mockTrieCtxFactory(ms), 3, b2.keys, blob)
	require.NoError(t, err, "the engine must fold cleanly after the failed attempt")
	require.Equal(t, seqRoot, root, "recovered streaming root != sequential")
	requireBranchParity(t, seqMs, ms)
}

// TestFoldPoolLifecycle_ReusableAfterReset drives one committer to an error, Resets it, and folds
// the same batch cleanly on the same object — proving Reset restores usability after a fault.
func TestFoldPoolLifecycle_ReusableAfterReset(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	batches := balancedBatches()
	b1, b2 := batches[0], batches[1]

	seqRoot, seqMs := seqReference(t, b1, b2)

	ms, blob := seedBatch1(t, b1)
	require.NoError(t, ms.applyPlainUpdates(b2.keys, b2.upds))

	fs := newFailState(ms)
	fs.account = injector{after: 1, kind: failError}

	sc := NewStreamingCommitter(fs.factory(), length.Addr, DefaultTrieConfig())
	defer sc.Release()
	sc.SetNumWorkers(3)

	tmpl := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer tmpl.Release()
	require.NoError(t, tmpl.SetState(blob))

	sc.SeedRootFrom(tmpl)
	for _, k := range b2.keys {
		sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
	}
	_, err := sc.Process(ctx)
	require.ErrorIs(t, err, errInjected)

	sc.Reset()
	sc.SetTrieContextFactory(mockTrieCtxFactory(ms))
	sc.SeedRootFrom(tmpl)
	for _, k := range b2.keys {
		sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
	}
	root, err := sc.Process(ctx)
	require.NoError(t, err)
	require.Equal(t, seqRoot, common.Copy(root), "root after Reset != sequential")
	requireBranchParity(t, seqMs, ms)
}

// mergeSeedFixture builds a re-touched whale whose depth-64 account prefix carries an on-disk
// storage-root branch, derives its fold DAG at a small K (so the storage subtree is a real merge),
// and returns the pool, the derived root task wired to a clean base, and the compact key the merge
// base seeds — the exact read a fault targets.
func mergeSeedFixture(t *testing.T, ctx context.Context, ms *MockState, keys [][]byte, upds []Update,
	accPrefix []byte, factory TrieContextFactory) (*foldPool, *foldTask, func()) {
	t.Helper()
	require.NoError(t, ms.applyPlainUpdates(keys, upds))

	tr := newPrefixTrie()
	for i, k := range keys {
		tr.Insert(KeyToHexNibbleHash(k), k, &upds[i])
	}
	root := deriveFoldFrontier(tr.root, 8, seedableOf(t, ms))
	require.NotNil(t, root)

	acct := findByPrefix(root, accPrefix)
	require.NotNil(t, acct, "the whale account leaf must exist at the depth-64 prefix")
	require.NotNil(t, acct.storage, "the whale must carry a storage-root merge subtask")
	require.Equal(t, foldMerge, acct.storage.kind)

	base := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())
	bctx, bclean := mockTrieCtxFactory(ms)()
	base.ResetContext(bctx)
	base.branchEncoder.setDeferUpdates(true)
	base.SetLeaveDeferredForCaller(true)
	require.NoError(t, unfoldRootWall(ctx, base))
	seedRootBase(base)
	root.base = base

	pool := &foldPool{
		numWorkers: 4,
		ctxFactory: factory,
		workerPool: &sync.Pool{New: func() any { return NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig()) }},
	}
	return pool, root, func() {
		base.Release()
		if bclean != nil {
			bclean()
		}
	}
}

// TestFoldPoolLifecycle_MergeSeedFailsClosed pins the merge-base seed error paths: a read error and
// a vanished (empty) branch. Derivation confirmed the branch, so seeding it must hard-error rather
// than fold an empty, sibling-dropping wall; either way the pool recycles every task's deferred and
// writes nothing.
func TestFoldPoolLifecycle_MergeSeedFailsClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	batch1, whale, accPrefix, nib3Locs := whaleStorageCorpus(770077, 40, 8)
	target := nibbles.HexToCompact(accPrefix)

	cases := []struct {
		name   string
		kind   failKind
		expect error
	}{
		{"read error", failError, errInjected},
		{"vanished branch", failEmpty, errStorageBaseNotBranch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ms := NewMockState(t)
			ms.SetConcurrentCommitment(true)
			seqTrie := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
			processBatch(t, ms, seqTrie, batch1.keys, batch1.upds)
			seqTrie.Release()
			before := snapshotBranches(ms)

			rnd := rand.New(rand.NewSource(99))
			ub := NewUpdateBuilder()
			ub.Balance(whale, 55555)
			for _, l := range nib3Locs {
				val := make([]byte, 32)
				rnd.Read(val)
				ub.Storage(whale, l, hex.EncodeToString(val))
			}
			k2, u2 := ub.Build()

			fs := newFailState(ms)
			fs.branch = injector{after: 1, prefix: target, kind: tc.kind}

			pool, root, cleanup := mergeSeedFixture(t, ctx, ms, k2, u2, accPrefix, fs.factory())
			defer cleanup()

			deferred, err := pool.run(ctx, root)
			require.ErrorIs(t, err, tc.expect)
			require.Nil(t, deferred)
			require.True(t, fs.branch.fired, "the merge-base seed fault must have fired")

			for _, task := range collectFoldTasks(root, nil) {
				require.Nilf(t, task.deferred, "task %x leaked deferred updates after a failed run", task.prefix)
			}
			requireBranchesUnchanged(t, before, ms)
		})
	}
}

// TestFoldPoolLifecycle_ApplyErrorFailsClosed fails the first deferred-apply PutBranch and asserts
// the error surfaces, the store stays untouched (the write loop stops on the first failure), and a
// fresh engine over a clean store reproduces the sequential root.
func TestFoldPoolLifecycle_ApplyErrorFailsClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	batch := balancedBatches()[0]

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	require.NoError(t, ms.applyPlainUpdates(batch.keys, batch.upds))
	before := snapshotBranches(ms)

	fs := newFailState(ms)
	fs.put = injector{after: 1, kind: failError}
	_, _, err := streamAttempt(ctx, t, ms, fs.factory(), 3, batch.keys, nil)
	require.ErrorIs(t, err, errInjected)
	require.True(t, fs.put.fired, "the deferred-apply write fault must have fired")
	requireBranchesUnchanged(t, before, ms)

	seqRoot, _ := seqReference(t, batch)
	clean := NewMockState(t)
	clean.SetConcurrentCommitment(true)
	require.NoError(t, clean.applyPlainUpdates(batch.keys, batch.upds))
	root, _, err := streamAttempt(ctx, t, clean, mockTrieCtxFactory(clean), 3, batch.keys, nil)
	require.NoError(t, err)
	require.Equal(t, seqRoot, root, "recovered root != sequential")
}

// TestFoldPoolLifecycle_ContextCancel cancels the context around a streaming Process and asserts a
// clean unwind: the error surfaces, the store is untouched, and a second Process over the restored
// (SetState) template yields the sequential root and branches.
func TestFoldPoolLifecycle_ContextCancel(t *testing.T) {
	t.Parallel()
	batches := balancedBatches()
	b1, b2 := batches[0], batches[1]

	seqRoot, seqMs := seqReference(t, b1, b2)

	ms, blob := seedBatch1(t, b1)
	require.NoError(t, ms.applyPlainUpdates(b2.keys, b2.upds))
	before := snapshotBranches(ms)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := streamAttempt(cancelled, t, ms, mockTrieCtxFactory(ms), 3, b2.keys, blob)
	require.ErrorIs(t, err, context.Canceled)
	requireBranchesUnchanged(t, before, ms)

	root, _, err := streamAttempt(context.Background(), t, ms, mockTrieCtxFactory(ms), 3, b2.keys, blob)
	require.NoError(t, err, "Process must succeed after a cancelled attempt")
	require.Equal(t, seqRoot, root, "post-cancel streaming root != sequential")
	requireBranchParity(t, seqMs, ms)
}

// TestFoldPool_DispatchCancels drives dispatchFoldTasks with an already-cancelled context and a fold
// that surfaces it, asserting the pool returns the cancellation promptly instead of hanging.
func TestFoldPool_DispatchCancels(t *testing.T) {
	t.Parallel()
	root := mkFoldTask(foldMerge, nil)
	for range 200 {
		mkFoldTask(foldLeaf, root)
	}
	subTasks := collectFoldTasks(root, nil)
	setFoldPending(subTasks)

	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	fold := func(fctx context.Context, _ *foldTask) error { return fctx.Err() }

	done := make(chan error, 1)
	go func() { done <- dispatchFoldTasks(cctx, 3, root, subTasks, fold) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(30 * time.Second):
		t.Fatal("dispatchFoldTasks did not return after cancellation: deadlock")
	}
}

// pinState hands each factory() call a distinct context that flags any read arriving after its own
// cleanup (the mmap use-after-munmap class) and returns poisoned bytes so a leaked read also
// corrupts the fold. Every task must confine its reads to its own pin scope, so violations stays 0.
type pinState struct {
	ms         *MockState
	violations atomic.Int64
}

type pinCtx struct {
	ms         *MockState
	closed     atomic.Bool
	violations *atomic.Int64
}

func (ps *pinState) factory() TrieContextFactory {
	return func() (PatriciaContext, func()) {
		c := &pinCtx{ms: ps.ms, violations: &ps.violations}
		return c, func() { c.closed.Store(true) }
	}
}

var poison = bytes.Repeat([]byte{0xff}, 64)

func (c *pinCtx) Branch(prefix []byte) ([]byte, kv.Step, error) {
	if c.closed.Load() {
		c.violations.Add(1)
		return common.Copy(poison), 0, nil
	}
	return c.ms.Branch(prefix)
}

func (c *pinCtx) PutBranch(prefix, data, prevData []byte) error {
	if c.closed.Load() {
		c.violations.Add(1)
	}
	return c.ms.PutBranch(prefix, data, prevData)
}

func (c *pinCtx) Account(plainKey []byte) (*Update, error) {
	if c.closed.Load() {
		c.violations.Add(1)
	}
	return c.ms.Account(plainKey)
}

func (c *pinCtx) Storage(plainKey []byte) (*Update, error) {
	if c.closed.Load() {
		c.violations.Add(1)
	}
	return c.ms.Storage(plainKey)
}

// TestFoldPoolLifecycle_PinScope folds a batch where every worker context poisons itself on cleanup
// and asserts the fold still matches the sequential root and branches with zero post-cleanup reads —
// the regression net for a task retaining an mmap-backed read past its own pin scope.
func TestFoldPoolLifecycle_PinScope(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	batches := balancedBatches()
	b1, b2 := batches[0], batches[1]

	seqRoot, seqMs := seqReference(t, b1, b2)

	ms, blob := seedBatch1(t, b1)
	require.NoError(t, ms.applyPlainUpdates(b2.keys, b2.upds))

	ps := &pinState{ms: ms}
	root, _, err := streamAttempt(ctx, t, ms, ps.factory(), 3, b2.keys, blob)
	require.NoError(t, err)
	require.Equal(t, seqRoot, root, "pin-scoped streaming root != sequential")
	requireBranchParity(t, seqMs, ms)
	require.Zero(t, ps.violations.Load(), "a task read a context after its own cleanup")
}
