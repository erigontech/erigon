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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
)

func lifecycleCorpus() (k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update, kc [][]byte, uc []Update) {
	var addrs []string
	for i, nib := range []int{1, 3, 7, 0xb} {
		addrs = append(addrs, addrHex(findAddressForNibble(nib, 600+i)), addrHex(findAddressForNibble(nib, 700+i)))
	}

	ub1 := NewUpdateBuilder()
	for i, a := range addrs {
		ub1.Balance(a, uint64(1000+i))
	}
	ub1.Storage(addrs[0], hex.EncodeToString(slotHashBytes(1)), "beef")
	ub1.Storage(addrs[0], hex.EncodeToString(slotHashBytes(2)), "f00d")
	k1, u1 = ub1.Build()

	ub2 := NewUpdateBuilder().Balance(addrs[2], 2222).Balance(addrs[5], 5555)
	k2, u2 = ub2.Build()

	ubc := NewUpdateBuilder()
	for i, a := range addrs {
		bal := uint64(1000 + i)
		switch i {
		case 2:
			bal = 2222
		case 5:
			bal = 5555
		}
		ubc.Balance(a, bal)
	}
	ubc.Storage(addrs[0], hex.EncodeToString(slotHashBytes(1)), "beef")
	ubc.Storage(addrs[0], hex.EncodeToString(slotHashBytes(2)), "f00d")
	kc, uc = ubc.Build()
	return k1, u1, k2, u2, kc, uc
}

func touchBatch(t *testing.T, ms *MockState, ut *Updates, keys [][]byte, upds []Update) {
	t.Helper()
	require.NoError(t, ms.applyPlainUpdates(keys, upds))
	for _, k := range keys {
		ut.TouchPlainKey(string(k), nil, ut.TouchAccount)
	}
}

func TestModeParallel_ProcessConsumesUpdates(t *testing.T) {
	t.Parallel()
	k1, u1, k2, u2, kc, uc := lifecycleCorpus()

	t.Run("parallel", func(t *testing.T) {
		t.Parallel()
		oracle, _ := engineRoot(t, modeSeq, 0, kc, uc)

		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		tr := newParTrie(t, ms, 4)
		defer tr.Release()
		ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
		defer ut.Close()

		touchBatch(t, ms, ut, k1, u1)
		require.Equal(t, uint64(len(k1)), ut.Size())
		processRoot(t, tr, ut)
		require.Zero(t, ut.Size(), "Process left the touched-key collection unconsumed")
		if root := ut.parallel.trie.root; root != nil {
			require.Zero(t, root.subtreeCount, "Process left the prefix trie populated")
		}

		touchBatch(t, ms, ut, k2, u2)
		require.Equal(t, uint64(len(k2)), ut.Size(), "block-2 collection must hold only block-2 keys")
		got := processRoot(t, tr, ut)
		require.Zero(t, ut.Size())
		require.Equal(t, oracle, got)

		again := processRoot(t, tr, ut)
		require.Equal(t, got, again, "a zero-touch Process must return the carried root")
	})
}

func TestModeParallel_ErrorKeepsCollection(t *testing.T) {
	t.Parallel()
	k1, u1, _, _, _, _ := lifecycleCorpus()
	oracle, _ := engineRoot(t, modeSeq, 0, k1, u1)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("parallel", func(t *testing.T) {
		t.Parallel()
		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		tr := newParTrie(t, ms, 4)
		defer tr.Release()
		ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
		defer ut.Close()

		touchBatch(t, ms, ut, k1, u1)
		_, err := tr.Process(canceled, ut, "", nil, WarmupConfig{})
		require.Error(t, err)
		require.Equal(t, uint64(len(k1)), ut.Size(), "error path must keep the collection for the retry")

		got := processRoot(t, tr, ut)
		require.Zero(t, ut.Size())
		require.Equal(t, oracle, got)
	})
}

type branchWriteCounter struct {
	PatriciaContext
	wrote map[string][]byte
	on    bool
}

func (c *branchWriteCounter) PutBranch(prefix, data, prev []byte) error {
	if c.on {
		c.wrote[string(prefix)] = bytes.Clone(data)
	}
	return c.PatriciaContext.PutBranch(prefix, data, prev)
}

func collapseCorpus() (k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update) {
	rnd := rand.New(rand.NewSource(20260904))

	var spread []string
	ub1 := NewUpdateBuilder()
	for nib := range 15 {
		for i := range 8 {
			a := addrHex(findAddressForNibble(nib, 70000+nib*1000+i))
			spread = append(spread, a)
			ub1.Balance(a, rnd.Uint64()+1)
			addRandomSlot(ub1, rnd, a)
		}
	}
	for i := range 900 {
		a := addrHex(findAddressForNibble(0xf, 40000+i))
		ub1.Balance(a, rnd.Uint64()+1)
		addRandomSlot(ub1, rnd, a)
	}
	k1, u1 = ub1.Build()

	ub2 := NewUpdateBuilder()
	for i, a := range spread {
		if i%8 == 0 {
			ub2.Balance(a, rnd.Uint64()+1)
			continue
		}
		ub2.Delete(a)
	}
	for i := range 900 {
		ub2.Balance(addrHex(findAddressForNibble(0xf, 40000+i)), rnd.Uint64()+1)
	}
	k2, u2 = ub2.Build()
	return k1, u1, k2, u2
}

func TestModeParallel_CancelDuringWalkKeepsCollection(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())

	tr := NewParallelPatriciaHashed(func(context.Context) (PatriciaContext, func()) {
		return &cancelOnAccountContext{cancel: cancel}, func() {}
	}, length.Addr, DefaultTrieConfig())
	defer tr.Release()

	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()
	ut.TouchPlainKey(string(findAddressForNibble(1, 2201)), nil, nil)

	_, err := tr.Process(ctx, ut, "", nil, WarmupConfig{})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, uint64(1), ut.Size())
}

type failingTrieContext struct {
	*MockState
	fail *atomic.Bool
}

func (c *failingTrieContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	if c.fail.Load() {
		return nil, 0, errors.New("injected branch read failure")
	}
	return c.MockState.Branch(prefix)
}

func (c *failingTrieContext) Account(plainKey []byte) (*Update, error) {
	if c.fail.Load() {
		return nil, errors.New("injected account read failure")
	}
	return c.MockState.Account(plainKey)
}

func (c *failingTrieContext) Storage(plainKey []byte) (*Update, error) {
	if c.fail.Load() {
		return nil, errors.New("injected storage read failure")
	}
	return c.MockState.Storage(plainKey)
}

func failAfterNContexts(ms *MockState, n int) (TrieContextFactory, *atomic.Bool) {
	var handed atomic.Int64
	var fail atomic.Bool
	return func(context.Context) (PatriciaContext, func()) {
		if handed.Add(1) > int64(n) {
			fail.Store(true)
		}
		return &failingTrieContext{MockState: ms, fail: &fail}, func() {}
	}, &fail
}

func TestModeParallel_MidWalkErrorRestoresBaseTrie(t *testing.T) {
	t.Parallel()
	rnd := rand.New(rand.NewSource(20260904))
	ub := NewUpdateBuilder()
	for i := range 5 {
		ub.Balance(addrHex(findAddressForNibble(i+1, 9100+i)), uint64(7000+i))
	}
	for i := range 900 {
		a := addrHex(findAddressForNibble(0xf, 40000+i))
		ub.Balance(a, rnd.Uint64()+1)
		addRandomSlot(ub, rnd, a)
		addRandomSlot(ub, rnd, a)
	}
	keys, upds := ub.Build()
	oracle, seqMs := engineRoot(t, modeSeq, 0, keys, upds)

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	require.NoError(t, ms.applyPlainUpdates(keys, upds))

	factory, fail := failAfterNContexts(ms, 2)
	tr := NewParallelPatriciaHashed(factory, length.Addr, DefaultTrieConfig())
	defer tr.Release()
	tr.SetNumWorkers(4)
	tr.ResetContext(ms)

	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()
	for _, k := range keys {
		ut.TouchPlainKey(string(k), nil, nil)
	}
	base := tr.RootTrie()
	before := base.root
	_, err := tr.Process(context.Background(), ut, "", nil, WarmupConfig{})
	require.Error(t, err, "the injected read failure must abort the round")
	require.Equal(t, uint64(len(keys)), ut.Size(), "error path must keep the collection for the retry")

	require.Zero(t, base.activeRows, "an aborted round must leave no open rows on the persistent base")
	require.Zero(t, base.currentKeyLen, "an aborted round must leave the base unpositioned")
	require.Empty(t, base.branchEncoder.deferred, "an aborted round must not leave branch updates queued on the base")
	require.Equal(t, before, base.root, "an aborted round must leave the base root as it found it")

	fail.Store(false)
	tr.SetTrieContextFactory(mockTrieCtxFactory(ms))
	tr.ResetContext(ms)
	got := processRoot(t, tr, ut)
	require.Equal(t, oracle, got, "a retry after a mid-walk failure must reproduce the sequential root")
	requireBranchParity(t, seqMs, ms)
}

func TestModeParallel_MidWalkErrorLeavesDomainUntouched(t *testing.T) {
	t.Parallel()
	k1, u1, k2, u2 := collapseCorpus()

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	tr := newParTrie(t, ms, 4)
	defer tr.Release()

	ut1 := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut1.Close()
	require.NoError(t, ms.applyPlainUpdates(k1, u1))
	for _, k := range k1 {
		ut1.TouchPlainKey(string(k), nil, nil)
	}
	processRoot(t, tr, ut1)

	baseCtx := &branchWriteCounter{PatriciaContext: ms, wrote: map[string][]byte{}}
	tr.ResetContext(baseCtx)
	factory, _ := failAfterNContexts(ms, 2)
	tr.SetTrieContextFactory(factory)

	ut2 := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut2.Close()
	require.NoError(t, ms.applyPlainUpdates(k2, u2))
	for _, k := range k2 {
		ut2.TouchPlainKey(string(k), nil, nil)
	}

	stored := map[string][]byte{}
	for k, v := range ms.cm {
		stored[k] = bytes.Clone(v)
	}

	base := tr.RootTrie()
	before := base.root
	baseCtx.on = true
	_, err := tr.Process(context.Background(), ut2, "", nil, WarmupConfig{})
	baseCtx.on = false
	require.Error(t, err, "the injected read failure must abort the round")

	require.Zero(t, base.activeRows, "an aborted round must leave no open rows on the persistent base")
	require.Zero(t, base.currentKeyLen, "an aborted round must leave the base unpositioned")
	require.Empty(t, base.branchEncoder.deferred, "an aborted round must not leave branch updates queued on the base")
	require.Equal(t, before, base.root, "an aborted round must leave the base root as it found it")

	require.Empty(t, baseCtx.wrote,
		"a round that collapses a branch must defer the deletion, so an abort writes no branch record: wrote %d", len(baseCtx.wrote))
	for prefix, prev := range stored {
		now, ok := ms.cm[prefix]
		require.True(t, ok, "an aborted round must not delete a stored branch record at %x", prefix)
		require.True(t, bytes.Equal(prev, now), "an aborted round must not change the stored branch record at %x", prefix)
	}
}

type putFailingTrieContext struct {
	PatriciaContext
}

func (c *putFailingTrieContext) PutBranch(prefix, data, prev []byte) error {
	return errors.New("injected branch write failure")
}

func TestModeParallel_DeferredApplyErrorKeepsPreRoundRoot(t *testing.T) {
	t.Parallel()
	k1, u1, k2, u2, _, _ := lifecycleCorpus()

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	tr := newParTrie(t, ms, 4)
	defer tr.Release()

	ut1 := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut1.Close()
	touchBatch(t, ms, ut1, k1, u1)
	before := processRoot(t, tr, ut1)

	tr.SetTrieContextFactory(func(context.Context) (PatriciaContext, func()) {
		return &putFailingTrieContext{PatriciaContext: ms}, func() {}
	})

	ut2 := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut2.Close()
	touchBatch(t, ms, ut2, k2, u2)
	_, err := tr.Process(context.Background(), ut2, "", nil, WarmupConfig{})
	require.Error(t, err, "the injected branch write failure must abort the deferred apply")
	require.Equal(t, uint64(len(k2)), ut2.Size(), "error path must keep the collection")

	got, rerr := tr.RootHash()
	require.NoError(t, rerr)
	require.Equal(t, before, got, "a failed deferred apply must not surface the staged root")
}

func TestModeParallel_DeferredModeWritesNoBranchBeforeRoot(t *testing.T) {
	t.Parallel()
	k1, u1, k2, u2 := collapseCorpus()

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	tr := newParTrie(t, ms, 4)
	defer tr.Release()

	ut1 := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut1.Close()
	require.NoError(t, ms.applyPlainUpdates(k1, u1))
	for _, k := range k1 {
		ut1.TouchPlainKey(string(k), nil, nil)
	}
	processRoot(t, tr, ut1)

	baseCtx := &branchWriteCounter{PatriciaContext: ms, wrote: map[string][]byte{}}
	tr.ResetContext(baseCtx)
	tr.SetLeaveDeferredForCaller(true)

	ut2 := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut2.Close()
	require.NoError(t, ms.applyPlainUpdates(k2, u2))
	for _, k := range k2 {
		ut2.TouchPlainKey(string(k), nil, nil)
	}

	baseCtx.on = true
	_, err := tr.Process(context.Background(), ut2, "", nil, WarmupConfig{})
	baseCtx.on = false
	require.NoError(t, err)

	require.True(t, tr.HasPendingDeferredUpdates(), "caller-deferred mode must leave the round's branch updates pending")
	require.Empty(t, baseCtx.wrote,
		"caller-deferred mode must write no branch record before the root is returned; wrote %d", len(baseCtx.wrote))
}
