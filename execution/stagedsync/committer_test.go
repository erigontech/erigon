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

package stagedsync

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types"
)

// TestShouldComputeOnRequest_GenesisFirstBatch is the regression test for
// the batch-mode genesis-commitment bug:
//
// In BatchCommitments mode the calculator only computes on
// commitComputeRequest. The very first batch of an exec3 cycle that runs
// only the genesis block produces lastBlockResult.BlockNum=0. The dedup
// check used to be `lastBlockResult.BlockNum > lastComputedBlock`, which
// for the (very common) initial state lastComputedBlock=0 evaluates to
// `0 > 0 == false`. So the calculator silently skipped computing and
// publishing — leaving the genesis commitment unwritten to sd. The next
// exec3 cycle's SeekCommitment then fell back to stage progress, treated
// blockNum=0 as "no progress" (mirror of bug 2), re-executed genesis,
// and produced a wrong trie root for block 1.
//
// The fix introduces hasComputed so the predicate can distinguish "never
// computed" from "computed block 0".
func TestShouldComputeOnRequest_GenesisFirstBatch(t *testing.T) {
	cc := &commitmentCalculator{
		lastBlockResult:   &blockResult{BlockNum: 0},
		lastComputedBlock: 0,
		hasComputed:       false,
	}
	assert.True(t, cc.shouldComputeOnRequest(),
		"first batch covering only genesis (block 0) MUST compute, "+
			"otherwise the genesis commitment is never written to sd "+
			"and the next exec3 cycle's SeekCommitment will fall back "+
			"to stage progress and re-execute genesis")
}

// TestShouldComputeOnRequest_AlreadyComputedSameBlock verifies the dedup:
// after computing block 0, a subsequent commitComputeRequest with no
// new block boundary should NOT recompute.
func TestShouldComputeOnRequest_AlreadyComputedSameBlock(t *testing.T) {
	cc := &commitmentCalculator{
		lastBlockResult:   &blockResult{BlockNum: 0},
		lastComputedBlock: 0,
		hasComputed:       true,
	}
	assert.False(t, cc.shouldComputeOnRequest(),
		"no new block boundary since last compute — skip and publish empty")
}

// TestShouldComputeOnRequest_AdvancedBlock verifies the normal case:
// a new block arrived since the last compute, recompute.
func TestShouldComputeOnRequest_AdvancedBlock(t *testing.T) {
	cc := &commitmentCalculator{
		lastBlockResult:   &blockResult{BlockNum: 5},
		lastComputedBlock: 3,
		hasComputed:       true,
	}
	assert.True(t, cc.shouldComputeOnRequest(),
		"new block boundary advanced past the last compute — recompute")
}

// TestShouldComputeOnRequest_NoBlockResult verifies that with no
// blockResult yet (the calculator was woken before any block landed),
// we publish the empty result instead of computing against nothing.
func TestShouldComputeOnRequest_NoBlockResult(t *testing.T) {
	cc := &commitmentCalculator{
		lastBlockResult:   nil,
		lastComputedBlock: 0,
		hasComputed:       false,
	}
	assert.False(t, cc.shouldComputeOnRequest(),
		"no blockResult to compute against — publish empty so drainBeforeExit unblocks")
}

// TestShouldComputeOnRequest_BlockZeroAfterAdvance verifies that even if
// some downstream code re-sends a stale block-0 result, the dedup still
// fires because we already advanced past it.
func TestShouldComputeOnRequest_BlockZeroAfterAdvance(t *testing.T) {
	cc := &commitmentCalculator{
		lastBlockResult:   &blockResult{BlockNum: 0},
		lastComputedBlock: 5,
		hasComputed:       true,
	}
	assert.False(t, cc.shouldComputeOnRequest(),
		"stale block 0 result while we've already computed block 5 — skip")
}

func TestAsOfStateReaderDoesNotOwnBranchesByDefault(t *testing.T) {
	require.False(t, (&asOfStateReader{}).OwnsBranches())
}

// TestHandleBlockRequest_EmptyBALFallsToIncremental pins the empty-BAL gate.
// A genuine empty BAL (0xc0) decodes to a non-nil empty slice, so a nil check
// alone selects BAL-driven mode and computes zero changes → parent root →
// spurious wrong-trie-root. Mode selection must gate on len(bal) > 0.
func TestHandleBlockRequest_EmptyBALFallsToIncremental(t *testing.T) {
	defer func(prev bool) { dbg.BALDrivenCommitment = prev }(dbg.BALDrivenCommitment)
	defer func(prev bool) { dbg.IgnoreBAL = prev }(dbg.IgnoreBAL)
	dbg.BALDrivenCommitment = true
	dbg.IgnoreBAL = false

	cc := &commitmentCalculator{
		pending:       map[uint64]*pendingBlock{},
		computedAhead: map[uint64]bool{},
		balRoots:      map[uint64][]byte{},
		// Not the batch's first block and no blockResult seen yet, so the
		// compute-ahead gate is shut — maybeComputeAhead returns before touching
		// the (nil) doms/updates, isolating the mode-selection under test.
		hasFirstBlock:      true,
		firstBlockNum:      100,
		hasSeenBlockResult: false,
	}

	emptyBAL := make(types.BlockAccessList, 0)
	require.NotNil(t, emptyBAL, "empty BAL must be non-nil to exercise the gate")

	cc.handleBlockRequest(context.Background(), &blockRequest{blockNum: 5, bal: emptyBAL})

	pb, ok := cc.pending[5]
	require.True(t, ok, "block request must be recorded")
	assert.Equal(t, calcModeIncremental, pb.mode,
		"an empty (non-nil) BAL declares no changes and must fall to "+
			"incremental mode; computing it ahead would compute the parent root and "+
			"fail an otherwise-valid block with ErrWrongTrieRoot")
}

// One block without a BAL ends compute-ahead for the whole batch: the chain stays
// anchored at the last block that advanced the commitment domain, so every later
// block reads a stale baseline and stays on the incremental path.
func TestContiguityGuard_ChainNeverRecovers(t *testing.T) {
	defer func(prev bool) { dbg.BALDrivenCommitment = prev }(dbg.BALDrivenCommitment)
	defer func(prev bool) { dbg.IgnoreBAL = prev }(dbg.IgnoreBAL)
	dbg.BALDrivenCommitment = true
	dbg.IgnoreBAL = false

	ctx := context.Background()
	cc := &commitmentCalculator{
		signalCtx:     ctx,
		pending:       map[uint64]*pendingBlock{},
		computedAhead: map[uint64]bool{},
		balRoots:      map[uint64][]byte{},
		hasFirstBlock: true,
		firstBlockNum: 5,
		perBlockFrom:  1 << 62,
		// Block 5 computed ahead, block 6 had no BAL and never advanced the domain.
		hasComputedAhead:       true,
		lastComputedAheadBlock: 5,
		hasSeenBlockResult:     true,
	}

	for n := uint64(7); n <= 12; n++ {
		cc.lastBlockResultSeen = n - 1 // gate open: only the contiguity guard may reject
		cc.pending[n] = &pendingBlock{
			req:  &blockRequest{blockNum: n, bal: make(types.BlockAccessList, 1)},
			mode: calcModeBALDriven,
		}
		cc.maybeComputeAhead(ctx, n)
		assert.False(t, cc.computedAhead[n], "block %d must not compute ahead across the gap at block 6", n)
	}
	assert.Equal(t, uint64(5), cc.lastComputedAheadBlock, "the chain must stay anchored at block 5")
}

// TestComputeAheadCap_StopsComputeAhead pins the orphan guard: once the shared
// executor context carries a stopCause, the calculator must not compute any
// block ahead past the coalesce block M — otherwise commitment would advance
// past the state exec stops at. The signal is read from the context cause, not
// a shared flag.
func TestComputeAheadCap_StopsComputeAhead(t *testing.T) {
	defer func(prev bool) { dbg.BALDrivenCommitment = prev }(dbg.BALDrivenCommitment)
	dbg.BALDrivenCommitment = true

	signalCtx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	cc := &commitmentCalculator{
		signalCtx: signalCtx,
		pending: map[uint64]*pendingBlock{
			5: {req: &blockRequest{blockNum: 5, bal: make(types.BlockAccessList, 1)}, mode: calcModeBALDriven},
		},
		computedAhead: map[uint64]bool{},
		balRoots:      map[uint64][]byte{},
		hasFirstBlock: true,
		firstBlockNum: 5, // gate open for block 5 without a prior blockResult
	}

	// Coalesce block M=4: block 5 is past M and must not compute ahead.
	cancel(&stopCause{block: 4, kind: stopMoreWork})
	cc.maybeComputeAhead(context.Background(), 5) // must return before computeBlockFromBAL

	assert.False(t, cc.computedAhead[5], "a compute-ahead past the coalesce block M must not run")
}

func TestComputeAheadCap_StopsOperationalWindDown(t *testing.T) {
	defer func(prev bool) { dbg.BALDrivenCommitment = prev }(dbg.BALDrivenCommitment)
	dbg.BALDrivenCommitment = true

	signalCtx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	cause := errors.New("commitment lazy-load failed")
	pe := &parallelExecutor{cancelExecLoop: cancel}
	pe.cancelOperational(4, cause)

	sc, ok := stopCauseOf(signalCtx)
	require.True(t, ok, "operational cancellation must publish its block boundary")
	require.Equal(t, uint64(4), sc.block)
	require.Equal(t, stopOperational, sc.kind)
	require.Same(t, cause, sc.err)

	cc := &commitmentCalculator{
		signalCtx: signalCtx,
		pending: map[uint64]*pendingBlock{
			5: {req: &blockRequest{blockNum: 5, bal: make(types.BlockAccessList, 1)}, mode: calcModeBALDriven},
		},
		computedAhead: map[uint64]bool{},
		balRoots:      map[uint64][]byte{},
		hasFirstBlock: true,
		firstBlockNum: 5,
		perBlockFrom:  100,
	}

	cc.maybeComputeAhead(context.Background(), 5)
	assert.False(t, cc.computedAhead[5], "failure wind-down must not compute ahead past its block")
}

func TestHandOffUpdatesRotatesTwoBuffers(t *testing.T) {
	first := commitment.NewUpdates(commitment.ModeParallel, t.TempDir(), commitment.KeyToHexNibbleHash)
	second := commitment.NewUpdates(commitment.ModeParallel, t.TempDir(), commitment.KeyToHexNibbleHash)
	cc := &commitmentCalculator{updates: first, spare: second}

	touch := func(u *commitment.Updates, addr byte) {
		u.TouchPlainKey(string(bytes.Repeat([]byte{addr}, 20)), nil, u.TouchAccount)
	}

	touch(cc.updates, 1)
	handed := cc.handOffUpdates()
	require.Same(t, first, handed, "the filled buffer must be the one handed off")
	require.NotZero(t, handed.Size(), "the handed-off buffer must keep the updates it will be folded from")
	require.Same(t, second, cc.updates, "the spare must rotate in")
	require.Zero(t, cc.updates.Size(), "the rotated-in buffer must be empty")

	touch(cc.updates, 2)
	handed = cc.handOffUpdates()
	require.Same(t, second, handed)
	require.NotZero(t, handed.Size(), "the handed-off buffer must keep the updates it will be folded from")
	require.Same(t, first, cc.updates, "rotation must reuse the first buffer, not allocate")
	require.Zero(t, cc.updates.Size(), "the reused buffer must be reset before refilling")
}
