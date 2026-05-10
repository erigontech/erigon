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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/state"
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

// TestHandleMessage_TxResultPinsAsOfReaderTxNum is the regression test for
// the snapshot-loaded lazy-load crash: cc.asOfReader is constructed with
// txNum=0, and only computeAndPublish / computeAndCheck (which run on
// blockResult) overwrite that field. But cc.state.ApplyWrites runs on
// every txResult and triggers ensureAccount / ensureStorage lazy-loads
// through the same reader. txResults arrive before the first blockResult,
// so the very first lazy-load fires with asOfReader.txNum=0. On a synced-
// from-genesis chain that's in-window so seekInFiles handles it
// gracefully; on a snapshot-loaded chain whose visible window starts
// well past genesis (e.g. perf-devnet-3 starting at txNum~2.9B) it fails
// hard with `seekInFiles(invIndex=storage,txNum=0) but data before
// txNum=N not available` and FCU fails on every block.
//
// The fix in handleMessage's *txResult branch pins asOfReader.txNum to
// the current tx's txNum BEFORE ApplyWrites runs. computeAndPublish /
// computeAndCheck overwrite the field back to lastTxNum+1 right before
// ComputeCommitment, so this per-tx setting only affects the lazy-load
// path and never leaks into the trie fold path.
//
// We test the post-condition (asOfReader.txNum == r.txNum after each
// txResult) — the simplest invariant that catches the absence of the
// fix. The ordering relative to ApplyWrites (set BEFORE) is enforced by
// inspection: the test cannot observe a successful lazy-load without
// pulling in real SharedDomains plumbing, but a missing assignment
// would leave txNum at its prior value and fail this test directly.
func TestHandleMessage_TxResultPinsAsOfReaderTxNum(t *testing.T) {
	asOfReader := &asOfStateReader{txNum: 0}
	cs := newCalcState(asOfReader, nil, "test")
	// Disable lazy-load — without a real SharedDomains the asOfReader's
	// Read would NPE on r.sd.GetAsOf. The fix's contract is independent
	// of whether the read succeeds: txNum must be pinned before the
	// load is even attempted.
	cs.domainReader = nil
	cc := &commitmentCalculator{
		asOfReader: asOfReader,
		state:      cs,
	}

	// A txResult must carry at least one write to enter the fix's
	// `if len(r.writes) > 0` branch — empty writes have no lazy-loads
	// to seed and therefore intentionally don't update txNum.
	someWrites := state.VersionedWrites{
		// Path/value content irrelevant — domainReader is nil so the
		// lazy-load path skips the real read. We only need len(writes)>0.
		&state.VersionedWrite{},
	}

	// First txResult: txNum jumps from 0 to 12345.
	cc.handleMessage(context.Background(), &txResult{
		txNum:  12345,
		writes: someWrites,
	})
	require.Equal(t, uint64(12345), cc.asOfReader.txNum,
		"asOfReader.txNum must be pinned to the current tx's txNum before "+
			"ApplyWrites; otherwise lazy-loads triggered by the writes use "+
			"the stale value (initially 0) and fail on snapshot-loaded chains.")

	// Subsequent txResult: txNum advances to 12346. Verifies the field
	// is updated on every txResult, not just the first one.
	cc.handleMessage(context.Background(), &txResult{
		txNum:  12346,
		writes: someWrites,
	})
	require.Equal(t, uint64(12346), cc.asOfReader.txNum,
		"asOfReader.txNum must advance on every txResult (each tx's "+
			"first-touch lazy-loads need the matching pre-tx baseline).")

	// Empty-writes txResult: the fix's len(writes)>0 guard skips the
	// pin. asOfReader stays where the prior tx left it. This is the
	// intended behavior — an empty writeset has no lazy-loads to seed,
	// so updating txNum would be wasted work and could mask real
	// regressions in producers that drop writes.
	cc.handleMessage(context.Background(), &txResult{
		txNum:  12347,
		writes: nil,
	})
	require.Equal(t, uint64(12346), cc.asOfReader.txNum,
		"empty writes → no lazy-load → don't bump txNum; the prior pin stands.")
}
