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
	"testing"

	"github.com/stretchr/testify/assert"
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
