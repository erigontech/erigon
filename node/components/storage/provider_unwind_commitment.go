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

package storage

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// commitmentRecomputeResult is the in-memory output of mode B's
// compute phase: the recomputed root + encoded trie state at
// lastTxNum, plus the branch collector the trie's Process emitted.
// Drained by ensureCommitmentAtBlockApply after the boundary-step
// shadow wipe.
type commitmentRecomputeResult struct {
	lastTxNum        uint64
	encodedTrieState []byte
	branches         *etl.Collector // caller must Close
}

// Close drains/releases the underlying collector. Safe to call when
// branches is nil (e.g. on the error path of Compute).
func (r *commitmentRecomputeResult) Close() {
	if r == nil || r.branches == nil {
		return
	}
	r.branches.Close()
	r.branches = nil
}

// ensureCommitmentAtBlock is mode-B sub-op #3 — anchor the commitment
// trie at toBlock.
//
// Works for both aligned and non-aligned cuts. The recompute primitive
// (commitmentdb.RecomputeAtTxNumWithoutSD) takes any toTxNum. The
// maxStep parameter bounds the file-side baseline lookup: a file is a
// valid baseline candidate iff its endTxNum ≤ maxStep*stepSize (so its
// commitment record's internal txnum ≤ lastTxNum).
//
// Algorithm (SD-free, single-tx):
//
//  1. maxStep = (lastTxNum+1) / stepSize. For aligned cuts (lastTxNum =
//     K*stepSize - 1), this is K, so the file ending at endTxNum =
//     K*stepSize stays a candidate — its commitment is exactly at
//     lastTxNum and history-range to fold is empty. For non-aligned cuts
//     (lastTxNum mid-step), this is K-1, so the file containing
//     lastTxNum (endTxNum = K*stepSize, commitment past lastTxNum) is
//     excluded; baseline comes from an earlier file ending at endTxNum
//     ≤ (K-1)*stepSize, and history covers the gap up to lastTxNum.
//
//  2. commitmentdb.RecomputeAtTxNumWithoutSD reads a file-side
//     baseline via GetLatestFromFilesUpToStep(maxStep), restores
//     the patricia trie state, replays accounts/storage/code touches
//     in (baselineTxNum, lastTxNum+1] via HistoryKeyTxNumRange (those
//     domains have history; the range query is well-defined), calls
//     trie.Process to fold them in, and returns the new root + encoded
//     trie state. No SharedDomains is opened — the behind-commitment
//     guard in NewSharedDomains is structurally incompatible with mode
//     B's mid-tx wiped-shadow state.
//
//  3. Validate the recomputed root against the block header's
//     stateRoot. A mismatch means the snapshot/history data couldn't
//     reproduce consensus — refuse loudly.
//
//  4. Write the new commitment entry into the writable shadow at
//     lastTxNum via TemporalMemBatch.DomainPut → Flush. After this
//     write the writable shadow holds the canonical commitment at
//     toBlock, surviving the next forward execution's NewSharedDomains
//     seek.
//
// ensureCommitmentAtBlockCompute runs the SD-less recompute primitive
// to obtain the trie state at toBlock's lastTxNum. It validates the
// root against the block header's stateRoot and returns the captured
// branches + encoded trie state for a subsequent Apply phase to write
// to the writable shadow AFTER the boundary-step wipe.
//
// Splitting compute from apply lets WipeWritableShadowPast wipe
// commitment branches at step=stepContaining without losing the
// recompute's output. The apply phase then writes both the captured
// branches and KeyCommitmentState at txnum=lastTxNum.
//
// Caller MUST call result.Close() (or pass it to Apply which closes
// internally) to release the etl collector.
func (p *Provider) ensureCommitmentAtBlockCompute(ctx context.Context, tx kv.TemporalRwTx, toBlock uint64) (*commitmentRecomputeResult, error) {
	if p.BlockReader == nil {
		return nil, fmt.Errorf("ensureCommitmentAtBlockCompute: nil BlockReader")
	}
	if p.Aggregator == nil {
		return nil, fmt.Errorf("ensureCommitmentAtBlockCompute: nil Aggregator")
	}

	header, err := p.BlockReader.HeaderByNumber(ctx, tx, toBlock)
	if err != nil {
		return nil, fmt.Errorf("HeaderByNumber(%d): %w", toBlock, err)
	}
	if header == nil {
		return nil, fmt.Errorf("ensureCommitmentAtBlockCompute: no header for block %d", toBlock)
	}

	lastTxNum, err := rawdbv3.TxNums.Max(ctx, tx, toBlock)
	if err != nil {
		return nil, fmt.Errorf("TxNums.Max(%d): %w", toBlock, err)
	}
	stepSize := p.Aggregator.StepSize()
	if stepSize == 0 {
		return nil, fmt.Errorf("aggregator StepSize() == 0")
	}
	stepBoundary := kv.Step((lastTxNum + 1) / stepSize)

	tmpDir := p.snapDir
	root, encodedTrieState, baselineTxNum, branches, err := commitmentdb.RecomputeAtTxNumWithoutSD(ctx, tx, tmpDir, lastTxNum, stepBoundary, stepSize)
	if err != nil {
		// ErrHistoryGap from the primitive doesn't carry ToBlock —
		// patch it in (caller-side context) so setHeadModeB's retry
		// loop can run a focused historic re-exec.
		if gap, ok := commitmentdb.IsHistoryGap(err); ok {
			gap.ToBlock = toBlock
			return nil, gap
		}
		return nil, fmt.Errorf("RecomputeAtTxNumWithoutSD(toBlock=%d, lastTxNum=%d, stepBoundary=%d): %w", toBlock, lastTxNum, stepBoundary, err)
	}
	if common.Hash(root) != header.Root {
		if branches != nil {
			branches.Close()
		}
		return nil, fmt.Errorf("recomputed root %x does not match header stateRoot %x at block %d (baselineTxNum=%d)", root, header.Root, toBlock, baselineTxNum)
	}
	return &commitmentRecomputeResult{
		lastTxNum:        lastTxNum,
		encodedTrieState: encodedTrieState,
		branches:         branches,
	}, nil
}

// ensureCommitmentAtBlockApply drains the branch collector from the
// Compute phase + writes the new commitment entries into the writable
// shadow at lastTxNum. Must run AFTER the boundary-step wipe in
// WipeWritableShadowPast so the writes go in clean (no orphan dups).
//
// Uses TemporalMemBatch (a lightweight memctx) — SD's constructor
// seeks commitment and trips the behind-commitment guard against the
// over-step file. The memctx writes directly through the
// domain-writer chain.
//
// Always closes result.branches (success or error).
func (p *Provider) ensureCommitmentAtBlockApply(ctx context.Context, tx kv.TemporalRwTx, toBlock uint64, result *commitmentRecomputeResult) error {
	if result == nil {
		return fmt.Errorf("ensureCommitmentAtBlockApply: nil result")
	}
	defer result.Close()

	metrics := &changeset.DomainMetrics{Domains: map[kv.Domain]*changeset.DomainIOMetrics{}}
	mem := tx.Debug().NewMemBatch(metrics)
	defer mem.Close()

	// Drain the branch collector into the writable shadow. Each
	// branch is written at txnum=lastTxNum (step=stepContaining).
	// The preceding boundary-step wipe removed any stale forward-
	// exec branches at the same step, so these go in clean.
	branchCount := 0
	if result.branches != nil {
		if err := result.branches.Load(nil, "", func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
			branchCount++
			return mem.DomainPut(kv.CommitmentDomain, string(k), v, result.lastTxNum, nil)
		}, etl.TransformArgs{}); err != nil {
			return fmt.Errorf("drain branches collector: %w", err)
		}
	}

	// Write the new commitment-state record (the full encoded trie
	// state) at lastTxNum. Future ensureCommitmentAtBlock /
	// SD.SeekCommitment uses this to restore the trie.
	cs := commitmentdb.NewCommitmentState(result.lastTxNum, toBlock, result.encodedTrieState)
	encoded, err := cs.Encode()
	if err != nil {
		return fmt.Errorf("encode commitment state: %w", err)
	}
	if err := mem.DomainPut(kv.CommitmentDomain, string(commitmentdb.KeyCommitmentState), encoded, result.lastTxNum, nil); err != nil {
		return fmt.Errorf("memctx DomainPut(CommitmentDomain, KeyCommitmentState): %w", err)
	}
	if err := mem.Flush(ctx, tx); err != nil {
		return fmt.Errorf("memctx Flush: %w", err)
	}
	if p.logger != nil {
		p.logger.Info("[storage] Provider.Unwind: commitment-anchor applied", "toBlock", toBlock, "lastTxNum", result.lastTxNum, "branches", branchCount)
	}
	return nil
}
