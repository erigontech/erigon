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
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state/kvmetrics"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// branchPair is one (K, V) entry from the recompute's PutBranch
// collector — a commitment branch valid at lastTxNum. Slice is
// sorted by K so regen can merge-walk it against the baseline .kv.
type branchPair struct {
	K []byte
	V []byte
}

// commitmentRecomputeResult is the in-memory output of mode B's
// compute phase: the recomputed root + encoded trie state at
// lastTxNum, plus the branches trie.Process emitted (sorted by K).
// The regen phase merge-walks these against the baseline commitment
// .kv to produce a self-coherent at-lastTxNum boundary file — no
// separate MDBX-write of branches is performed (the wipe leaves the
// boundary step's commitment MDBX empty by design).
type commitmentRecomputeResult struct {
	lastTxNum        uint64
	encodedTrieState []byte
	branches         []branchPair
}

// Close is a no-op now that branches live in an in-memory slice;
// kept for API stability so existing defer sites don't need to be
// touched.
func (r *commitmentRecomputeResult) Close() {}

// drainCollectorSorted materialises an etl.Collector into a sorted
// []branchPair. The collector's own Load emits in key-sorted order,
// so append + assert-sorted is enough; we sort defensively in case a
// future Collector implementation changes.
func drainCollectorSorted(c *etl.Collector) ([]branchPair, error) {
	if c == nil {
		return nil, nil
	}
	defer c.Close()
	out := make([]branchPair, 0, 1024)
	if err := c.Load(nil, "", func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		out = append(out, branchPair{
			K: append([]byte(nil), k...),
			V: append([]byte(nil), v...),
		})
		return nil
	}, etl.TransformArgs{}); err != nil {
		return nil, fmt.Errorf("drain branches collector: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].K, out[j].K) < 0 })
	return out, nil
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
		if gap, ok := commitmentdb.IsHistoryGap(err); ok {
			gap.ToBlock = toBlock
			return nil, gap
		}
		return nil, fmt.Errorf("RecomputeAtTxNumWithoutSD(toBlock=%d, lastTxNum=%d, stepBoundary=%d): %w", toBlock, lastTxNum, stepBoundary, err)
	}
	// Materialise the collector into a sorted slice up front so both
	// consumers (regen file merge-walk + any diagnostic loggers) can
	// iterate it. The collector's own Load emits sorted, so this is a
	// straight append; drainCollectorSorted sorts defensively.
	branchPairs, drainErr := drainCollectorSorted(branches)
	if drainErr != nil {
		return nil, fmt.Errorf("drain collector after recompute (toBlock=%d): %w", toBlock, drainErr)
	}
	if common.Hash(root) != header.Root {
		return nil, fmt.Errorf("recomputed root %x does not match header stateRoot %x at block %d (baselineTxNum=%d)", root, header.Root, toBlock, baselineTxNum)
	}
	return &commitmentRecomputeResult{
		lastTxNum:        lastTxNum,
		encodedTrieState: encodedTrieState,
		branches:         branchPairs,
	}, nil
}

// ensureCommitmentAtBlockApply writes the recomputed commitment
// branches + KeyCommitmentState anchor into MDBX at step=stepContaining
// (lastTxNum), txnum=lastTxNum. Must run AFTER WipeWritableShadowPast's
// whole-step commitment clear so the writes go in without orphan dups.
//
// The parallel merge-walk regen (regenerateBoundaryStepFiles for the
// commitment domain) also produces a self-coherent at-lastTxNum .kv
// when a straddler file exists — files with endStep past stepBoundary
// get truncated to endStep=stepBoundary, with baseline + branches
// merged inside. That covers the "reader picks a wider straddler file
// over MDBX" case (the 2026-07-04 iter 3 mode_b @30k wrong-trie-root).
//
// MDBX carries the writes for the case where NO straddler file exists
// (all commitment files end at endStep ≤ maxStep). Without a straddler,
// regenerateBoundaryStepFiles doesn't fire for commitment — so if MDBX
// didn't hold the anchor, SeekCommitment would fall back to the baseline
// file's KeyCommitmentState (encoding an earlier txnum) and forward
// exec would try to replay txns already applied in-state (nonce-too-low
// or wrong-state). Live-caught 2026-07-04 iter 1 mode_a depth 50.
func (p *Provider) ensureCommitmentAtBlockApply(ctx context.Context, tx kv.TemporalRwTx, toBlock uint64, result *commitmentRecomputeResult) error {
	if result == nil {
		return fmt.Errorf("ensureCommitmentAtBlockApply: nil result")
	}

	metrics := &kvmetrics.DomainMetrics{Domains: map[kv.Domain]*kvmetrics.DomainIOMetrics{}}
	mem := tx.Debug().NewMemBatch(metrics)
	defer mem.Close()

	for _, bp := range result.branches {
		if err := mem.DomainPut(kv.CommitmentDomain, string(bp.K), bp.V, result.lastTxNum, nil); err != nil {
			return fmt.Errorf("memctx DomainPut(CommitmentDomain, branch): %w", err)
		}
	}

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
		p.logger.Info("[storage] Provider.Unwind: commitment-anchor applied", "toBlock", toBlock, "lastTxNum", result.lastTxNum, "branches", len(result.branches))
	}
	return nil
}
