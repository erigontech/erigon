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
	"path/filepath"
	"sort"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// unwindSnapshotsPastBlock removes every snapshot file whose content
// extends past toBlock. Mode-B sub-op #1; runs under the
// post-quiescence precondition established by Provider.Unwind's caller.
//
// Works for both aligned cuts (lastTxNum at a step boundary; the file
// containing lastTxNum ends exactly at that boundary, so trimming
// leaves the file's coverage matching the new tip) and non-aligned
// cuts (lastTxNum mid-step; the file containing lastTxNum keeps its
// excess coverage past lastTxNum, which is masked at read time by the
// writable shadow's boundary-step diff-replay — see
// WipeWritableShadowPast).
//
// Files removed:
//
//   - block files where FromBlock > toBlock — i.e. files whose
//     entire content is strictly past the new tip. The straddle
//     file (FromBlock ≤ toBlock < ToBlock) STAYS: it still holds
//     the headers / bodies / etc. for blocks in [FromBlock,
//     toBlock], which the DB-reset's CanonicalHash truncation
//     gates from being visible past toBlock. Removing the straddle
//     file would leave the writable DB with no source for blocks
//     in [FromBlock, toBlock], so a subsequent BlockReader read of
//     those headers would return nil — exactly the failure mode
//     that issue #2 from the 2026-06-01 live cycle surfaced: a
//     second mode B targeting a block inside the removed straddle
//     file's range failed with "no header for block N".
//   - domain / history / idx files (all step-indexed) where
//     ToStep > stepBoundary, where stepBoundary == (lastTxNum/stepSize)+1
//     — that is, files extending strictly past the boundary step (the
//     step containing lastTxNum). The boundary step's own file stays
//     and contributes its in-range entries to reads.
//
// Caplin / meta / salt files are intentionally out of scope here:
// caplin lives on a slot axis (separate aligned-mode workstream);
// meta + salt are chain-wide rather than per-range, so "past
// toBlock" doesn't apply.
//
// Removal sequence per file:
//
//  1. Inventory.RemoveFile — held-view refcounts get pendingDeletes;
//     ChangeSet subscribers see one notification per file.
//  2. Filesystem delete (file + companion .torrent) — idempotent.
//  3. downloaderClient.Delete (relative names) — stops seeding.
//  4. republishChainToml — peers see the shorter manifest.
//
// Returns the sorted list of removed file names (relative to snapDir)
// for the caller to log or test against.
func (p *Provider) unwindSnapshotsPastBlock(ctx context.Context, tx kv.TemporalRwTx, toBlock uint64) ([]string, error) {
	if p.Inventory == nil {
		// No inventory to traverse; nothing to trim. Tools / tests
		// that construct a bare Provider without the snapshot-flow
		// component hit this branch.
		return nil, nil
	}

	stepBoundary, err := p.computeStepBoundaryForBlock(ctx, tx, toBlock)
	if err != nil {
		return nil, err
	}

	toRemove := p.collectFilesPastBlock(toBlock, stepBoundary)

	// Headers straddle rebuild: the file whose [FromBlock, ToBlock)
	// straddles toBlock has valid headers for blocks ≤ toBlock that
	// the writable DB doesn't carry (OtterSync exec doesn't write
	// kv.Headers). Removing it strands those blocks (live-rig issue
	// #2 from the 2026-06-01 cycle). Rebuild it to cover only
	// [FromBlock, chunkAlignedToBlock(toBlock)), drive the headers
	// IndexBuilderFunc to produce the new accessor, then stage the
	// old file for post-commit deletion via pendingTrim alongside
	// the strictly-past files.
	//
	// Scope: this commit handles HEADERS only. Bodies straddle
	// files keep their stale post-toBlock content but reads are
	// gated by CanonicalHash truncation (no hash-based reverse
	// lookup for bodies). Transactions straddle files leak via
	// eth_getTransactionByHash — known follow-up.
	//
	// Non-1000-aligned toBlock (toBlock+1 not a multiple of 1000)
	// would require seeding leftover [chunkAlignedToBlock(toBlock),
	// toBlock] into the writable DB; this commit returns an explicit
	// error rather than silently producing inconsistent state.
	straddle, err := p.headersStraddleFile(toBlock)
	if err != nil {
		return nil, fmt.Errorf("headersStraddleFile(%d): %w", toBlock, err)
	}
	var rebuildPaths []string
	if straddle != nil {
		newTo := chunkAlignedToBlock(toBlock)
		if newTo != toBlock+1 {
			return nil, fmt.Errorf("mode-B headers straddle rebuild: toBlock+1=%d not aligned to %d-block boundary; leftover-seed for non-aligned cuts is a known follow-up", toBlock+1, snaptype.Erigon2MinSegmentSize)
		}
		if newTo <= straddle.From {
			// The "straddle" actually starts past toBlock — treat as
			// strictly-past. Already in `toRemove`; nothing to do here.
		} else {
			newFI, err := rebuildHeadersStraddleFile(ctx, *straddle, newTo, p.snapDir, p.snapTmpDir, p.ChainConfig, p.logger)
			if err != nil {
				return nil, fmt.Errorf("rebuildHeadersStraddleFile(%s → newTo=%d): %w", straddle.Name(), newTo, err)
			}
			// Record new file paths (.seg + idx file names) for
			// AbortUnwind cleanup.
			rebuildPaths = append(rebuildPaths, newFI.Path)
			for _, idxName := range newFI.Type.IdxFileNames(newFI.From, newFI.To) {
				rebuildPaths = append(rebuildPaths, filepath.Join(newFI.Dir(), idxName))
			}
			// The OLD straddle file must be deleted post-commit. Add
			// it to `toRemove` so the existing trim machinery handles
			// it. Also delete the old accessors.
			oldFile := straddle // capture
			fakeEntry := &snapshot.FileEntry{Name: oldFile.Name()}
			toRemove = append(toRemove, fakeEntry)
			for _, idxName := range oldFile.Type.IdxFileNames(oldFile.From, oldFile.To) {
				toRemove = append(toRemove, &snapshot.FileEntry{Name: idxName})
			}
		}
	}

	if len(toRemove) == 0 && len(rebuildPaths) == 0 {
		return nil, nil
	}

	names := make([]string, 0, len(toRemove))
	paths := make([]string, 0, len(toRemove))
	for _, e := range toRemove {
		names = append(names, e.Name)
		paths = append(paths, filepath.Join(p.snapDir, e.Name))
	}

	// Stage the FS / inventory / downloader / republish ops for
	// post-commit execution. None of these are tx-bound, and once
	// an FS unlink or downloader notify lands it can't be reversed
	// by the mode-B tx rolling back. Staging here + executing in
	// FinalizeUnwind (after tx.Commit) means a failed/rolled-back
	// mode-B leaves the datadir unchanged and retriable. See
	// Provider.pendingTrim + FinalizeUnwind / AbortUnwind.
	sort.Strings(names)
	p.pendingTrimLock.Lock()
	p.pendingTrim = &pendingTrimState{names: names, paths: paths}
	if len(rebuildPaths) > 0 {
		p.pendingRebuild = &pendingRebuildState{paths: rebuildPaths}
	}
	p.pendingTrimLock.Unlock()
	return names, nil
}

// computeStepBoundaryForBlock returns the first step that must be
// trimmed entirely — i.e. files whose ToStep > stepBoundary cover only
// txnums past lastTxNum and must go; files whose ToStep == stepBoundary
// (the file containing lastTxNum) stay.
//
// The boundary = `(lastTxNum / stepSize) + 1`. For aligned cuts
// (lastTxNum is the last txnum of step stepContaining), this collapses
// to `(lastTxNum + 1) / stepSize`; for non-aligned cuts the file
// containing lastTxNum has excess coverage past lastTxNum that is
// handled at read time by the writable shadow's boundary-step
// diff-replay (see WipeWritableShadowPast).
//
// Returns 0 with no error when the Provider has no Aggregator
// (state-file trim is skipped; the caller checks Aggregator-nil
// separately).
func (p *Provider) computeStepBoundaryForBlock(ctx context.Context, tx kv.TemporalRwTx, toBlock uint64) (uint64, error) {
	if p.Aggregator == nil {
		return 0, nil
	}
	lastTxNum, err := rawdbv3.TxNums.Max(ctx, tx, toBlock)
	if err != nil {
		return 0, fmt.Errorf("read TxNums.Max(%d): %w", toBlock, err)
	}
	stepSize := p.Aggregator.StepSize()
	if stepSize == 0 {
		return 0, fmt.Errorf("aggregator StepSize() == 0 — chain misconfigured")
	}
	return (lastTxNum / stepSize) + 1, nil
}

// collectFilesPastBlock walks the inventory and returns every file
// whose content is strictly past toBlock. Block files use FromBlock
// (the straddle file FromBlock ≤ toBlock < ToBlock stays because it
// still holds blocks ≤ toBlock); state files use ToStep against
// stepBoundary by the same principle (the boundary step's file stays
// and the boundary-step diff-replay handles in-step pruning at read
// time). State files are only collected when p.Aggregator != nil —
// without an aggregator the stepBoundary input is 0 and would
// over-trim everything.
func (p *Provider) collectFilesPastBlock(toBlock, stepBoundary uint64) []*snapshot.FileEntry {
	var out []*snapshot.FileEntry

	for _, e := range p.Inventory.BlockFiles() {
		if e.FromBlock > toBlock {
			out = append(out, e)
		}
	}

	if p.Aggregator != nil {
		for _, domain := range p.Inventory.Domains() {
			for _, e := range p.Inventory.AllDomainFiles(domain) {
				if e.ToStep > stepBoundary {
					out = append(out, e)
				}
			}
		}
	}

	return out
}
