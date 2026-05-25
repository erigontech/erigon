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

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// unwindSnapshotsPastBlock removes every snapshot file whose content
// extends past toBlock. Mode-B sub-op #1; runs under aligned-mode and
// post-quiescence preconditions both established by Provider.Unwind's
// caller.
//
// Aligned-mode invariant: TxNums.Max(toBlock)+1 lands on a step
// boundary (i.e. (lastTxNum+1) % stepSize == 0). Violation surfaces
// as an error — that signals a chain that claims aligned mode but has
// step boundaries misaligned from block boundaries, which is a
// configuration bug, not something SetHead should silently paper
// over.
//
// Files removed:
//
//   - block files where ToBlock > toBlock (mechanical, ToBlock is a
//     literal block number in aligned mode);
//   - domain / history / idx files (all step-indexed) where
//     ToStep > stepBoundary, where stepBoundary == (lastTxNum+1)/stepSize.
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
	if len(toRemove) == 0 {
		return nil, nil
	}

	names := make([]string, 0, len(toRemove))
	paths := make([]string, 0, len(toRemove))
	for _, e := range toRemove {
		names = append(names, e.Name)
		paths = append(paths, filepath.Join(p.snapDir, e.Name))
	}

	for _, name := range names {
		p.Inventory.RemoveFile(name)
	}

	for _, path := range paths {
		_ = dir.RemoveFile(path)
		_ = dir.RemoveFile(path + ".torrent")
	}

	if p.downloaderClient != nil {
		if err := p.downloaderClient.Delete(ctx, names); err != nil && p.logger != nil {
			// Downloader failures don't abort the trim — the local
			// filesystem and inventory are already consistent; a
			// stale torrent on the downloader side is a self-healing
			// concern (operator restart fixes it).
			p.logger.Warn("[storage] Provider.Unwind: downloaderClient.Delete failed (continuing)", "err", err, "files", len(names))
		}
	}

	if p.republishChainToml != nil {
		if err := p.republishChainToml(); err != nil && p.logger != nil {
			// Same reasoning as above — local state is consistent;
			// peers see the old manifest until the next publish
			// cycle. Not a wedge.
			p.logger.Warn("[storage] Provider.Unwind: republishChainToml failed (continuing)", "err", err)
		}
	}

	sort.Strings(names)
	return names, nil
}

// computeStepBoundaryForBlock validates the aligned-mode invariant for
// toBlock and returns the step boundary that ends at toBlock's last
// txNum. Errors out if the invariant is violated. Returns 0 with no
// error when the Provider has no Aggregator (state-file trim is
// skipped in that case; the caller checks Aggregator-nil separately).
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
	if (lastTxNum+1)%stepSize != 0 {
		return 0, fmt.Errorf("aligned-mode invariant violated: toBlock=%d last-txNum=%d does not land on a step boundary (stepSize=%d, remainder=%d) — chain claims block-aligned but step boundaries don't align to block boundaries", toBlock, lastTxNum, stepSize, (lastTxNum+1)%stepSize)
	}
	return (lastTxNum + 1) / stepSize, nil
}

// collectFilesPastBlock walks the inventory and returns every file
// whose content extends past toBlock. Block files use ToBlock
// directly; state files use ToStep against stepBoundary. State files
// are only collected when p.Aggregator != nil — without an aggregator
// the stepBoundary input is 0 and would over-trim everything.
func (p *Provider) collectFilesPastBlock(toBlock, stepBoundary uint64) []*snapshot.FileEntry {
	var out []*snapshot.FileEntry

	for _, e := range p.Inventory.BlockFiles() {
		if e.ToBlock > toBlock {
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
