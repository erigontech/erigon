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

package state

import (
	"context"

	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

// keepCommitmentMergeOnly holds every domain but commitment. A rebuild shard runs
// inside a range-scoped iterator over the accounts and storage files, so those must
// keep the layout they were opened with until the range ends.
func keepCommitmentMergeOnly(r *Ranges) {
	for id := range r.domain {
		if kv.Domain(id) != kv.CommitmentDomain {
			r.domain[id] = DomainRanges{}
		}
	}
	for i := range r.invertedIndex {
		r.invertedIndex[i] = nil
	}
}

// mergeCommitmentStep collapses the shard files a rebuild has produced so far.
// Every shard opens the commitment files of the shards before it, so left flat the
// per-key lookup cost grows with the shard count and the range degrades to
// quadratic. Merging only commitment is what makes this safe to call mid-range.
func (a *Aggregator) mergeCommitmentStep(ctx context.Context, toTxNum uint64) (somethingDone bool, err error) {
	aggTx := a.BeginFilesRo()
	defer aggTx.Close()
	mxRunningMerges.Inc()
	defer mxRunningMerges.Dec()

	// Referencing ties the accounts/storage/commitment ranges together, and holding
	// the other two would stall the merge anyway.
	if a.referencesInCommitmentBranches() || aggTx.commitmentVisibleFilesReferenced() {
		return false, nil
	}

	r := aggTx.findMergeRange(toTxNum, a.StepSize(), a.StepsInFrozenFile())
	keepCommitmentMergeOnly(r)
	if !r.any() {
		// Reclaims the inputs of earlier merges, which stay pinned for as long as a
		// range-scoped reader still references them.
		a.cleanAfterMerge(nil)
		return false, nil
	}

	outs, err := aggTx.filesInRange(r)
	if err != nil {
		return false, err
	}

	in, err := aggTx.mergeFiles(ctx, outs, r)
	if err != nil {
		in.Close()
		return true, err
	}
	a.integrateMergedDirtyFiles(in)
	return true, nil
}

// rebuildShardMaxSteps sizes a rebuild shard for the machine. A shard holds its
// whole key slice in memory before dumping, so the step count tracks RAM. Tiers
// sit below the nominal sizes because a box reports less than it is sold with,
// and the top one is clamped: past it the range becomes too few, too large shards
// to recover from an interrupted run cheaply.
func rebuildShardMaxSteps(totalMemory uint64) uint64 {
	const gb = uint64(1) << 30
	for _, tier := range []struct{ atLeast, steps uint64 }{
		{384 * gb, 512},
		{192 * gb, 256},
		{96 * gb, 128},
	} {
		if totalMemory >= tier.atLeast {
			return tier.steps
		}
	}
	return commitment.DefaultRebuildShardMaxSteps
}

func defaultRebuildShardMaxSteps() uint64 { return rebuildShardMaxSteps(estimate.TotalMemory()) }
