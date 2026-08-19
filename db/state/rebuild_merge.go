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
	"math/bits"

	"github.com/erigontech/erigon/db/kv"
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

	mxRunningMerges.Inc()
	defer mxRunningMerges.Dec()

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

// rebuildShardSteps sizes a shard from the memory on the machine and the key
// density of the range it covers. The range itself is the only upper bound: a
// constant one puts a wide enough range back into the many-shard regime however
// large the box, which is the cost sharding exists to avoid in the first place.
//
// The budget charges a shard for every key it walks, well above the marginal
// cost measured on mainnet, so it errs towards more and smaller shards. Ranges
// come from the accounts file step ranges and are already power-of-two spans;
// only the memory-derived count needs flooring to keep shard boundaries aligned
// with the merge ranges they feed.
func rebuildShardSteps(totalMemory, stepsInRange, keysPerStep uint64) uint64 {
	const bytesPerKey = 1400

	if stepsInRange == 0 {
		return 1
	}
	if keysPerStep == 0 {
		return stepsInRange
	}
	steps := prevPowerOfTwo(totalMemory / 2 / (bytesPerKey * keysPerStep))
	return min(max(steps, 1), stepsInRange)
}

func prevPowerOfTwo(n uint64) uint64 {
	if n == 0 {
		return 0
	}
	return uint64(1) << (bits.Len64(n) - 1)
}
