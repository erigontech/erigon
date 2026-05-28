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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// ensureCommitmentAtBlock is mode-B sub-op #3 — anchor the commitment
// trie at toBlock.
//
// Algorithm (SD-free, single-tx):
//
//  1. stepBoundary = (toBlockLastTxNum+1) / stepSize. Files whose end
//     step ≤ stepBoundary may carry a usable baseline commitment;
//     anything beyond is the over-step file the wipe in sub-op #2
//     deliberately superseded.
//
//  2. commitmentdb.RecomputeAtTxNumWithoutSD reads a file-side
//     baseline via GetLatestFromFilesUpToStep(stepBoundary), restores
//     the patricia trie state, replays accounts/storage/code touches
//     in (baselineTxNum, toBlockLastTxNum+1] via HistoryKeyTxNumRange
//     (those domains have history; the range query is well-defined),
//     calls trie.Process to fold them in, and returns the new root +
//     encoded trie state. No SharedDomains is opened — the
//     behind-commitment guard in NewSharedDomains is structurally
//     incompatible with mode B's mid-tx wiped-shadow state.
//
//  3. Validate the recomputed root against the block header's
//     stateRoot. A mismatch means the snapshot/history data couldn't
//     reproduce consensus — refuse loudly.
//
//  4. Write the new commitment entry into the writable shadow at
//     toBlockLastTxNum via TemporalMemBatch.DomainPut → Flush. After
//     this write the writable shadow holds the canonical commitment
//     at toBlock, surviving the next forward execution's
//     NewSharedDomains seek.
func (p *Provider) ensureCommitmentAtBlock(ctx context.Context, tx kv.TemporalRwTx, toBlock uint64) error {
	if p.BlockReader == nil {
		return fmt.Errorf("ensureCommitmentAtBlock: nil BlockReader")
	}
	if p.Aggregator == nil {
		return fmt.Errorf("ensureCommitmentAtBlock: nil Aggregator")
	}

	header, err := p.BlockReader.HeaderByNumber(ctx, tx, toBlock)
	if err != nil {
		return fmt.Errorf("HeaderByNumber(%d): %w", toBlock, err)
	}
	if header == nil {
		return fmt.Errorf("ensureCommitmentAtBlock: no header for block %d", toBlock)
	}

	lastTxNum, err := rawdbv3.TxNums.Max(ctx, tx, toBlock)
	if err != nil {
		return fmt.Errorf("TxNums.Max(%d): %w", toBlock, err)
	}
	stepSize := p.Aggregator.StepSize()
	if stepSize == 0 {
		return fmt.Errorf("aggregator StepSize() == 0")
	}
	if (lastTxNum+1)%stepSize != 0 {
		return fmt.Errorf("ensureCommitmentAtBlock: aligned-mode invariant violated: toBlock=%d lastTxNum=%d does not land on a step boundary (stepSize=%d)", toBlock, lastTxNum, stepSize)
	}
	stepBoundary := kv.Step((lastTxNum + 1) / stepSize)

	tmpDir := p.snapDir
	root, encodedTrieState, baselineTxNum, err := commitmentdb.RecomputeAtTxNumWithoutSD(ctx, tx, tmpDir, lastTxNum, stepBoundary, stepSize)
	if err != nil {
		return fmt.Errorf("RecomputeAtTxNumWithoutSD(toBlock=%d, lastTxNum=%d, stepBoundary=%d): %w", toBlock, lastTxNum, stepBoundary, err)
	}
	if common.Hash(root) != header.Root {
		return fmt.Errorf("recomputed root %x does not match header stateRoot %x at block %d (baselineTxNum=%d)", root, header.Root, toBlock, baselineTxNum)
	}

	// Write the new commitment entry into the writable shadow at
	// toBlockLastTxNum. We use TemporalMemBatch (a lightweight
	// memctx) rather than SharedDomains — SD's constructor seeks
	// commitment and trips the behind-commitment guard against the
	// over-step file. The memctx writes directly through the
	// domain-writer chain.
	cs := commitmentdb.NewCommitmentState(lastTxNum, toBlock, encodedTrieState)
	encoded, err := cs.Encode()
	if err != nil {
		return fmt.Errorf("encode commitment state: %w", err)
	}
	metrics := &changeset.DomainMetrics{Domains: map[kv.Domain]*changeset.DomainIOMetrics{}}
	mem := tx.Debug().NewMemBatch(metrics)
	defer mem.Close()
	if err := mem.DomainPut(kv.CommitmentDomain, string(commitmentdb.KeyCommitmentState), encoded, lastTxNum, nil); err != nil {
		return fmt.Errorf("memctx DomainPut(CommitmentDomain, KeyCommitmentState): %w", err)
	}
	if err := mem.Flush(ctx, tx); err != nil {
		return fmt.Errorf("memctx Flush: %w", err)
	}
	return nil
}
