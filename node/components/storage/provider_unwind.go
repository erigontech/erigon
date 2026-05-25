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

	"github.com/erigontech/erigon/db/kv"
)

// UnwindOpts holds the per-call inputs Provider.Unwind needs that it
// cannot derive from its own state.
//
// Mode B as implemented today (snapshot-trim + DB-reset, no
// commitment write) only needs a writable temporal tx. The
// commitment-recompute path (next commit) will add the fields it
// requires back here — *SharedDomains will be re-introduced via
// the commitmentdb.CommitmentStateWriter interface, asserted
// structurally when that callsite lands.
type UnwindOpts struct {
	// Tx is the writable temporal transaction the storage-layer
	// sub-ops run inside. SetHead owns its lifecycle; Provider.Unwind
	// does NOT commit.
	Tx kv.TemporalRwTx
}

// Unwind is the storage-layer entry point for an *administrative*
// past-diffset unwind to an arbitrary block in aligned mode (mode B
// in docs/plans/20260525-admin-sethead-unwind-design.md). The
// exec-stage unwind path is unrelated: it stays on the existing
// AggregatorRoTx.Unwind / unwindExec3 chain and remains bounded by
// rawtemporaldb.CanUnwindBeforeBlockNum.
//
// This commit (2b) lands the architecture only — validation +
// dispatch surface. The three sub-ops the design calls for are
// implemented together in commit 2c (they cannot ship independently
// because snapshot-trim without DB-reset leaves a wedge of orphan
// DB entries pointing past the new snapshot tip, and DB-reset
// without snapshot-trim leaves the file set inconsistent with the
// chain head):
//
//  1. Snapshot-trim past toBlock — delete/truncate segments whose
//     To > toBlock, drop their torrents, republish chain.toml.
//  2. DB reset past toBlock — truncate TxNums + canonical hashes,
//     reset Headers / Bodies / BlockHashes / Execution stages, clear
//     diffsets > toBlock, reset HeadBlockHash / HeadHeaderHash /
//     ForkchoiceHead.
//  3. Commitment anchor at toBlock — if commitment is already there
//     (toBlock on a step boundary), no work; otherwise recompute
//     from history via GetAsOf.
//
// Why this method can lift CLAUDE.md's "Unwind beyond data in
// snapshots not allowed" for aligned chains: that rule was a
// placeholder for the code that lands across 2b + 2c. The rule
// stands for non-aligned chains because trimming an arbitrary block
// out of a 1k-rounded file would corrupt it; aligned mode lifts it
// because the unit of cutting *is* the block.
//
// Concurrency: Provider.Unwind does not synchronise. SetHead has
// already waited for ExecModule quiescence (no SharedDomains in
// flight) before invoking the Unwinder. Caller owns opts.Tx
// lifecycle and the commit.
func (p *Provider) Unwind(ctx context.Context, toBlock uint64, opts UnwindOpts) error {
	if p == nil {
		return fmt.Errorf("storage.Provider.Unwind: nil provider")
	}
	if !p.BlockAligned() {
		return fmt.Errorf("storage.Provider.Unwind: chain is not block-aligned (Config.Snapshot.BlockAlignedBoundaries=false); admin arbitrary-block unwind requires --snap.block-aligned-boundaries (the existing exec-stage CanUnwindBeforeBlockNum guard governs non-aligned chains)")
	}
	if opts.Tx == nil {
		return fmt.Errorf("storage.Provider.Unwind: opts.Tx is nil")
	}
	_ = ctx
	_ = toBlock

	// Sub-ops 1 + 2 + 3 land together in commit 2c.
	return fmt.Errorf("storage.Provider.Unwind: mode B sub-ops not yet implemented (snapshot-trim + DB-reset + commitment-recompute land together in commit 2c per docs/plans/20260525-admin-sethead-unwind-design.md to avoid leaving a wedge of orphan DB state past the new snapshot tip)")
}
