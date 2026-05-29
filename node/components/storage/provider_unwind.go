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
// cannot derive from its own state. The complete mode-B chain only
// needs a writable temporal tx; the writable-domain shadow wipe and
// commitment-anchor verification both run inside it.
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
// Post-state invariant: MDBX is empty past toBlock; snapshot files
// are the only state. Cold-start-equivalent.
//
// Sub-ops, in order — landed together to avoid intermediate-wedge
// states (snapshot-trim without DB-reset leaves orphan DB entries
// pointing past the new tip; DB-reset without snapshot-trim leaves
// the file set inconsistent with the new chain head):
//
//  1. Snapshot-trim past toBlock — see unwindSnapshotsPastBlock.
//     Removes block files with ToBlock > toBlock and state files
//     with ToStep > stepBoundary, drops their torrents, republishes
//     chain.toml.
//  2. DB-reset past toBlock — see unwindDBPastBlock. Truncates
//     TxNums + canonical hashes, resets Headers / Bodies /
//     BlockHashes / Execution stages, clears ChangeSets3 > toBlock,
//     resets HeadBlockHash / HeadHeaderHash / ForkchoiceHead, and
//     wipes the writable-domain MDBX shadow (accounts / storage /
//     code / commitment + standalone IIs) past lastTxNum so the
//     snapshot files are the authoritative state.
//  3. Commitment anchor at toBlock — see ensureCommitmentAtBlock.
//     Pure verification: the commitment file's entry for toBlock's
//     step boundary surfaces naturally once the writable shadow is
//     gone. A mismatch surfaces as a fatal chain-malformed error
//     rather than being papered over by a recompute.
//
// The post-state is observably identical to a freshly-started node
// that has just processed frozen blocks up to toBlock (the cold-start
// equivalence claim in the design doc). External CL forkchoice
// coordination is test-driven per the design — implemented without
// any EL/CL signal here; the test rig is responsible for confirming
// standard Engine API responses are sufficient.
//
// Why this method can lift CLAUDE.md's "Unwind beyond data in
// snapshots not allowed": that rule was a placeholder for the code
// that just landed. The unit of cutting is the block: aligned chains
// trim entire files at step boundaries that coincide with block
// boundaries; non-aligned chains keep the file containing toBlock and
// rely on the writable shadow's boundary-step diff-replay (see
// WipeWritableShadowPast) to mask the file's excess coverage past
// toBlock. Either way no in-place file mutation occurs.
//
// Concurrency: Provider.Unwind does not synchronise. SetHead has
// already waited for ExecModule quiescence (no SharedDomains in
// flight) before invoking the Unwinder. Caller owns opts.Tx
// lifecycle and the commit.
func (p *Provider) Unwind(ctx context.Context, toBlock uint64, opts UnwindOpts) error {
	if p == nil {
		return fmt.Errorf("storage.Provider.Unwind: nil provider")
	}
	if opts.Tx == nil {
		return fmt.Errorf("storage.Provider.Unwind: opts.Tx is nil")
	}

	removed, err := p.unwindSnapshotsPastBlock(ctx, opts.Tx, toBlock)
	if err != nil {
		return fmt.Errorf("storage.Provider.Unwind: snapshot-trim: %w", err)
	}
	if p.logger != nil && len(removed) > 0 {
		p.logger.Info("[storage] Provider.Unwind: snapshot files trimmed past toBlock", "toBlock", toBlock, "files", len(removed))
	}

	// Recompute + write the commitment anchor BEFORE the writable
	// shadow wipe. With the SD-free recompute path, the recompute
	// looks for a baseline commitment via GetLatestFromDB (writable
	// shadow) and GetLatestFromFilesUpToStep — the writable shadow
	// still holds forward-execution's commitment writes at this
	// point, which provides a tight baseline. The recompute's write
	// lands at toBlock's last txnum (in step (stepBoundary-1)); the
	// subsequent wipe (step ≥ stepBoundary) leaves it intact.
	if err := p.ensureCommitmentAtBlock(ctx, opts.Tx, toBlock); err != nil {
		return fmt.Errorf("storage.Provider.Unwind: commitment-anchor: %w", err)
	}

	if err := p.unwindDBPastBlock(ctx, opts.Tx, toBlock); err != nil {
		return fmt.Errorf("storage.Provider.Unwind: db-reset: %w", err)
	}

	return nil
}
