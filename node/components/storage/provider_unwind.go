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
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
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

	// Engine is the consensus engine used by the partial-block re-exec
	// path (mid-block step cut, no per-tx history locally — typical of
	// minimal-prune datadirs). May be nil for callers that know toBlock
	// is NOT in a mid-block-cut scenario; Provider.Unwind detects the
	// scenario upfront via the file-side commitment state and returns a
	// clear error if Engine is nil but required.
	Engine rules.EngineReader
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

	// 1. Probe-compute the commitment anchor. Two outcomes:
	//    - Success: the chain has enough local history to validate
	//      directly. Captures the recompute result for Apply (step 5).
	//    - ErrHistoryGap: snapshot step boundary cuts mid-block AND
	//      per-tx history is absent locally (typical minimal-prune
	//      datadirs where exec started at the snapshot tip). Mode-B
	//      then routes through the wipe-first re-exec path below.
	//
	// Detecting mid-block-cut UPFRONT (before any mutation) is the
	// classification the user-direction 2026-06-04 calls out: pick the
	// flow based on the chain's data shape, not "always try X first."
	recompute, err := p.ensureCommitmentAtBlockCompute(ctx, opts.Tx, toBlock)
	if err != nil {
		var gap *commitmentdb.ErrHistoryGap
		if !errors.As(err, &gap) {
			// Genuine consensus mismatch (not the gap-shape error) —
			// refuse loud and early, no mutation.
			return fmt.Errorf("storage.Provider.Unwind: commitment-anchor compute: %w", err)
		}
		// Mid-block-cut + no history — partial-block re-exec required.
		if opts.Engine == nil {
			return fmt.Errorf("storage.Provider.Unwind: %w (UnwindOpts.Engine is nil)", err)
		}
		if p.logger != nil {
			p.logger.Info("[storage] Provider.Unwind: history gap detected, switching to wipe-first partial-block re-exec flow",
				"fromTxN", gap.FromTxNum, "fromBlock", gap.FromBlock, "toTxN", gap.ToTxNum, "toBlock", gap.ToBlock)
		}
		recompute, err = p.unwindWithReExec(ctx, opts.Tx, toBlock, opts.Engine, gap)
		if err != nil {
			return fmt.Errorf("storage.Provider.Unwind: wipe-first re-exec: %w", err)
		}
		// unwindWithReExec already ran the snapshot-trim + DB-reset +
		// commitment-apply for us; jump straight to regen+verify.
		defer recompute.Close()
		return p.unwindFinalize(ctx, opts.Tx, toBlock, recompute)
	}
	defer recompute.Close() // idempotent — Apply also closes

	// 2. Snapshot-trim (staged for post-commit FS deletion).
	removed, err := p.unwindSnapshotsPastBlock(ctx, opts.Tx, toBlock)
	if err != nil {
		return fmt.Errorf("storage.Provider.Unwind: snapshot-trim: %w", err)
	}
	if p.logger != nil && len(removed) > 0 {
		p.logger.Info("[storage] Provider.Unwind: snapshot files trimmed past toBlock", "toBlock", toBlock, "files", len(removed))
	}

	// 3. + 4. DB-reset (TxNums/canonicalHash/headPointers truncation)
	//    + WipeWritableShadowPast (per-domain wipe past lastTxNum +
	//    boundary-step diff-replay for history-tracked domains +
	//    whole-step wipe of commitment+RCache at stepContaining).
	//    unwindDBPastBlock orchestrates both.
	if err := p.unwindDBPastBlock(ctx, opts.Tx, toBlock); err != nil {
		return fmt.Errorf("storage.Provider.Unwind: db-reset: %w", err)
	}

	// 5. Apply the recompute result. Drains the branch collector +
	//    writes KeyCommitmentState into the now-cleaned writable
	//    shadow. The wipe's whole-step commitment clear (in step 3+4)
	//    guarantees these writes land without orphan dups.
	if err := p.ensureCommitmentAtBlockApply(ctx, opts.Tx, toBlock, recompute); err != nil {
		return fmt.Errorf("storage.Provider.Unwind: commitment-anchor apply: %w", err)
	}

	return p.unwindFinalize(ctx, opts.Tx, toBlock, recompute)
}

// unwindFinalize runs the regen + verify tail shared by both the
// standard and the wipe-first re-exec mode-B flows. By the time this
// runs the writable shadow is clean past toBlock and the commitment
// anchor has been written. It stages boundary-step regen files for the
// post-commit FinalizeUnwind swap, then verifies DB-image consistency.
func (p *Provider) unwindFinalize(ctx context.Context, tx kv.TemporalRwTx, toBlock uint64, recompute *commitmentRecomputeResult) error {
	// 6. Regenerate every state-domain boundary-step .kv so the
	//    on-disk content reflects the unwind target. Without this the
	//    boundary-step file's KeyCommitmentState record (at the
	//    file's max txNum, encoding the pre-unwind chain tip) shadows
	//    the writable shadow's mode-B anchor and the catch-up
	//    downloader wedges with ErrBehindCommitment. Stages .regen
	//    files for FinalizeUnwind to atomically swap + rebuild
	//    accessors against post-commit. See
	//    docs/plans/20260603-mode-b-boundary-step-regen-plan.md.
	lastTxNum, err := rawdbv3.TxNums.Max(ctx, tx, toBlock)
	if err != nil {
		return fmt.Errorf("storage.Provider.Unwind: lookup lastTxNum: %w", err)
	}
	pendingRegen, err := p.regenerateBoundaryStepFiles(ctx, tx, toBlock, lastTxNum, recompute.encodedTrieState)
	if err != nil {
		return fmt.Errorf("storage.Provider.Unwind: regenerate boundary-step files: %w", err)
	}
	if pendingRegen != nil {
		p.pendingTrimLock.Lock()
		p.pendingRegen = pendingRegen
		p.pendingTrimLock.Unlock()
	}

	// 7. Verify the DB image is consistent with the unwind target
	//    before the tx commits. Catches silent wipe-completeness gaps
	//    in any of the sub-ops above; a failure here rolls the whole
	//    mode-B back via the caller's AbortUnwind, which is far better
	//    than leaving a half-unwound DB that surfaces hours later as
	//    a wrong-block-data or wrong-state-root error.
	if err := verifyPostUnwindDBImage(ctx, tx, toBlock, lastTxNum); err != nil {
		return fmt.Errorf("storage.Provider.Unwind: %w", err)
	}

	return nil
}

// unwindWithReExec is the partial-block-cut variant of mode-B. Entered
// when the upfront ensureCommitmentAtBlockCompute returns ErrHistoryGap
// (= the snapshot step boundary cut mid-block AND per-tx history is
// absent locally — typical of minimal-prune datadirs whose exec started
// at the snapshot tip).
//
// Flow (wipe-first, then re-exec, then validate):
//
//  1. Snapshot-trim past toBlock (staged for post-commit FS deletion).
//  2. DB-reset + WipeWritableShadowPast — clears forward-exec entries
//     past toBlock so they don't shadow our re-exec output via GetLatest.
//  3. PopulateHistoryByReExec — re-executes the affected blocks
//     (partial for the gap.FromBlock; full for subsequent), writing
//     per-tx state into the writable shadow at txN ≤ toTxN. Because
//     forward-exec entries past toBlock are wiped, GetLatest now
//     returns our re-exec output for keys we touched.
//  4. ensureCommitmentAtBlockCompute — re-attempt the recompute.
//     HistoryKeyTxNumRange now finds our re-exec records;
//     trie.Process folds in the touched keys; GetAsOf walks through
//     the (now-existing) anchor records OR falls back to GetLatest
//     (which serves re-exec output post-wipe). The recompute validates
//     against header.stateRoot.
//  5. ensureCommitmentAtBlockApply — writes the validated commitment
//     anchor at toTxN.
//
// On any failure the caller's tx.Rollback (via AbortUnwind) reverts
// every step.
func (p *Provider) unwindWithReExec(ctx context.Context, tx kv.TemporalRwTx, toBlock uint64, engine rules.EngineReader, gap *commitmentdb.ErrHistoryGap) (*commitmentRecomputeResult, error) {
	// Snapshot per-block receipts BEFORE the wipe. The wipe whole-step
	// clears the RCache domain at stepContaining (which spans the gap's
	// txnums), so post-wipe ReadReceiptsCacheV2 returns zero. The
	// partial-block resume in PopulateHistoryByReExec needs receipts for
	// fromBlock to reconstruct the gas pool from prior txns' cumulative
	// gas usage. Reading them upfront preserves the data through the
	// upcoming wipe.
	preWipeReceipts := make(map[uint64]types.Receipts, gap.ToBlock-gap.FromBlock+1)
	txNumsReader := p.BlockReader.TxnumReader()
	for blockN := gap.FromBlock; blockN <= gap.ToBlock; blockN++ {
		block, _, berr := p.BlockReader.BlockWithSenders(ctx, tx, common.Hash{}, blockN)
		if berr != nil {
			return nil, fmt.Errorf("snapshot pre-wipe receipts: BlockWithSenders(%d): %w", blockN, berr)
		}
		if block == nil {
			return nil, fmt.Errorf("snapshot pre-wipe receipts: block %d not found", blockN)
		}
		receipts, rerr := rawdb.ReadReceiptsCacheV2(tx, block, txNumsReader)
		if rerr != nil {
			return nil, fmt.Errorf("snapshot pre-wipe receipts: block %d: %w", blockN, rerr)
		}
		preWipeReceipts[blockN] = receipts
	}

	// Snapshot-trim staged for post-commit FS deletion.
	removed, err := p.unwindSnapshotsPastBlock(ctx, tx, toBlock)
	if err != nil {
		return nil, fmt.Errorf("snapshot-trim: %w", err)
	}
	if p.logger != nil && len(removed) > 0 {
		p.logger.Info("[storage] Provider.Unwind(reexec): snapshot files trimmed past toBlock", "toBlock", toBlock, "files", len(removed))
	}

	// DB-reset + writable-shadow wipe past toBlock.
	if err := p.unwindDBPastBlock(ctx, tx, toBlock); err != nil {
		return nil, fmt.Errorf("db-reset: %w", err)
	}

	// Re-exec: populates writable shadow at txN ≤ toTxN. After this
	// the recompute's GetAsOf fallback (via wiped GetLatest) serves
	// our re-exec output.
	if err := p.PopulateHistoryByReExec(ctx, tx, engine, gap.FromTxNum, gap.FromBlock, gap.ToBlock, preWipeReceipts); err != nil {
		return nil, fmt.Errorf("re-exec: %w", err)
	}

	// Re-attempt the recompute. Should succeed: history records exist
	// in (cs.txN, toTxN], and GetAsOf fallback returns re-exec output.
	recompute, err := p.ensureCommitmentAtBlockCompute(ctx, tx, toBlock)
	if err != nil {
		return nil, fmt.Errorf("post-reexec commitment compute: %w", err)
	}

	// Apply the validated commitment anchor.
	if err := p.ensureCommitmentAtBlockApply(ctx, tx, toBlock, recompute); err != nil {
		recompute.Close()
		return nil, fmt.Errorf("commitment-anchor apply: %w", err)
	}

	return recompute, nil
}
