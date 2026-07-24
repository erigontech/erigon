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
	"os"
	"sort"
	"strings"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
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

// Unwind is the storage-layer entry point for an administrative
// past-diffset unwind to toBlock (mode B). The exec-stage unwind path
// is unrelated. Post-state: MDBX empty past toBlock, snapshot files
// hold the only state — observably identical to a cold-start node
// processing frozen blocks up to toBlock.
//
// The sub-ops (snapshot-trim, DB-reset + writable-shadow wipe,
// commitment anchor, boundary-step regen, verify) must land together;
// leaving the FS and DB out of step wedges subsequent forward exec.
//
// Caller (SetHead) has already quiesced the ExecModule and owns
// opts.Tx's lifetime and commit.
func (p *Provider) Unwind(ctx context.Context, toBlock uint64, opts UnwindOpts) error {
	if p == nil {
		return fmt.Errorf("storage.Provider.Unwind: nil provider")
	}
	if opts.Tx == nil {
		return fmt.Errorf("storage.Provider.Unwind: opts.Tx is nil")
	}

	// Pre-condition: every block-snapshot .seg file on disk past
	// toBlock MUST be known to Inventory so the trim pass catches it.
	// Without this the trim silently misses on-disk files
	// (collectFilesPastBlock iterates Inventory.BlockFiles), no-ops the
	// snapshot side, and the commitment anchor still applies — leaving
	// the writable DB at toBlock while on-disk snapshots cover past it.
	//
	// Self-heal: register the orphans into Inventory ourselves. The
	// orphans typically come from OtterSync's "Requesting remaining
	// snapshots from downloader" path (RequestSnapshotsDownload over
	// gRPC, not the bus-driven flow) — so no flow.DownloadComplete
	// fires and the orchestrator never learns of them. Refusing the
	// unwind here is too brittle: every retire-after-unwind cycle that
	// pulls 1k stubs subsumed by a wider merged sibling would wedge.
	// Live-caught 2026-06-12 (orphan retire output) and 2026-06-19 soak
	// v15/v17/v18 (preverified 1k stubs subsumed by 100k merged chunk).
	if err := p.healInventoryOrphansPastBlock(toBlock); err != nil {
		return err
	}

	// 0. Ensure preverified history for the compute walk range is on
	//    disk. Under --prune.mode=minimal the bootstrap filter skips
	//    state history (see db/snapshotsync/preverified_filter.go),
	//    leaving the compute's walk from a step-256-aligned baseline
	//    unable to find keys touched pre-retention. The ensure step
	//    downloads only what's missing and returns a cleanup callback
	//    that removes the temp files after Unwind returns — critical
	//    for test determinism (see memory/mode-b-temp-history-cleanup-decision.md).
	historyCleanup, err := p.ensureHistoryForUnwindWalk(ctx, opts, toBlock)
	if err != nil {
		return fmt.Errorf("storage.Provider.Unwind: ensure history: %w", err)
	}
	defer historyCleanup()

	// 1. Compute the commitment anchor. History for the walk range was
	//    downloaded upfront (step 0); anything else is a genuine
	//    consensus mismatch — refuse loud and early, no mutation.
	recompute, err := p.ensureCommitmentAtBlockCompute(ctx, opts.Tx, toBlock)
	if err != nil {
		return fmt.Errorf("storage.Provider.Unwind: commitment-anchor compute: %w", err)
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

// unwindFinalize runs the regen + verify tail of mode-B unwind. By
// the time this runs the writable shadow is clean past toBlock and the
// commitment anchor has been written. It stages boundary-step regen
// files for the post-commit FinalizeUnwind swap, then verifies DB-image
// consistency.
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

	// 6b. Invalidate aggregator-lifetime caches past lastTxNum. Mode-B
	//     never goes through SharedDomains.Unwind, so without this the
	//     BranchCache (and any future cache added to Aggregator.Unwind)
	//     keeps entries with txN > lastTxNum and forward-exec after
	//     mode-B reads stale cached commitment branches — surfaces as
	//     wrong-trie-root a handful of blocks past the unwind target.
	if p.Aggregator != nil {
		p.Aggregator.Unwind(lastTxNum)
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

	// Inventory-past-toBlock is not checked inline. At this point the
	// snapshot-trim has only STAGED removals in pendingTrim;
	// Inventory.RemoveFile + FS delete happen post-tx in
	// FinalizeUnwind because they're irreversible. A transient
	// "Inventory has entries past toBlock" state therefore exists by
	// design between unwindFinalize and FinalizeUnwind, and any
	// runtime check here would either false-positive on that
	// transient state or require expensive FS rescans. The invariant
	// "after FinalizeUnwind, FS and Inventory agree and contain no
	// entries past toBlock" is enforced via unit tests (see
	// provider_unwind_finalize_inventory_test.go).

	return nil
}

// findInventoryEntriesPastBlock returns Inventory block-file entries
// whose FromBlock is strictly past toBlock. Mirror of
// findInventoryOrphansPastBlock: that one catches files on disk not
// in Inventory; this one catches Inventory entries that should have
// been removed by snapshot-trim. The wedge it guards against — iter
// 3 of the 5-iter mode-B soak 2026-06-14 reported inv_extras=3 from
// a Retire that produced files past target while SetHead's quiescence
// wait covered only SharedDomains. The cancel-before-unwind fix
// prevents that race; this is the defense-in-depth assertion run in
// the same tx so any regression rolls the unwind back instead of
// committing silent corruption.
//
// Straddle files (FromBlock ≤ toBlock < ToBlock) are kept — they are
// handled by the dedicated straddle-rebuild path in unwindFinalize
// and are not "extras."
//
// Returns nil + nil when Inventory is unset (harness/test paths).
func (p *Provider) findInventoryEntriesPastBlock(toBlock uint64) ([]string, error) {
	if p.Inventory == nil {
		return nil, nil
	}
	var extras []string
	for _, e := range p.Inventory.BlockFiles() {
		if e.FromBlock > toBlock {
			extras = append(extras, e.Name)
		}
	}
	sort.Strings(extras)
	return extras, nil
}

// healInventoryOrphansPastBlock registers every on-disk v1.1-*.seg
// file past toBlock that isn't already in Inventory. Called from
// Provider.Unwind as a pre-condition for the trim pass — without it,
// collectFilesPastBlock iterates Inventory.BlockFiles and silently
// misses files written via paths that don't fire flow.DownloadComplete
// (OtterSync's RequestSnapshotsDownload-over-gRPC after each mode-B
// recovery is the common source — 1k stubs subsumed by a wider
// merged sibling). PopulateFromName-style parsing fills FromBlock /
// ToBlock so the entry classifies in collectFilesPastBlock's
// range-based filter.
//
// No-op when snapDir or Inventory aren't set (tools and tests that
// construct a bare Provider).
func (p *Provider) healInventoryOrphansPastBlock(toBlock uint64) error {
	orphans, err := p.findInventoryOrphansPastBlock(toBlock)
	if err != nil {
		return fmt.Errorf("storage.Provider.Unwind: inventory pre-flight check: %w", err)
	}
	if len(orphans) == 0 {
		return nil
	}
	for _, name := range orphans {
		entry := &snapshot.FileEntry{
			Name:         name,
			Local:        true,
			Advertisable: true,
		}
		if info, _, ok := snaptype.ParseFileName(p.snapDir, name); ok {
			entry.FromBlock = info.From
			entry.ToBlock = info.To
		}
		if err := p.Inventory.AddFile(entry); err != nil {
			return fmt.Errorf("storage.Provider.Unwind: inventory self-heal (file %s): %w", name, err)
		}
	}
	if p.logger != nil {
		p.logger.Info("[storage] Provider.Unwind: self-healed inventory drift",
			"orphans", len(orphans), "files", orphans, "toBlock", toBlock)
	}
	return nil
}

// findInventoryOrphansPastBlock scans the snapshots dir for top-level
// v1.1-*.seg block snapshot files whose range extends at or past
// toBlock, then cross-checks against Inventory.BlockFiles(). Returns
// the sorted list of file names that exist on disk but are NOT in
// Inventory — the orphans Provider.Unwind would silently miss.
//
// The predicate is `info.To > toBlock` — covers BOTH the strictly-past
// files (From > toBlock) AND the straddle file (From ≤ toBlock < To).
// The straddle file MUST be healable: mode-B's rebuild pass scans
// Inventory to find the straddle for truncation; without this file in
// Inventory, the rebuild silently returns rebuilt=0 and the file
// survives the unwind covering blocks past toBlock — breaking the
// "no file straddles toBlock after unwind" invariant.
//
// Returns nil + nil when SnapDir or Inventory are unset (tools and
// tests that don't carry the full storage component) — those callers
// don't have the architectural setup the check is guarding against.
func (p *Provider) findInventoryOrphansPastBlock(toBlock uint64) ([]string, error) {
	if p.snapDir == "" || p.Inventory == nil {
		return nil, nil
	}
	entries, err := os.ReadDir(p.snapDir)
	if err != nil {
		return nil, fmt.Errorf("read snapDir %s: %w", p.snapDir, err)
	}
	known := make(map[string]struct{})
	for _, e := range p.Inventory.BlockFiles() {
		known[e.Name] = struct{}{}
	}
	var orphans []string
	for _, de := range entries {
		if de.IsDir() {
			continue
		}
		name := de.Name()
		if !strings.HasSuffix(name, ".seg") {
			continue
		}
		// Top-level v1.1-* are block snapshots. v2.0-* state-aggregator
		// files live in subdirs (domain/, history/, idx/) and never
		// reach this top-level scan.
		if !strings.HasPrefix(name, "v1.1-") {
			continue
		}
		info, _, ok := snaptype.ParseFileName(p.snapDir, name)
		if !ok {
			continue
		}
		// Include files whose range extends past toBlock: strictly-past
		// (From > toBlock) AND the straddle (From ≤ toBlock < To). The
		// straddle must be healable so mode-B's rebuild can find it to
		// truncate; otherwise the file survives the unwind, covering
		// blocks past toBlock and breaking the no-straddle invariant.
		if info.To <= toBlock {
			continue
		}
		if _, in := known[name]; in {
			continue
		}
		orphans = append(orphans, name)
	}
	sort.Strings(orphans)
	return orphans, nil
}
