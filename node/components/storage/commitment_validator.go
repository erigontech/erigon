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
	"sort"
	"sync"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// CommitmentDomainValidator is the Stage-2 batch validator: when a
// commitment-domain step batch reaches LifecycleIndexed, this opens
// the commitment.kv via the existing aggregator read path, decodes
// KeyCommitmentState, runs the consistency checks, and registers the
// derived (step, block) binding into the inventory.
//
// Three responsibilities in one pass:
//
//  1. **State decoded successfully.** The KeyCommitmentState entry
//     for the step's range exists and parses. Catches torrent-piece-
//     hash-pass but format-corrupt files.
//
//  2. **Recorded blockNum is consistent with recorded txNum.** The
//     block whose txnum range includes the commitment's recorded
//     txNum must equal the recorded blockNum. Catches files where
//     the (txNum, blockNum) pair was clobbered or written against
//     the wrong block. Note: commitment files are written at STEP
//     boundaries (txNum = step's last txnum), and step boundaries
//     normally cut mid-block — so an "end-of-block" check would
//     reject every well-formed file. The block-membership check
//     captures the real invariant.
//
//  3. **Bind step → block.** On success, register the
//     (toStep, blockNum) binding in the inventory so block-snapshot
//     callers can map block ranges into step units.
//
// Runs identically on publisher and consumer — both call this from
// their lifecycle's BuildOnBatchValidation chain. Failure on either
// side prevents the step from advancing to Advertisable.
//
// Non-commitment-domain steps short-circuit (no-op): there's no
// commitment.kv to read, and the validator has nothing to check.
type CommitmentDomainValidator struct {
	DB          kv.TemporalRoDB
	BlockReader services.FullBlockReader
	Inventory   *snapshot.Inventory

	// Logger may be nil. When set, ValidateStep emits a positive
	// "binding registered" Info line on every successful validation
	// — used by multi-day fleet tests to distinguish lifecycle-path
	// bindings (commitment files going through the per-step batch
	// hook on publisher OR consumer) from the bootstrap-seeder
	// path (which has its own existing log line). Bootstrap-path
	// callers leave this nil to suppress duplicate logs.
	Logger log.Logger

	// PausedCache is optional. When set, ValidateStep memoizes the
	// LoadCommitmentRoot result for files that hit the partial-block
	// pause path. On the publisher's race window (commitment lands
	// before the matching block .seg reaches Advertisable), each
	// sweep retries; with the cache, retries skip the heavyweight
	// read+decode+invariants chain and re-check only the
	// blockSegAdvertisableForBlock gate. Cleared on successful
	// validation. Nil → cache disabled (behaviour unchanged from
	// before this field existed).
	PausedCache *PausedCommitmentCache

	// PruneMode classifies how to handle integrity.ErrAnchorBodyMissing
	// from LoadCommitmentRoot (Phase C — the txnum-range cross-check
	// against bodies.seg for the recorded AtBlock).
	//
	// Under archive mode + post-horizon under any pruning mode, bodies
	// are present and Phase C runs. ErrAnchorBodyMissing in those cases
	// is transient (bodies still downloading) — return ErrPause so the
	// lifecycle retries on the next sweep.
	//
	// Under any mode that prunes blocks with a finite distance
	// (minimal), bodies below blockTip - distance are INTENTIONALLY
	// absent — Phase C can never run for files anchored there. Phase A
	// (file-internal record) + Phase B (header.Root cross-check) still
	// run and remain authoritative; Phase C is skipped with a logged
	// warning rather than treated as a failure.
	//
	// Initialised=false (zero value) → treat ErrAnchorBodyMissing as
	// always-transient (ErrPause). Validators constructed by tools or
	// by tests that don't care about pruning leave this zero-valued.
	PruneMode prune.Mode

	// StateReady reports whether the state domain (commitment, …) has
	// finished loading into the queryable DB — i.e. the aggregator has
	// OpenFolder'd and InitialStateReady has fired. The commitment
	// validator's reads (GetLatestFromFiles on KeyCommitmentState) only
	// return consistent results once the domain's visible-file set has
	// stabilised; running against a half-loaded domain produces spurious
	// "commitment root not found" / "startTxNum below step start" /
	// "endTxNum past step end" errors. While !StateReady() the validator
	// returns ErrPause (transient, no quarantine tick).
	//
	// The FrozenBlocks()==0 gate below only covers block-segment
	// readiness — headers load before the state domain, so that gate
	// passes while commitment is still pending. StateReady closes that
	// gap.
	//
	// nil → gate disabled (tools / tests that load everything up front).
	StateReady func() bool
}

// PausedCommitmentCache memoizes the read+decode+invariants result of
// commitment validation across paused retries. Concurrency-safe.
// Production wiring (storage Provider) constructs one and injects via
// CommitmentDomainValidator.PausedCache; tests can leave it nil.
type PausedCommitmentCache struct {
	mu sync.Mutex
	m  map[string]integrity.CommitmentRootInfo
}

// NewPausedCommitmentCache returns an empty cache.
func NewPausedCommitmentCache() *PausedCommitmentCache {
	return &PausedCommitmentCache{m: make(map[string]integrity.CommitmentRootInfo)}
}

// Get returns the cached info and whether the entry exists.
func (c *PausedCommitmentCache) Get(name string) (integrity.CommitmentRootInfo, bool) {
	if c == nil {
		return integrity.CommitmentRootInfo{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	info, ok := c.m[name]
	return info, ok
}

// Put records info for name (overwriting any prior entry).
func (c *PausedCommitmentCache) Put(name string, info integrity.CommitmentRootInfo) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[name] = info
}

// Forget clears the entry for name. Called when validation passes
// (no longer paused) or when external state changes invalidate the
// cached info.
func (c *PausedCommitmentCache) Forget(name string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, name)
}

// Name implements validation.StepValidator.
func (CommitmentDomainValidator) Name() string { return "commitment_domain_state_at_end" }

// pruneModeExpectsBodyAbsent reports whether the configured prune mode
// would have INTENTIONALLY pruned the body for blockNum. Used to
// classify integrity.ErrAnchorBodyMissing from LoadCommitmentRoot:
//
//   - true  → body pruned by design (anchor below the prune horizon).
//     Phase C is unrunnable for the lifetime of this file;
//     callers should skip Phase C and log, not pause.
//   - false → body should be available. Absence is transient (segment
//     still downloading); callers should pause and retry.
//
// Mirrors the horizon logic from
// db/snapshotsync/preverified_filter.go's bug-Z block-prune filter
// (PruneTo(BlockTip) gives the lowest block kept) so this validator
// classifies blocks the same way the bootstrap-time filter did.
//
// Falls back to false (i.e., body should be available) for tools/tests
// that construct the validator without an initialised prune.Mode.
// Returns false under archive mode where Blocks.Enabled() is false.
func (v CommitmentDomainValidator) pruneModeExpectsBodyAbsent(blockNum uint64) bool {
	if !v.PruneMode.Initialised || !v.PruneMode.Blocks.Enabled() {
		return false
	}
	if v.BlockReader == nil {
		return false
	}
	tip := v.BlockReader.FrozenBlocks()
	if tip == 0 {
		return false
	}
	horizon := v.PruneMode.Blocks.PruneTo(tip)
	return blockNum < horizon
}

// ValidateStep implements validation.StepValidator. Short-circuits
// for non-commitment-domain groups (introspects files[0] for the
// domain — within a group all files share the relevant axis).
func (v CommitmentDomainValidator) ValidateStep(ctx context.Context, files []*snapshot.FileEntry) error {
	if len(files) == 0 || files[0].Domain != snapshot.DomainCommitment {
		return nil
	}
	fromStep := files[0].FromStep
	toStep := files[0].ToStep
	if v.DB == nil {
		return fmt.Errorf("CommitmentDomainValidator: nil DB")
	}
	if v.BlockReader == nil {
		return fmt.Errorf("CommitmentDomainValidator: nil BlockReader")
	}

	// At fresh bootstrap, the snapshot files are on disk but the EL
	// hasn't OpenSegments'd yet — BlockReader.FrozenBlocks() returns
	// 0 and any header lookup will fail. Return ErrPause so the
	// lifecycle retries on next sweep instead of accumulating
	// failures toward quarantine. The block-aligned validators
	// (HeaderChain / TxRoot / Receipt) detect this via per-block
	// nil-header checks; commitment's deeper LoadCommitmentRoot path
	// makes per-block detection awkward, so an early gate is cleaner.
	if v.BlockReader.FrozenBlocks() == 0 {
		return fmt.Errorf("BlockReader frozen tip = 0 (EL hasn't opened segments yet): %w", validation.ErrPause)
	}

	// State domain still loading — the aggregator hasn't finished
	// OpenFolder, so GetLatestFromFiles on the commitment domain reads
	// an unstable visible-file set. Pause rather than fail; a quarantine
	// tick here would strand a perfectly good file.
	if v.StateReady != nil && !v.StateReady() {
		return fmt.Errorf("state domain not loaded yet (InitialStateReady not fired): %w", validation.ErrPause)
	}

	tx, err := v.DB.BeginTemporalRo(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// Cache hit on a previously-paused file: skip the heavyweight
	// read+decode+invariants and re-check only the gate that's
	// expected to change between sweeps (block .seg reaching
	// Advertisable). Cache stays valid for the file's lifetime at
	// LifecycleIndexed — once it advances, the lifecycle dispatch
	// won't call us again on this name.
	info, cached := v.PausedCache.Get(files[0].Name)
	// bodyPruned: the anchor block's body is intentionally absent under
	// this prune mode, so Phase C could not run and PartialBlock()
	// cannot be determined — Phase B must be skipped (see the
	// ErrAnchorBodyMissing case below).
	var bodyPruned bool
	if !cached {
		stepSize := tx.Debug().StepSize()
		endTxNum := toStep * stepSize
		startTxNum := fromStep * stepSize
		var loadErr error
		info, loadErr = integrity.LoadCommitmentRoot(ctx, tx, v.BlockReader, startTxNum, endTxNum)
		switch {
		case loadErr == nil:
			// Phase A + Phase C both ran.
		case errors.Is(loadErr, integrity.ErrAnchorBodyMissing):
			// Phase A succeeded; Phase C couldn't run — the anchor
			// block's body is absent. Phase A's info (RootHash,
			// BlockNum, TxNum) is populated; BlockMinTxNum/BlockMaxTxNum
			// stay zero. If the block is ABOVE the prune horizon the
			// body should be present and just hasn't loaded yet:
			// transient, pause and retry. If BELOW the horizon the body
			// is intentionally pruned — handled by the deterministic
			// gate after this switch.
			if !v.pruneModeExpectsBodyAbsent(info.BlockNum) {
				return fmt.Errorf("commitment step [%d, %d): %w; pausing: %w",
					fromStep, toStep, loadErr, validation.ErrPause)
			}
		case errors.Is(loadErr, integrity.ErrCommitmentRangeMismatch):
			// GetLatestFromFiles returned the 'state' record from a
			// wider, merged file — this step file has just been
			// superseded by a merge. Transient: the stale step entry is
			// dropped by the next disk-reconcile sweep. Pause rather
			// than fail; a quarantine tick here would strand a file
			// that is about to be removed anyway.
			return fmt.Errorf("commitment step [%d, %d): %w; pausing: %w",
				fromStep, toStep, loadErr, validation.ErrPause)
		default:
			// Phase A failure (ErrCommitmentRecordInvalid) or other
			// real Phase C failure (ErrCommitmentTxNumRange) — propagate.
			return fmt.Errorf("commitment step [%d, %d): %w", fromStep, toStep, loadErr)
		}
	}

	// Deterministic Phase-B gate (validator hardening). Phase B (the
	// header cross-check) is valid only for full-block commitments;
	// telling full from partial needs BlockMaxTxNum from Phase C, which
	// needs the anchor block's body. For blocks below the prune horizon
	// the body is intentionally absent, so partial-block status is
	// unknowable — and LoadCommitmentRoot's body lookup is
	// non-deterministic under the live node's in-flux aggregator state
	// (it can spuriously resolve a body, let Phase C "succeed" with a
	// bogus range, and make PartialBlock() wrongly false — running
	// Phase B then compares a mid-block state root to a block header
	// and always mismatches). Gate Phase B on the anchor block's
	// horizon position — reliably known from Phase A — not on whether
	// the body lookup happened to succeed this sweep. Below the
	// horizon: validate by Phase A (internal record) alone.
	if v.pruneModeExpectsBodyAbsent(info.BlockNum) {
		bodyPruned = true
		if v.Logger != nil {
			v.Logger.Info("[storage] commitment Phase B+C skipped (anchor block below prune horizon — block alignment unknowable)",
				"file", files[0].Name, "atBlock", info.BlockNum,
				"step", fmt.Sprintf("[%d, %d)", fromStep, toStep))
		}
	}

	switch {
	case bodyPruned:
		// Phase A (internal record) alone — see the deterministic gate
		// above. No header cross-check: block alignment is unknowable
		// without the pruned body.
	case !info.PartialBlock():
		if err := integrity.VerifyCommitmentMatchesHeader(ctx, tx, v.BlockReader, info); err != nil {
			// ErrAnchorHeaderMissing is transient — header segment not
			// yet opened by EL. Pause and retry rather than quarantine.
			if errors.Is(err, integrity.ErrAnchorHeaderMissing) {
				return fmt.Errorf("commitment step [%d, %d): %w; pausing: %w",
					fromStep, toStep, err, validation.ErrPause)
			}
			return fmt.Errorf("commitment step [%d, %d): %w", fromStep, toStep, err)
		}
	case !blockSegAdvertisableForBlock(v.Inventory, info.BlockNum):
		// Partial-block commitment above the horizon: pause until the
		// matching block .seg is Advertisable for replay-verify. See
		// docs/plans/20260510-partial-block-validation-model.md.
		v.PausedCache.Put(files[0].Name, info)
		return fmt.Errorf("partial-block commitment for block %d (txNum=%d, blockMaxTxNum=%d) — block .seg not yet Advertisable; pausing (step [%d, %d)): %w",
			info.BlockNum, info.TxNum, info.BlockMaxTxNum, fromStep, toStep, validation.ErrPause)
	}
	// Validation passed — drop any prior pause-cache entry.
	v.PausedCache.Forget(files[0].Name)

	if v.Inventory != nil {
		v.Inventory.RegisterStepBlockBoundary(toStep, info.BlockNum)
		v.Inventory.SetAnchors(files[0].Name, snapshot.Anchors{
			Root:           info.RootHash,
			AtBlock:        info.BlockNum,
			AtTxNum:        info.TxNum,
			IsPartialBlock: info.PartialBlock(),
		})
	}
	logBindingRegistered(v.Logger, "lifecycle", files[0].Name, toStep, &info.BlockNum)
	return nil
}

// logBindingRegistered emits the canonical (step, block) binding log
// with a source tag. Used by both the lifecycle-path validator and
// the bootstrap-seeder; multi-day fleet tests grep one signal for
// both. block is a pointer because the bootstrap caller doesn't
// always know it (it's resolved lazily via the validator's side
// effect on Inventory).
func logBindingRegistered(logger log.Logger, source, name string, toStep uint64, block *uint64) {
	if logger == nil {
		return
	}
	if block != nil {
		logger.Info("[storage] binding registered",
			"source", source, "name", name, "toStep", toStep, "block", *block)
		return
	}
	logger.Info("[storage] binding registered",
		"source", source, "name", name, "toStep", toStep)
}

// blockSegAdvertisableForBlock reports whether some block-domain
// FileEntry in inv covers blockNum and is at LifecycleAdvertisable.
// Uses the FromBlock/ToBlock fields already populated by
// PopulateFromName at AddFile time — no re-parse. Iterates in reverse
// because partial-block validations are tip-relative and matching
// block .seg entries are appended last.
func blockSegAdvertisableForBlock(inv *snapshot.Inventory, blockNum uint64) bool {
	if inv == nil {
		return false
	}
	files := inv.BlockFiles()
	for i := len(files) - 1; i >= 0; i-- {
		f := files[i]
		if !f.Advertisable || f.ToBlock <= f.FromBlock {
			continue
		}
		if blockNum >= f.FromBlock && blockNum < f.ToBlock {
			return true
		}
	}
	return false
}

// extractBootstrapCommitmentAnchors walks every commitment-domain
// file in the inventory and runs Phase A (ExtractCommitmentRecord) to
// populate the inventory's (step, block) bindings and per-file
// Anchors. Phase A reads only the file's KeyCommitmentState record —
// no header lookup, no body lookup, no segment open required — so it
// works at fresh startup before EL has opened its header segments AND
// under any prune mode (bodies below the prune horizon are
// intentionally absent under minimal mode; Phase A doesn't need them).
//
// This is "the more bootstrap work the initial publisher needs" so
// the V2 manifest the publisher subsequently generates can carry
// per-file ProofRoot / AtBlock / AtTxNum anchors for every commitment
// file, not just the tip. Consumers receiving that manifest verify
// each entry's ProofRoot against their own header.Root (Phase B,
// always works since headers are kept back to zero) — no per-file
// bodies needed on the consumer side either.
//
// Runs synchronously but does only file reads — does NOT block
// execution start. EL execution proceeds forward from the snapshot
// tip while this populates anchors historically; the two run
// independently (anchors are per-file, execution is per-block).
//
// On per-file Phase A failure (genuine record corruption), logs at
// Warn with the file name + error and continues with the next file.
// Files that fail here will also fail later in the lifecycle driver's
// OnValidation pass — the redundancy is intentional, so the operator
// gets one consolidated bootstrap-time log AND the per-file failure
// is also tracked for quarantine.
func extractBootstrapCommitmentAnchors(ctx context.Context, inv *snapshot.Inventory, db kv.TemporalRoDB, br services.FullBlockReader, logger log.Logger) {
	if inv == nil || db == nil {
		return
	}
	commitmentFiles := inv.AllDomainFiles(snapshot.DomainCommitment)
	if len(commitmentFiles) == 0 {
		if logger != nil {
			logger.Debug("[storage] no bootstrap commitment files; block-files will wait until lifecycle produces one")
		}
		return
	}
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		if logger != nil {
			logger.Warn("[storage] bootstrap anchor extraction: begin tx", "err", err)
		}
		return
	}
	defer tx.Rollback()
	stepSize := tx.Debug().StepSize()

	var extracted, failed int
	for _, entry := range commitmentFiles {
		if entry == nil || entry.ToStep == 0 {
			continue
		}
		// Skip if anchors already set (e.g. an earlier pass through
		// this loop, or the lifecycle path beat us to it). Re-running
		// Phase A would re-derive the same values — idempotent — but
		// the skip is cheap and avoids duplicate log lines.
		if !entry.Anchors.IsZero() {
			continue
		}
		startTxNum := entry.FromStep * stepSize
		endTxNum := entry.ToStep * stepSize
		info, err := integrity.ExtractCommitmentRecord(ctx, tx, startTxNum, endTxNum)
		if err != nil {
			failed++
			if logger != nil {
				logger.Warn("[storage] bootstrap anchor extraction failed",
					"file", entry.Name,
					"step", fmt.Sprintf("[%d, %d)", entry.FromStep, entry.ToStep),
					"err", err)
			}
			continue
		}
		inv.RegisterStepBlockBoundary(entry.ToStep, info.BlockNum)
		// Phase A doesn't know PartialBlock (needs body for txnum range)
		// — leave IsPartialBlock=false. Historical bootstrap files are
		// block-aligned by retire convention; partial-block files only
		// appear at the tip and the lifecycle path runs full validation
		// on those (including PartialBlock detection via Phase C).
		inv.SetAnchors(entry.Name, snapshot.Anchors{
			Root:    info.RootHash,
			AtBlock: info.BlockNum,
			AtTxNum: info.TxNum,
		})
		extracted++
	}
	_ = br // reserved for future Phase B (header check) at bootstrap if we want it inline; currently the lifecycle driver runs Phase B asynchronously
	if logger != nil {
		logger.Info("[storage] bootstrap commitment anchors extracted",
			"extracted", extracted, "failed", failed, "totalFiles", len(commitmentFiles))
	}
}

// seedLatestCommitmentBinding finds the highest commitment-domain
// step in the inventory and runs CommitmentDomainValidator on it
// directly to register a (step, block) binding. Called once after
// bootstrap populates the inventory.
//
// Bootstrap files start at LifecycleAdvertisable directly (back-
// compat: visible-in-aggregator implies fully validated by previous
// runs), so they bypass the lifecycle's per-step batch validation
// and CommitmentDomainValidator never fires on them. Without a
// binding registered, the block-step wait gate in
// BuildOnBatchValidation blocks ALL block files indefinitely. This
// helper closes that loop by registering the latest bootstrap
// commitment binding directly.
//
// On error or empty inventory, no binding is registered — block
// files keep waiting until a commitment file goes through the
// lifecycle (e.g. post-tip retire produces one).
// stepValidator is the narrow interface seedLatestCommitmentBinding
// needs from CommitmentDomainValidator — exposed to make the
// candidate-iteration logic testable without a real DB / BlockReader.
type stepValidator interface {
	ValidateStep(ctx context.Context, files []*snapshot.FileEntry) error
}

func seedLatestCommitmentBinding(ctx context.Context, inv *snapshot.Inventory, v stepValidator, logger log.Logger) {
	if inv == nil {
		return
	}
	commitmentFiles := inv.AllDomainFiles(snapshot.DomainCommitment)
	if len(commitmentFiles) == 0 {
		if logger != nil {
			logger.Debug("[storage] no bootstrap commitment files; block-files will wait until lifecycle produces one")
		}
		return
	}

	// Sort by ToStep descending — try the most recent first, fall
	// back to earlier steps if the latest fails (e.g. mid-block
	// commitment files written by an interrupted retire). We need
	// any binding to release the block-step wait gate; an older
	// binding still covers all blocks at or below its step boundary.
	candidates := make([]*snapshot.FileEntry, 0, len(commitmentFiles))
	for _, e := range commitmentFiles {
		if e == nil || e.ToStep == 0 {
			continue
		}
		candidates = append(candidates, e)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ToStep > candidates[j].ToStep
	})
	if len(candidates) == 0 {
		return
	}

	var lastErr error
	var lastFailed *snapshot.FileEntry
	for _, candidate := range candidates {
		if err := v.ValidateStep(ctx, []*snapshot.FileEntry{candidate}); err != nil {
			lastErr = err
			lastFailed = candidate
			continue
		}
		logBindingRegistered(logger, "bootstrap", candidate.Name, candidate.ToStep, nil)
		return
	}

	// All candidates failed. Logged at Warn so operators can
	// investigate persistent failures; the most recent failure
	// carries enough context to diagnose.
	if logger != nil && lastFailed != nil {
		logger.Warn("[storage] failed to seed any bootstrap commitment binding (all candidates rejected)",
			"candidates", len(candidates),
			"lastTried", lastFailed.Name, "lastStep", lastFailed.ToStep, "err", lastErr)
	}
}
