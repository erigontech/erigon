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
	"sort"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
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
}

// Name implements validation.StepValidator.
func (CommitmentDomainValidator) Name() string { return "commitment_domain_state_at_end" }

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

	tx, err := v.DB.BeginTemporalRo(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// The step covers txnum range [FromStep*S, ToStep*S). Look up the
	// latest commitment state at-or-before maxTxNum = ToStep*S - 1.
	stepSize := tx.Debug().StepSize()
	endTxNum := toStep * stepSize
	if endTxNum == 0 {
		return fmt.Errorf("commitment step has zero ToStep — invalid step key")
	}
	maxTxNum := endTxNum - 1

	val, ok, fileStart, fileEnd, err := tx.Debug().GetLatestFromFiles(
		kv.CommitmentDomain, commitmentdb.KeyCommitmentState, maxTxNum)
	if err != nil {
		return fmt.Errorf("read KeyCommitmentState: %w", err)
	}
	if !ok {
		return fmt.Errorf("KeyCommitmentState not found for step [%d, %d)",
			fromStep, toStep)
	}

	// Verify the file's data range is contained within the step's
	// nominal txnum range. Strict equality is wrong because Erigon
	// MERGES commitment files: a merged file named e.g.
	// "v2.0-commitment.8960-8962.kv" carries only the LATEST
	// commitment state (older states are superseded by merge), so
	// its internal data range is narrower than the merged-name range
	// — fileStart can sit at the start of the LATER step instead of
	// at the merge-name's nominal start. The relevant invariant is
	// containment, not equality.
	expectedStart := fromStep * stepSize
	if fileStart < expectedStart {
		return fmt.Errorf("commitment file startTxNum %d is below step start %d (step [%d, %d))",
			fileStart, expectedStart, fromStep, toStep)
	}
	if fileEnd > endTxNum {
		return fmt.Errorf("commitment file endTxNum %d is past step end %d (step [%d, %d))",
			fileEnd, endTxNum, fromStep, toStep)
	}

	rootHashBytes, blockNum, txNum, err := commitment.HexTrieExtractStateRoot(val)
	if err != nil {
		return fmt.Errorf("decode KeyCommitmentState: %w", err)
	}
	if len(rootHashBytes) == 0 {
		return fmt.Errorf("decoded commitment root is empty for step [%d, %d)",
			fromStep, toStep)
	}

	// Sanity: txNum lies within the file's declared range.
	if txNum >= fileEnd {
		return fmt.Errorf("commitment txNum %d is gte fileEnd %d (step [%d, %d))",
			txNum, fileEnd, fromStep, toStep)
	}
	if txNum < fileStart {
		return fmt.Errorf("commitment txNum %d is lt fileStart %d (step [%d, %d))",
			txNum, fileStart, fromStep, toStep)
	}

	// Block-membership check: the txNum recorded in the commitment
	// state must lie within the block range that the commitment
	// claims (blockNum). Erigon writes commitment files at STEP
	// boundaries (txNum == fileEnd-1), and step boundaries cut
	// mid-block in the typical case — so an over-strict
	// "txNum == blockMaxTxNum" assertion would reject every
	// well-formed commitment. The right invariant is: the recorded
	// blockNum must be the block whose [minTxNum, maxTxNum] range
	// includes the recorded txNum.
	txNumReader := v.BlockReader.TxnumReader()
	containingBlock, ok, err := txNumReader.FindBlockNum(ctx, tx, txNum)
	if err != nil {
		return fmt.Errorf("FindBlockNum(txNum=%d): %w", txNum, err)
	}
	if !ok {
		return fmt.Errorf("no block contains txNum=%d (step [%d, %d), recorded blockNum=%d)",
			txNum, fromStep, toStep, blockNum)
	}
	if containingBlock != blockNum {
		return fmt.Errorf("commitment recorded blockNum=%d but txNum=%d lives in block %d (step [%d, %d))",
			blockNum, txNum, containingBlock, fromStep, toStep)
	}

	// Block-aligned commitments cross-check rootHash against
	// header.stateRoot directly; partial-block commitments (the
	// typical retire-step output) have no consensus anchor for
	// mid-block state, so they pause until the matching block .seg
	// is Advertisable for replay-verify. See
	// docs/plans/20260510-partial-block-validation-model.md.
	if len(rootHashBytes) != 32 {
		return fmt.Errorf("commitment rootHash is %d bytes, want 32 (step [%d, %d))",
			len(rootHashBytes), fromStep, toStep)
	}
	var rootArr [32]byte
	copy(rootArr[:], rootHashBytes)

	blockMaxTxNum, err := txNumReader.Max(ctx, tx, blockNum)
	if err != nil {
		return fmt.Errorf("TxnumReader.Max(blockNum=%d): %w", blockNum, err)
	}
	isPartialBlock := commitment.IsPartialBlock(txNum, blockMaxTxNum)

	if !isPartialBlock {
		// Block-aligned: direct cryptographic check closes the
		// state-loop. The commitment's recorded end-of-block state
		// MUST equal the consensus-committed header.stateRoot.
		header, err := v.BlockReader.HeaderByNumber(ctx, tx, blockNum)
		if err != nil {
			return fmt.Errorf("HeaderByNumber(blockNum=%d): %w", blockNum, err)
		}
		if header == nil {
			return fmt.Errorf("HeaderByNumber(blockNum=%d) returned nil header (step [%d, %d))",
				blockNum, fromStep, toStep)
		}
		if header.Root != rootArr {
			return fmt.Errorf("commitment rootHash %x disagrees with block %d header.stateRoot %x (step [%d, %d))",
				rootArr, blockNum, header.Root, fromStep, toStep)
		}
	} else if !blockSegAdvertisableForBlock(v.Inventory, blockNum) {
		// Pause (validation.ErrPause is treated as transient by the
		// lifecycle dispatch — no quarantine tick).
		return fmt.Errorf("partial-block commitment for block %d (txNum=%d, blockMaxTxNum=%d) — block .seg not yet Advertisable; pausing (step [%d, %d)): %w",
			blockNum, txNum, blockMaxTxNum, fromStep, toStep, validation.ErrPause)
	}

	// All checks passed — register the (step, block) binding for
	// downstream consumers (block→step mapping in PopulateFromName,
	// orchestrator policy decisions) AND stamp the cryptographic
	// anchors onto the FileEntry so they reach the V2 manifest via
	// chaintoml_v2.GenerateV2.
	if v.Inventory != nil {
		v.Inventory.RegisterStepBlockBoundary(toStep, blockNum)
		v.Inventory.SetAnchors(files[0].Name, snapshot.Anchors{
			Root:           rootArr,
			AtBlock:        blockNum,
			AtTxNum:        txNum,
			IsPartialBlock: isPartialBlock,
		})
	}
	logBindingRegistered(v.Logger, "lifecycle", files[0].Name, toStep, &blockNum)
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
