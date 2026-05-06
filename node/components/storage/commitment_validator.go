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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
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
//  2. **State is at the END of the commitment file** (the user-
//     specified consistency check). The recorded txNum must equal
//     the maxTxNum of the recorded blockNum — i.e. the commitment
//     was written AFTER the last txn of the last block in the step,
//     not mid-block. Catches files mis-named or built against the
//     wrong step boundary.
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

	// Verify the file boundary aligns with this step.
	expectedStart := fromStep * stepSize
	if fileStart != expectedStart {
		return fmt.Errorf("commitment file startTxNum mismatch: got %d, want %d (step [%d, %d))",
			fileStart, expectedStart, fromStep, toStep)
	}
	if fileEnd != endTxNum {
		return fmt.Errorf("commitment file endTxNum mismatch: got %d, want %d (step [%d, %d))",
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

	// State-at-end-of-file check: the recorded state must sit at the
	// END of its block (txNum == blockMaxTxNum). A txNum less than
	// blockMaxTxNum means the commitment was written mid-block — the
	// file is partial / mis-built and shouldn't be advertised.
	txNumReader := v.BlockReader.TxnumReader()
	blockMaxTxNum, err := txNumReader.Max(ctx, tx, blockNum)
	if err != nil {
		return fmt.Errorf("look up blockMaxTxNum for block %d: %w", blockNum, err)
	}
	if txNum != blockMaxTxNum {
		return fmt.Errorf("commitment state is not at end-of-block: txNum=%d, blockMaxTxNum=%d, blockNum=%d, step=[%d, %d)",
			txNum, blockMaxTxNum, blockNum, fromStep, toStep)
	}

	// All checks passed — register the (step, block) binding for
	// downstream consumers (block→step mapping in PopulateFromName,
	// orchestrator policy decisions).
	if v.Inventory != nil {
		v.Inventory.RegisterStepBlockBoundary(toStep, blockNum)
	}
	return nil
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
func seedLatestCommitmentBinding(ctx context.Context, inv *snapshot.Inventory, v CommitmentDomainValidator, logger log.Logger) {
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

	// Find the entry with the highest ToStep — its (step, block)
	// binding covers all blocks at or below.
	var latest *snapshot.FileEntry
	for _, e := range commitmentFiles {
		if e == nil || e.ToStep == 0 {
			continue
		}
		if latest == nil || e.ToStep > latest.ToStep {
			latest = e
		}
	}
	if latest == nil {
		return
	}

	files := []*snapshot.FileEntry{latest}
	if err := v.ValidateStep(ctx, files); err != nil {
		// Bootstrap files might fail validation for various reasons
		// (file format edge cases, missing block-reader entries for
		// older snapshots, etc.). The lifecycle path's normal flow
		// will eventually populate a binding from a fresh commitment
		// file produced by retire post-tip. Logged at Warn so
		// operators can investigate persistent failures.
		if logger != nil {
			logger.Warn("[storage] failed to seed bootstrap commitment binding",
				"name", latest.Name, "step", latest.ToStep, "err", err)
		}
		return
	}
	if logger != nil {
		logger.Info("[storage] seeded bootstrap commitment binding",
			"name", latest.Name, "toStep", latest.ToStep)
	}
}
