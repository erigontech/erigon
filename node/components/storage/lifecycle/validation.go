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

package lifecycle

import (
	"context"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// ContentSourceFor resolves a file's bytes for validation. Production
// wiring returns a validation.FileContent pointing at the file's path
// under snap-dir; tests can return a validation.BytesContent backed by
// canned content. May return nil for stage-1 validators that do not
// need file bytes (name/range/kind checks).
type ContentSourceFor func(*snapshot.FileEntry) validation.ContentSource

// BuildOnValidation returns a Handler suitable for Driver.OnValidation.
// The handler runs the per-file validation Chain against the entry;
// on success advances to LifecycleAdvertisable; on failure returns the
// wrapped error.
//
// Per-file validation is retained for callers that want individual
// file checks. The production wiring uses BuildOnBatchValidation
// (per-step) — see docs/plans/20260504-step-and-minimum-unified.md
// for the reasoning.
//
// chain may be empty — an empty Chain accepts everything.
// contentFor may be nil; validators that need bytes get nil and must
// handle that case themselves.
// logger may be nil; on successful advance the handler emits an Info
// log line.
func BuildOnValidation(chain validation.Chain, contentFor ContentSourceFor, inv *snapshot.Inventory, logger log.Logger) Handler {
	return func(_ context.Context, e *snapshot.FileEntry) error {
		var content validation.ContentSource
		if contentFor != nil {
			content = contentFor(e)
		}
		if err := chain.Validate(e, content); err != nil {
			return err
		}
		if inv.AdvanceTo(e.Name, snapshot.LifecycleAdvertisable) && logger != nil {
			logger.Info("[storage-lifecycle] advanced", "file", e.Name, "to", "Advertisable")
		}
		return nil
	}
}

// BuildOnBatchValidation returns a Handler that runs per-step
// (StepChain) validation in TWO passes — minimum first, extras second —
// so the step's minimum publishable subset becomes Advertisable as
// soon as it's locally Indexed, without waiting for the rest of the
// step.
//
// Pass 1 (minimum-first):
//
//	When the step's minimum subset (per FileEntry.IsMinimum) is fully
//	at LifecycleIndexed (or beyond) but at least one minimum file is
//	still below LifecycleAdvertisable, the chain runs across just the
//	minimum subset. On pass, those files advance to Advertisable. The
//	publisher can advertise / consumers can use the minimum
//	immediately — extras may still be downloading or building.
//
// Pass 2 (extras / full step):
//
//	When ALL files in the step are at LifecycleIndexed (or beyond)
//	but at least one extras file is still below Advertisable, the
//	chain runs across the extras subset. On pass, extras advance to
//	Advertisable.
//
// Both passes can fire in the same handler invocation — pass 1
// advances the minimum, pass 2 then sees the extras ready and
// advances them.
//
// Singletons (caplin / meta / salt — files with zero StepKey) bypass
// the batch path entirely; they advance individually.
//
// On step-incomplete (the relevant subset hasn't all reached Indexed):
// the handler returns nil — no error, no advance, sweep retries.
// On chain failure: returns the wrapped error; driver logs at Debug
// and increments the file's failure counter.
//
// logger may be nil; on successful advance the handler emits Info
// log lines distinguishing the minimum pass from the extras pass.
func BuildOnBatchValidation(chain validation.StepChain, inv *snapshot.Inventory, logger log.Logger) Handler {
	return func(ctx context.Context, e *snapshot.FileEntry) error {
		// Dispatch by file kind:
		//   - state file (Domain != "") → step-axis grouping
		//   - block file (FromBlock/ToBlock set) → block-range axis
		//   - everything else (caplin, meta, salt) → singleton path
		if e.Domain != "" {
			return runStepGroup(ctx, e, chain, inv, logger)
		}
		if !e.BlockKey().IsZero() {
			return runBlockGroup(ctx, e, chain, inv, logger)
		}
		// Singleton: advance directly.
		if inv.AdvanceTo(e.Name, snapshot.LifecycleAdvertisable) && logger != nil {
			logger.Info("[storage-lifecycle] advanced", "file", e.Name, "to", "Advertisable")
		}
		return nil
	}
}

func runStepGroup(ctx context.Context, e *snapshot.FileEntry, chain validation.StepChain, inv *snapshot.Inventory, logger log.Logger) error {
	key := e.StepKey()
	group := inv.FilesAtStep(key)
	if len(group.Files) == 0 {
		return nil
	}

	// Pass 1: minimum subset, if any.
	if minimum := group.Minimum(); needsValidation(minimum) {
		if err := chain.Validate(ctx, minimum); err != nil {
			return err
		}
		advanced := inv.AdvanceFiles(fileNames(minimum), snapshot.LifecycleAdvertisable)
		if logger != nil && len(advanced) > 0 {
			logger.Info("[storage-lifecycle] step minimum advanced",
				"from", key.FromStep, "to", key.ToStep, "domain", string(key.Domain),
				"files", len(advanced))
		}
	}

	// Pass 2: extras, once full step is Indexed.
	if extras := group.Extras(); needsValidation(extras) && group.AllAtState(snapshot.LifecycleIndexed) {
		if err := chain.Validate(ctx, extras); err != nil {
			return err
		}
		advanced := inv.AdvanceFiles(fileNames(extras), snapshot.LifecycleAdvertisable)
		if logger != nil && len(advanced) > 0 {
			logger.Info("[storage-lifecycle] step extras advanced",
				"from", key.FromStep, "to", key.ToStep, "domain", string(key.Domain),
				"files", len(advanced))
		}
	}
	return nil
}

func runBlockGroup(ctx context.Context, e *snapshot.FileEntry, chain validation.StepChain, inv *snapshot.Inventory, logger log.Logger) error {
	key := e.BlockKey()

	// Block files advance only after a commitment-derived
	// (step, block) binding covers their block range. Without that
	// binding the publisher can't attest, and the consumer can't
	// verify, that the block range corresponds to a known canonical
	// step. Block files beyond the last validated step legitimately
	// wait here — they either get a step from a future commitment
	// binding (consumer path) or from local execution producing
	// commitment for them (publisher path).
	if _, hasBinding := inv.BlockToStep(key.ToBlock); !hasBinding {
		return nil
	}

	group := inv.FilesAtBlockRange(key)
	if len(group.Files) == 0 {
		return nil
	}

	// Pass 1: minimum subset (headers.seg + headers.idx).
	if minimum := group.Minimum(); needsValidation(minimum) {
		if err := chain.Validate(ctx, minimum); err != nil {
			return err
		}
		advanced := inv.AdvanceFiles(fileNames(minimum), snapshot.LifecycleAdvertisable)
		if logger != nil && len(advanced) > 0 {
			logger.Info("[storage-lifecycle] block-range minimum advanced",
				"fromBlock", key.FromBlock, "toBlock", key.ToBlock,
				"files", len(advanced))
		}
	}

	// Pass 2: extras (bodies, transactions, accessors).
	if extras := group.Extras(); needsValidation(extras) && group.AllAtState(snapshot.LifecycleIndexed) {
		if err := chain.Validate(ctx, extras); err != nil {
			return err
		}
		advanced := inv.AdvanceFiles(fileNames(extras), snapshot.LifecycleAdvertisable)
		if logger != nil && len(advanced) > 0 {
			logger.Info("[storage-lifecycle] block-range extras advanced",
				"fromBlock", key.FromBlock, "toBlock", key.ToBlock,
				"files", len(advanced))
		}
	}
	return nil
}

// needsValidation reports whether the given subset is fully at
// LifecycleIndexed (or beyond) AND at least one file is still below
// LifecycleAdvertisable. False for empty subsets.
func needsValidation(files []*snapshot.FileEntry) bool {
	if len(files) == 0 {
		return false
	}
	someBelowAdvertisable := false
	for _, f := range files {
		if f.State < snapshot.LifecycleIndexed {
			return false
		}
		if f.State < snapshot.LifecycleAdvertisable {
			someBelowAdvertisable = true
		}
	}
	return someBelowAdvertisable
}

func fileNames(files []*snapshot.FileEntry) []string {
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = f.Name
	}
	return out
}
