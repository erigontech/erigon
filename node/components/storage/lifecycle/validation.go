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
		key := e.StepKey()
		if key.IsZero() {
			if inv.AdvanceTo(e.Name, snapshot.LifecycleAdvertisable) && logger != nil {
				logger.Info("[storage-lifecycle] advanced", "file", e.Name, "to", "Advertisable")
			}
			return nil
		}

		group := inv.FilesAtStep(key)
		if len(group.Files) == 0 {
			return nil
		}

		// Block-domain steps (empty Domain) advance only after a
		// commitment-derived (step, block) binding covers their block
		// range. Without that binding the publisher can't attest, and
		// the consumer can't verify, that the block range corresponds
		// to a known canonical step. The binding is registered by
		// CommitmentDomainValidator when commitment.kv steps batch-
		// validate. Until then, block files wait at Indexed —
		// returning nil here yields no error, no quarantine, just
		// "try again next sweep / ChangeSet".
		//
		// FileEntry.ToStep is in block-units for block files (per
		// snaptype.ParseFileName's *1000 multiplier), so we can pass
		// it directly to BlockToStep.
		if key.Domain == "" {
			if _, hasBinding := inv.BlockToStep(key.ToStep); !hasBinding {
				return nil
			}
		}

		// Pass 1: minimum subset, if any.
		minimum := group.Minimum()
		if needsValidation(minimum) {
			minGroup := snapshot.StepGroup{Key: key, Files: minimum}
			if err := chain.Validate(ctx, minGroup); err != nil {
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
		extras := group.Extras()
		if needsValidation(extras) && group.AllAtState(snapshot.LifecycleIndexed) {
			extrasGroup := snapshot.StepGroup{Key: key, Files: extras}
			if err := chain.Validate(ctx, extrasGroup); err != nil {
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
