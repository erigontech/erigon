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

// BuildOnBatchValidation returns a Handler that runs a per-step
// (StepChain) validation: when an entry reaches LifecycleIndexed, the
// handler waits until ALL files in the entry's step group are also at
// LifecycleIndexed (or beyond); then runs the StepChain across the
// group; on success atomically advances every file in the group to
// LifecycleAdvertisable.
//
// This is the production wiring for V2 publication coherence. The
// publisher's chain.toml entries are gated on Advertisable, so a step
// that fails batch validation never gets advertised — and the
// step-sibling rule means a `.seg` whose `.idx` is broken or missing
// never advances on its own.
//
// Singletons (caplin / meta / salt — files with zero StepKey) bypass
// the batch path: they advance individually since they have no
// step-siblings. This preserves the existing behaviour for non-stepped
// files.
//
// Idempotence: the handler may be called for multiple files in the
// same step in one sweep cycle. Whichever fires first runs validation
// + advances the step; subsequent calls see the files already past
// LifecycleIndexed and short-circuit (the driver's dispatch only
// fires OnValidation when state == LifecycleIndexed).
//
// On step-incomplete (some siblings still below Indexed): handler
// returns nil — no error, no advance, sweep retries next cycle.
// On chain failure: handler returns the wrapped error; driver logs at
// Debug and increments the file's failure counter.
//
// logger may be nil; on successful step advance the handler emits an
// Info log "step advanced" naming the step key + file count.
func BuildOnBatchValidation(chain validation.StepChain, inv *snapshot.Inventory, logger log.Logger) Handler {
	return func(ctx context.Context, e *snapshot.FileEntry) error {
		key := e.StepKey()
		if key.IsZero() {
			// Non-stepped singleton: advance directly.
			if inv.AdvanceTo(e.Name, snapshot.LifecycleAdvertisable) && logger != nil {
				logger.Info("[storage-lifecycle] advanced", "file", e.Name, "to", "Advertisable")
			}
			return nil
		}

		group := inv.FilesAtStep(key)
		if len(group.Files) == 0 || !group.AllAtState(snapshot.LifecycleIndexed) {
			// Step incomplete: at least one sibling not yet at Indexed.
			// Wait for next sweep / ChangeSet.
			return nil
		}

		if err := chain.Validate(ctx, group); err != nil {
			return err
		}

		advanced := inv.AdvanceStep(key, snapshot.LifecycleAdvertisable)
		if logger != nil && len(advanced) > 0 {
			logger.Info("[storage-lifecycle] step advanced",
				"from", key.FromStep, "to", key.ToStep, "domain", string(key.Domain),
				"files", len(advanced))
		}
		return nil
	}
}
