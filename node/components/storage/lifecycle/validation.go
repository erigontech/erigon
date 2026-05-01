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
// The handler runs the validation Chain against the entry; on success
// it advances to LifecycleAdvertisable; on failure it returns the
// wrapped error so Driver.dispatch can log at Debug and the next
// sweep retries.
//
// Validation is the extension point per
// app-integration-review-items.md item #3: the chain is a plain
// []Validator slice composed at construction time. Adding a new
// validator means writing one struct + two methods in any package
// that imports validation; no enum, no central registry, no bridge.
// See node/components/storage/validation/EXTENDING.md for the worked
// example.
//
// chain may be empty — an empty Chain accepts everything, which means
// every Indexed file advances to Advertisable. This is the default
// shape for nodes that do not run producer-side validation.
//
// contentFor may be nil; in that case validators receive nil
// ContentSource, which is permitted for stage-1 validators that don't
// need bytes.
func BuildOnValidation(chain validation.Chain, contentFor ContentSourceFor, inv *snapshot.Inventory) Handler {
	return func(_ context.Context, e *snapshot.FileEntry) error {
		var content validation.ContentSource
		if contentFor != nil {
			content = contentFor(e)
		}
		if err := chain.Validate(e, content); err != nil {
			return err
		}
		inv.AdvanceTo(e.Name, snapshot.LifecycleAdvertisable)
		return nil
	}
}
