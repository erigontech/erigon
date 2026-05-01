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
)

// IndexBuilder produces the dependent index files for a primary file
// at LifecycleDownloaded. Implementations wrap the existing build code
// from db/snapshotsync (BuildMissedIndices) and db/state (accessor
// builders); the driver invokes the wrapper without knowing the
// underlying details.
//
// Behaviour contract:
//   - On nil return: the caller can assume the primary's dependencies
//     have been emitted to disk and either are or will shortly be
//     reflected in the inventory (via OnFilesChange / explicit
//     AddFile by the production wiring).
//   - On error return: the build did not produce the deps. The driver
//     leaves the entry at LifecycleDownloaded; the next sweep retries.
//
// Implementations may batch internally — e.g. a single BuildMissedIndices
// invocation that handles many files in one pass, with per-file callers
// verifying their own results afterward. Step 4 does not specify the
// batching strategy; the production wrapper in step 6 chooses.
type IndexBuilder interface {
	BuildMissedIndices(ctx context.Context, primary *snapshot.FileEntry) error
}

// BuildOnIndexing returns a Handler suitable for Driver.OnIndexing.
// The handler invokes builder.BuildMissedIndices, then verifies the
// primary's Dependencies are all Local in the inventory. On full
// dependency satisfaction, it advances the entry to LifecycleIndexed.
//
// If a dependency is still missing after the build (e.g. because the
// production OnFilesChange path hasn't yet propagated the new file
// into the inventory), the handler returns nil — the next sweep or
// ChangeSet event will re-check. This is the standard idempotent
// retry pattern; failure to advance is not an error.
//
// Builder errors propagate to the caller, which logs them at Debug
// per Driver.dispatch's contract.
func BuildOnIndexing(builder IndexBuilder, inv *snapshot.Inventory) Handler {
	return func(ctx context.Context, e *snapshot.FileEntry) error {
		if err := builder.BuildMissedIndices(ctx, e); err != nil {
			return err
		}
		for _, depName := range e.Dependencies {
			dep, ok := inv.GetByName(depName)
			if !ok || !dep.Local {
				// Dep not yet propagated; wait for next sweep.
				return nil
			}
		}
		// All deps present (or no deps required) → advance.
		inv.AdvanceTo(e.Name, snapshot.LifecycleIndexed)
		return nil
	}
}
