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

package harness

import (
	"context"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// ErrNotFound is re-exported from snapshot for scenario assertions; the
// stub returns whatever WaitForReady returns, including snapshot.ErrNotFound
// for the Missing case.
var ErrNotFound = snapshot.ErrNotFound

// StubReadHandle simulates a read against (Inventory + optional held
// view) embodying the wait-or-pending contract:
//
//   - File present in held view AND ready → return Ready immediately.
//   - File declared in live inventory AND ready → return Ready immediately.
//   - File declared but not ready → wait on ChangeSet until ready or
//     ctx deadline. On deadline → ErrPending.
//   - File not declared anywhere → ErrNotFound.
//
// "Ready" means Local=true and (when RequireAdvertisable is set)
// Advertisable=true. The handle stands in for what Aggregator and
// RoSnapshots will eventually do natively when they grow an Inventory
// dependency at view construction.
type StubReadHandle struct {
	Inventory *snapshot.Inventory

	// RequireAdvertisable: when true, files with Local=true but
	// Advertisable=false count as Pending (mirrors the producer-gate
	// scenario).
	RequireAdvertisable bool
}

// Read attempts to read the named file. view may be nil for tests that
// don't exercise held-view semantics. Held views with a captured ready
// clone short-circuit the wait — the refcount discipline keeps the file
// readable through transitions. Otherwise the read delegates to
// Inventory.WaitForReady, which is the canonical wait-or-pending
// implementation that real read handles will reuse.
func (h *StubReadHandle) Read(ctx context.Context, view snapshot.HeldView, name string) (string, error) {
	if view != nil {
		if e, ok := view.Get(name); ok && readyEntry(e, h.RequireAdvertisable) {
			return e.Name, nil
		}
	}
	if err := h.Inventory.WaitForReady(ctx, name, h.RequireAdvertisable); err != nil {
		return "", err
	}
	e, _ := h.Inventory.GetByName(name)
	return e.Name, nil
}

// readyEntry mirrors snapshot.isReady (which is private to the snapshot
// package) for the held-view short-circuit. Real read handles that have
// access to the snapshot package's internals will use the package-private
// helper directly; consumers outside snapshot reproduce the check here.
func readyEntry(e *snapshot.FileEntry, requireAdvertisable bool) bool {
	if !e.Local {
		return false
	}
	if requireAdvertisable && !e.Advertisable {
		return false
	}
	return true
}
