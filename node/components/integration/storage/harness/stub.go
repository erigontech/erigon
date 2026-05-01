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
	"errors"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/views"
)

// ErrNotFound — the not-declared (Missing) outcome from a stub read.
// Real read handles return their package-specific not-found error class;
// the stub uses this sentinel so scenarios can assert with errors.Is.
var ErrNotFound = errors.New("stub read: file not declared")

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
// don't exercise held-view semantics.
func (h *StubReadHandle) Read(ctx context.Context, view snapshot.HeldView, name string) (string, error) {
	if view != nil {
		if e, ok := view.Get(name); ok && h.ready(e) {
			return e.Name, nil
		}
	}

	// Subscribe before re-checking live state to avoid lost-update races
	// between "file just landed" and "we just started waiting".
	sub, unsub := h.Inventory.Subscribe()
	defer unsub()

	if e, ok := h.Inventory.GetByName(name); ok && h.ready(e) {
		return e.Name, nil
	}
	if _, ok := h.Inventory.GetByName(name); !ok {
		if view == nil {
			return "", ErrNotFound
		}
		if _, inView := view.Get(name); !inView {
			return "", ErrNotFound
		}
	}

	for {
		select {
		case <-ctx.Done():
			return "", views.ErrPending
		case _, ok := <-sub:
			if !ok {
				return "", views.ErrPending
			}
			if e, ok := h.Inventory.GetByName(name); ok && h.ready(e) {
				return e.Name, nil
			}
		}
	}
}

func (h *StubReadHandle) ready(e *snapshot.FileEntry) bool {
	if !e.Local {
		return false
	}
	if h.RequireAdvertisable && !e.Advertisable {
		return false
	}
	return true
}
