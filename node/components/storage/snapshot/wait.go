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

package snapshot

import (
	"context"
	"errors"

	"github.com/erigontech/erigon/node/components/storage/views"
)

// ErrNotFound is returned when a read or wait targets a file that is not
// declared in inventory at all. Distinct from views.ErrPending, which is
// returned when a file IS declared but did not become ready before ctx
// expired.
var ErrNotFound = errors.New("inventory: file not declared")

// WaitForReady blocks until the named file becomes Ready in live
// inventory state, or returns:
//
//   - nil when the file becomes Ready (Local=true, plus Advertisable=true
//     when requireAdvertisable is set).
//   - ErrNotFound when the file is not declared at the moment of call
//     (no Subscribe before initial check necessary — undeclared files
//     don't transition to declared via the same code path; AddFile is
//     a separate event path).
//   - views.ErrPending when ctx expires while the file is still not
//     ready.
//
// This is the building block for the wait-or-pending contract that
// Aggregator and RoSnapshots will eventually use internally on read-miss
// in their visible set. Callers pass the read's ctx so the wait inherits
// the request deadline.
//
// Semantics intentionally avoid distinguishing "transient not-found"
// (file just removed during wait) from ErrNotFound at start: if the
// file is declared at start, we wait on it; if removal happens during
// the wait, we treat that as a non-arriving Pending and let ctx decide
// when to give up. The alternative (returning a removal error mid-wait)
// would surface implementation-internal lifecycle to callers.
func (inv *Inventory) WaitForReady(ctx context.Context, name string, requireAdvertisable bool) error {
	// Subscribe BEFORE the initial check to close the lost-update race:
	// without this ordering, a concurrent MarkLocal between GetByName
	// and Subscribe would leave us waiting forever on a file that's
	// already ready.
	sub, unsub := inv.Subscribe()
	defer unsub()

	e, ok := inv.GetByName(name)
	if !ok {
		return ErrNotFound
	}
	if isReady(e, requireAdvertisable) {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return views.ErrPending
		case _, ok := <-sub:
			if !ok {
				return views.ErrPending
			}
			e, ok := inv.GetByName(name)
			if !ok {
				// File un-declared during wait. Don't return ErrNotFound
				// (the wait STARTED with the file declared); let ctx
				// decide the give-up moment.
				continue
			}
			if isReady(e, requireAdvertisable) {
				return nil
			}
		}
	}
}

// isReady is the ready-check used by WaitForReady. Local=true is the
// minimum; requireAdvertisable additionally gates on the producer-side
// validation flag.
func isReady(e *FileEntry, requireAdvertisable bool) bool {
	if !e.Local {
		return false
	}
	if requireAdvertisable && !e.Advertisable {
		return false
	}
	return true
}
