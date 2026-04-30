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

// Package views holds the contract types shared across the storage view
// layer — the typed sentinel errors, availability vocabulary, and held-view
// interface that read handles (Aggregator, RoSnapshots) and the Inventory
// metadata registry coordinate around.
//
// See docs/plans/20260430-storage-views-spec.md for the full contract.
package views

import (
	"errors"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// ErrPending is returned by reads when a file is declared in inventory and
// expected to land, but has not become Ready within the read's ctx
// deadline. Callers test with errors.Is(err, ErrPending).
//
// A read returning ErrPending is a soft fail: the caller can retry with a
// longer deadline, or surface the condition upward as "available later".
// It is distinct from "file is not declared at all", which returns the
// underlying not-found error class.
var ErrPending = errors.New("storage view: read pending — declared but not local within deadline")

// AvailabilityState is the three-state world the contract surfaces to
// planners. Read handles do not return it directly (they return values or
// ErrPending); planners query it via the forward-availability projection.
type AvailabilityState int

const (
	// Ready: file is local and (where applicable) advertisable. Reads return
	// values immediately.
	Ready AvailabilityState = iota
	// Pending: file is declared in inventory but not yet local (or not yet
	// advertisable). Reads block until ctx deadline; planners observe the
	// state without blocking.
	Pending
	// Missing: file is not declared in inventory and will not arrive without
	// operator action. Reads return the not-found error class.
	Missing
)

func (a AvailabilityState) String() string {
	switch a {
	case Ready:
		return "Ready"
	case Pending:
		return "Pending"
	case Missing:
		return "Missing"
	}
	return "unknown"
}

// ChangeSet describes an atomic transition emitted by Inventory. A
// transition fires for AddFile, RemoveFile, ReplaceWithMerge,
// MarkAdvertisable, and trust/local promotions.
//
// Subscribers use ChangeSet primarily as a wake signal: re-check the
// entries you care about against the live Inventory after each receive.
// The Files field lists names touched by this transition for fine-grained
// dispatch when receivers want it; it is not a delta replay.
type ChangeSet struct {
	// Files names every entry the transition touched. Empty slice means a
	// transition with no per-file effect (rare; reserved for batched
	// trust/policy updates).
	Files []string
}

// HeldView is an immutable snapshot of Inventory state. Mutations to
// Inventory after View() returns do not affect a held view. Eviction of
// files referenced by held views is deferred until the last referencing
// view closes — analogous to db/snapshotsync.RoSnapshots' segment
// refcount discipline.
type HeldView interface {
	// Files returns the FileEntry slice for the given domain captured at
	// View() time. The returned entries are clones — mutating them does
	// not affect the inventory or other held views.
	Files(domain snapshot.Domain) []*snapshot.FileEntry

	// BlockFiles returns the captured block-file slice (entries with empty
	// Domain and Kind=KindKV).
	BlockFiles() []*snapshot.FileEntry

	// Get returns the captured FileEntry for the named file, or (nil, false)
	// if the view did not reference it at construction.
	Get(name string) (*snapshot.FileEntry, bool)

	// RefCount returns the current held-view refcount for the named file.
	// Tests use this to verify deferred-eviction; production code does not
	// rely on it.
	RefCount(name string) int

	// Close releases the view. Decrements refcount on each captured file;
	// any deferred-eviction unlinks fire when refcount drops to zero.
	// Multi-call safe.
	Close()
}

// HeldViewProvider is the interface implemented by Inventory once item #1
// lands. The harness's stub implements it for testing the contract before
// the real Inventory grows the methods.
type HeldViewProvider interface {
	// View captures the current Inventory state and returns a HeldView.
	View() HeldView

	// Subscribe returns a channel receiving ChangeSet notifications and an
	// unsubscribe function. The unsubscribe closes the channel; calls on
	// the channel after unsubscribe return zero value with ok=false.
	Subscribe() (<-chan ChangeSet, func())
}
