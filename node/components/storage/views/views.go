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

// Package views holds the cross-cutting types shared by storage read
// paths — the typed sentinel error returned on soft-fail, the
// availability vocabulary the forward-availability projection surfaces
// to planners, and the small Awaiter interface read handles depend on
// for wait-on-miss behaviour.
//
// Held-view types (HeldView, ChangeSet, InventoryView) live in the
// snapshot package alongside Inventory itself, because they describe
// Inventory's surface and would create an import cycle if defined here.
//
// See docs/plans/20260430-storage-views-spec.md for the full contract,
// and docs/plans/20260501-readhandle-integration.md for the read-handle
// wiring shape.
package views

import (
	"context"
	"errors"
)

// ErrPending is returned by reads when a file is declared in inventory
// and expected to land, but has not become Ready within the read's ctx
// deadline. Callers test with errors.Is(err, ErrPending).
//
// A read returning ErrPending is a soft fail: the caller can retry with
// a longer deadline, or surface the condition upward as "available
// later". It is distinct from "file is not declared at all", which
// returns the underlying not-found error class.
var ErrPending = errors.New("storage view: read pending — declared but not local within deadline")

// AvailabilityState is the three-state world the contract surfaces to
// planners. Read handles do not return it directly (they return values
// or ErrPending); planners query it via the forward-availability
// projection.
type AvailabilityState int

const (
	// Ready: file is local and (where applicable) advertisable. Reads
	// return values immediately.
	Ready AvailabilityState = iota
	// Pending: file is declared in inventory but not yet local (or not
	// yet advertisable). Reads block until ctx deadline; planners
	// observe the state without blocking.
	Pending
	// Missing: file is not declared in inventory and will not arrive
	// without operator action. Reads return the not-found error class.
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

// Awaiter is the small interface read handles (RoSnapshots,
// Aggregator) depend on at view construction so they can wait for a
// declared-but-not-local file to become Ready on read-miss. The
// interface is single-method and structurally satisfied by
// *snapshot.Inventory — read handles never see the full Inventory
// API; they see only what they need.
//
// Production wiring passes the inventory's Awaiter to RoSnapshots
// and Aggregator via SetAwaiter (the post-construction setter shape;
// see docs/plans/20260501-readhandle-integration.md). CLI tools and
// tests that don't run the snapshot-flow component pass nil — read
// handles fall back to today's hard-fail behaviour on miss.
type Awaiter interface {
	// WaitForReady blocks until the named file becomes Ready (Local
	// AND, when requireAdvertisable is set, Advertisable too) in the
	// inventory; or returns ErrPending when ctx expires; or returns
	// the not-found error class when the file is not declared at the
	// moment of call.
	WaitForReady(ctx context.Context, name string, requireAdvertisable bool) error
}
