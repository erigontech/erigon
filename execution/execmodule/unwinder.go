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

package execmodule

import (
	"context"

	"github.com/erigontech/erigon/db/kv"
)

// Unwinder is the seam between SetHead's existing exec-stage unwind
// pipeline and the storage-layer admin unwind primitive (production:
// *node/components/storage.Provider). Defined here as a narrow
// interface so execution/execmodule has no direct dependency on the
// storage package; the production wiring in node/eth/backend.go uses
// a thin adapter to satisfy this interface.
//
// SetHead calls Unwinder *after* its existing RunUnwind pass has
// rolled the temporal db and aggregator state to toBlock. The
// Unwinder is responsible for the storage-layer sub-ops that the
// exec-stage path cannot handle on its own — commitment-entry write
// at an arbitrary block, and (once implemented) snapshot file trim
// past toBlock.
//
// Nil-valued unwinder: SetHead skips the call entirely, preserving
// the pre-Provider.Unwind behavior. This is the case for the
// execmoduletester harness, which does not stand up a storage
// Provider; production wires the real adapter via backend.go.
type Unwinder interface {
	// BlockAligned reports whether the chain was configured with
	// --snap.block-aligned-boundaries. SetHead uses this to decide
	// whether mode B (past-diffset arbitrary-block admin unwind) is
	// engagable on this chain. On non-aligned chains mode B never
	// runs and SetHead keeps the legacy CanUnwindToBlockNum
	// rejection for deep targets.
	BlockAligned() bool

	// Unwind runs the storage-layer admin unwind sub-ops at toBlock.
	// Only invoked by SetHead in mode B (toBlock past the diffset
	// window AND aligned mode on). See
	// docs/plans/20260525-admin-sethead-unwind-design.md.
	Unwind(ctx context.Context, toBlock uint64, args UnwindArgs) error
}

// UnwindArgs carries the inputs SetHead supplies to the Unwinder.
// Mirrors storage.UnwindOpts shape; the adapter at the wiring point
// translates between the two so neither package leaks into the other.
//
// Mode B as implemented today (snapshot-trim + DB-reset, no
// commitment write) only needs a writable temporal tx. The
// commitment-recompute path (next commit) will add the fields it
// requires back here.
type UnwindArgs struct {
	// Tx is the writable temporal transaction the storage-layer
	// sub-ops run inside. SetHead owns its lifecycle (the Unwinder
	// does NOT commit; control returns to SetHead which commits).
	Tx kv.TemporalRwTx
}
