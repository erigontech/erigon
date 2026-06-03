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

package storage

import (
	"context"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
)

// StateAggregator is the subset of db/state.Aggregator that the storage
// Provider depends on. Depending on this interface rather than the concrete
// *state.Aggregator lets the p2p_integration harness inject a mock whose
// accessor builds are no-op-success, so the real Provider lifecycle (driver
// + orchestrator + validators) can run against synthetic fixtures that the
// real index-builder would reject. *state.Aggregator satisfies it structurally.
type StateAggregator interface {
	Files() []string
	OpenFolder() error
	BuildMissedAccessors(ctx context.Context, workers int) error
	LockCollation()
	UnlockCollation()

	// StepSize is the number of txNums per aggregator step — needed by
	// the snapshot-trim sub-op in Provider.Unwind to translate a
	// toBlock target into a step boundary so state files (.kv / .v /
	// .ef / .efi / .kvi, all step-indexed) can be classified against
	// the block boundary. Production *state.Aggregator satisfies this
	// structurally; mocks add a return-constant stub.
	StepSize() uint64

	// WipeWritableShadowPast clears every writable-domain MDBX entry
	// (accounts / storage / code / commitment + standalone IIs) whose
	// coordinate falls past lastTxNum. Mode-B SetHead calls it as part
	// of the DB-reset sub-op to reach cold-start-equivalence: writable
	// shadow holds nothing newer than the snapshot-trimmed file tip.
	// Production *state.Aggregator satisfies this structurally; mocks
	// implement it as a no-op (the harness has no real state).
	WipeWritableShadowPast(ctx context.Context, tx kv.TemporalRwTx, lastTxNum uint64) error

	// DomainCompression returns the per-domain seg.FileCompression
	// used for the domain's .kv primary files. Mode-B's boundary-step
	// regeneration (Phase 3) needs this so the rewritten .kv matches
	// the original's wire format. Production *state.Aggregator
	// returns a.Cfg(domain).Compression; mocks return CompressNone.
	DomainCompression(domain kv.Domain) seg.FileCompression
}
