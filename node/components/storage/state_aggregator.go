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
	"time"

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
	BuildMissedAccessors(ctx context.Context, workers int, opts ...kv.BuildAccessorsOption) error
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

	// Unwind is the cross-cutting aggregator-side unwind step:
	// invalidates every aggregator-lifetime cache keyed by txN past
	// the unwind target. SharedDomains.Unwind reaches the same surface
	// via commitment.AggregatorUnwindHandler; mode-B Provider.Unwind
	// calls this directly. Anything added to (*Aggregator).Unwind
	// applies to both unwind paths automatically.
	Unwind(txN uint64)

	// SetUnwindInProgress gates background build+merge during a
	// mode-B unwind.
	SetUnwindInProgress(v bool)

	// WaitForBuildAndMergeQuiescence blocks until in-flight
	// build/merge goroutines exit, or the timeout elapses.
	WaitForBuildAndMergeQuiescence(timeout time.Duration) error

	// DomainKVFilePathV4 returns the v4.0 raw-txnum-named .kv path for
	// the given domain. Mode-C unwind emits its boundary-step truncate
	// under this name so the file's advertised endTxN matches its
	// as-of-lastTxN content (rather than lying via the step-boundary
	// convention).
	DomainKVFilePathV4(domain kv.Domain, fromTxN, toTxN uint64) string

	// BuildKVAccessors builds the .bt/.kvei/.kvi sidecars for a
	// freshly-written domain .kv file. dataPath is the physical .kv
	// location (may carry a .regen suffix during Provider.Unwind);
	// finalPath is the eventual naming the accessor filenames get
	// derived from. Concrete Aggregator opens the .kv at dataPath as a
	// seg.Decompressor and calls the domain's existing accessor-build
	// primitives (BuildBtreeIndexWithDecompressor for BT+existence,
	// buildHashMapAccessorAt for hashmap) so the domain's own
	// per-accessor mask dictates what gets built.
	//
	// Without this, mode-C v4 .kv emits ship with no accessors and are
	// silently excluded from every DomainRoTx visible set — forward-exec
	// reads bypass v4 and return pre-window state (leg-M v1 2026-08-06
	// iter 1 mode_b gas mismatch −135,654 root cause). Satisfies the
	// AccessorBuilder interface in the storage package so the emit
	// functions can call it directly.
	BuildKVAccessors(ctx context.Context, domain kv.Domain, dataPath, finalPath string) error
}
