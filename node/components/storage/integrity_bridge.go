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
	"fmt"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// IntegrityBridge wraps a single db/integrity.Check function as a
// validation.BatchValidator. The producer-side BatchChain runs these
// at the moment the storage component decides a batch of files is
// candidate for promotion to advertisable.
//
// The dispatch table mirrors the existing `erigon seg integrity` CLI
// (cmd/utils/app/snapshots_cmd.go ~doIntegrity); the bridge moves
// the invocation site from "stopped-node external script" to
// "live-node producer-side gate" without rewriting the checks
// themselves.
//
// Construction is by factory function below — operators wire the
// db/blockReader/cache/sampler from storage.Provider's existing
// fields so the bridge has no per-check setup boilerplate.
type IntegrityBridge struct {
	// Check is the db/integrity Check enum value selecting which
	// underlying function to invoke.
	Check integrity.Check

	// Required for every check (the chain DB). RwDB rather than RoDB
	// because some checks (E3EfFiles, HistoryCheckNoSystemTxs) take
	// the writable interface; the read-only checks accept RwDB
	// transparently.
	DB kv.TemporalRwDB

	// Required for most checks; nil-safe for the few that don't
	// consume it (e.g. StateVerify takes only db).
	BlockReader services.FullBlockReader

	// Sampler config — cheap, can be supplied for every bridge even
	// when the underlying check ignores it.
	SamplerCfg integrity.SamplerCfg

	// Cache speeds up repeated runs for the kvi-flavoured checks
	// (CommitmentKvi, CommitmentKvDeref). May be nil for checks
	// that don't use it.
	Cache *integrity.IntegrityCache

	// FailFast is forwarded to the underlying check. Producer-side
	// gate use typically wants true (any failure aborts the batch);
	// scrubbing tools may set false to collect all failures.
	FailFast bool

	Logger log.Logger

	// FromStep is forwarded to checks that accept a starting step
	// (StateVerify, E3EfFiles). Zero is the default — scan from
	// genesis.
	FromStep uint64
}

// Name implements validation.BatchValidator. Returns the
// db/integrity.Check string so error attribution and log output
// match the CLI tool's terminology.
func (b *IntegrityBridge) Name() string { return string(b.Check) }

// ValidateBatch implements validation.BatchValidator. Dispatches on
// b.Check to the matching db/integrity function. Unknown checks
// (e.g. Bor checks on a non-Bor chain, or newly-added enum values
// the bridge hasn't been updated for) return an error so the
// producer-side gate can surface the misconfiguration rather than
// silently pass.
func (b *IntegrityBridge) ValidateBatch(ctx context.Context) error {
	if b.DB == nil {
		return fmt.Errorf("IntegrityBridge[%s]: nil DB", b.Check)
	}
	logger := b.Logger
	if logger == nil {
		logger = log.Root()
	}

	switch b.Check {
	case integrity.Blocks:
		return integrity.SnapBlocksRead(ctx, b.DB, b.BlockReader, 0, 0, b.FailFast)

	case integrity.HeaderNoGaps:
		return integrity.NoGapsInCanonicalHeaders(ctx, b.DB, b.BlockReader, b.FailFast)

	case integrity.BlocksTxnID:
		// IntegrityTxnID lives on the concrete block reader (not the
		// services.FullBlockReader interface). Unwrap defensively;
		// nil-safe for callers that wired only the interface.
		br, ok := b.BlockReader.(*freezeblocks.BlockReader)
		if !ok {
			return fmt.Errorf("IntegrityBridge[%s]: requires concrete *freezeblocks.BlockReader", b.Check)
		}
		return br.IntegrityTxnID(ctx, b.FailFast)

	case integrity.InvertedIndex:
		return integrity.E3EfFiles(ctx, b.DB, b.FailFast, b.FromStep)

	case integrity.HistoryNoSystemTxs:
		return integrity.HistoryCheckNoSystemTxs(ctx, b.DB, b.BlockReader)

	case integrity.ReceiptsNoDups:
		return integrity.CheckReceiptsNoDups(ctx, b.SamplerCfg, b.DB, b.BlockReader, b.FailFast)

	case integrity.RCacheNoDups:
		return integrity.CheckRCacheNoDups(ctx, b.SamplerCfg, b.DB, b.BlockReader, b.FailFast)

	case integrity.StateProgress:
		return integrity.CheckStateProgress(ctx, b.DB, b.BlockReader, b.FailFast)

	case integrity.CommitmentRoot:
		return integrity.CheckCommitmentRoot(ctx, b.DB, b.BlockReader, b.FailFast, logger)

	case integrity.CommitmentKvi:
		// Kvi check ignores the SamplerCfg ratio — the cache is the
		// speedup mechanism. Mirror the CLI's scCopy.SampleRatio = 0.
		scCopy := b.SamplerCfg
		scCopy.SampleRatio = 0
		return integrity.CheckCommitmentKvi(ctx, scCopy, b.DB, b.Cache, b.FailFast, logger)

	case integrity.CommitmentKvDeref:
		// Deprecated per integrity_action_type.go; bridge supports
		// it for completeness but the chain shouldn't include it.
		return integrity.CheckCommitmentKvDeref(ctx, b.DB, b.Cache, b.FailFast, logger)

	case integrity.CommitmentHistVal:
		// Mirror the CLI's slow-check sample-ratio reduction.
		scCopy := b.SamplerCfg
		scCopy.SampleRatio /= 100
		return integrity.CheckCommitmentHistVal(ctx, scCopy, b.DB, b.BlockReader, b.FailFast, logger)

	case integrity.StateVerify:
		return integrity.CheckStateVerify(ctx, b.DB, b.FailFast, b.FromStep, logger)

	// StateRootVerifyByHistory needs a `to` block computed from
	// state progress (uses state.HasAgg + stages.GetStageProgress
	// fallback — heavy imports the bridge package shouldn't pull
	// in). Defer wiring until the storage component owns the
	// equivalent helper natively. Until then, callers can dispatch
	// this check directly via cmd/utils/app's doIntegrity path.
	case integrity.StateRootVerifyByHistory:
		return fmt.Errorf("IntegrityBridge[%s]: StateRootVerifyByHistory needs the cmd-side stateProgress helper; defer until storage component exports an equivalent", b.Check)

	// Publishable currently calls a CLI-private doPublishable in
	// the CLI command body. The bridge can't dispatch it without
	// either exporting that function or duplicating its body —
	// neither belongs in this commit. Return a structured error so
	// callers know to treat Publishable specially (e.g. invoke the
	// CLI helper directly until the function is exported).
	case integrity.Publishable:
		return fmt.Errorf("IntegrityBridge[%s]: Publishable check is not yet exposed as a library function — invoke via cmd/utils/app or wait for the export that turns doPublishable into integrity.CheckPublishable", b.Check)

	// Bor checks need bor-specific deps the bridge doesn't yet
	// surface; gate at the storage-component level.
	case integrity.BorEvents, integrity.BorSpans, integrity.BorCheckpoints:
		return fmt.Errorf("IntegrityBridge[%s]: Bor checks need bor-store + heimdall.Store dependencies; wire those when the storage component owns them and the bridge will pick them up", b.Check)
	}

	return fmt.Errorf("IntegrityBridge: unknown integrity.Check %q", b.Check)
}

// NewIntegrityChain builds a validation.BatchChain from a list of
// integrity.Check identifiers, all sharing the same DB / BlockReader
// / Cache / SamplerCfg / FailFast configuration. Convenience
// constructor for the producer-side gate's "run all the fast checks
// every time we publish" pattern.
//
// Checks not supported by the bridge (Publishable, StateRootVerifyByHistory,
// Bor*) are included as-is; their ValidateBatch returns the
// structured deferred-support error when called. Callers can filter
// beforehand or handle the per-check errors at chain-run time.
func NewIntegrityChain(
	checks []integrity.Check,
	db kv.TemporalRwDB,
	blockReader services.FullBlockReader,
	cache *integrity.IntegrityCache,
	samplerCfg integrity.SamplerCfg,
	failFast bool,
	fromStep uint64,
	logger log.Logger,
) validation.BatchChain {
	chain := make(validation.BatchChain, 0, len(checks))
	for _, c := range checks {
		chain = append(chain, &IntegrityBridge{
			Check:       c,
			DB:          db,
			BlockReader: blockReader,
			SamplerCfg:  samplerCfg,
			Cache:       cache,
			FailFast:    failFast,
			FromStep:    fromStep,
			Logger:      logger,
		})
	}
	return chain
}
