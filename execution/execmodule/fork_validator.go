// Copyright 2022 The Erigon Authors
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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/blockmetrics"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

type BlockTimings [2]time.Duration

const (
	BlockTimingsValidationIndex    = 0
	BlockTimingsFlushExtendingFork = 1
)

const timingsCacheSize = 16

const retainedCandidateLimit = 4

type validatedCandidate struct {
	domains       *execctx.SharedDomains
	notifications *Accumulation
}

type ForkValidator struct {
	candidates    *lru.Cache[common.Hash, *validatedCandidate]
	maxReorgDepth uint64
	// pipeline executor used for fork validation (ValidateBlock).
	executor    *PipelineExecutor
	blockReader dbservices.FullBlockReader
	// this is the current point where we processed the chain so far.
	currentHeight uint64
	// block hashes that are deemed valid
	validHashes *lru.Cache[common.Hash, bool]

	ctx context.Context

	// we want fork validator to be thread safe so let
	lock sync.Mutex

	timingsCache *lru.Cache[common.Hash, BlockTimings]

	blockMetricsCache *lru.Cache[common.Hash, *blockmetrics.Record]
}

func newForkValidator(ctx context.Context, currentHeight uint64, executor *PipelineExecutor, blockReader dbservices.FullBlockReader, maxReorgDepth uint64, slowBlockThreshold *time.Duration) *ForkValidator {
	validHashes, err := lru.New[common.Hash, bool]("validHashes", int(maxReorgDepth)*8)
	if err != nil {
		panic(err)
	}

	timingsCache, err := lru.New[common.Hash, BlockTimings]("timingsCache", timingsCacheSize)
	if err != nil {
		panic(err)
	}

	var blockMetricsCache *lru.Cache[common.Hash, *blockmetrics.Record]
	if slowBlockThreshold != nil {
		dbg.KVReadLevelledMetrics = true
		blockMetricsCache, err = lru.New[common.Hash, *blockmetrics.Record]("blockMetricsCache", timingsCacheSize)
		if err != nil {
			panic(err)
		}
	}
	candidates, err := lru.New[common.Hash, *validatedCandidate]("validatedCandidates", retainedCandidateLimit)
	if err != nil {
		panic(err)
	}
	return &ForkValidator{
		candidates:        candidates,
		executor:          executor,
		currentHeight:     currentHeight,
		blockReader:       blockReader,
		ctx:               ctx,
		validHashes:       validHashes,
		timingsCache:      timingsCache,
		blockMetricsCache: blockMetricsCache,
		maxReorgDepth:     maxReorgDepth,
	}
}

// NotifyCurrentHeight is to be called at the end of the stage cycle and represent the last processed block.
func (fv *ForkValidator) NotifyCurrentHeight(currentHeight uint64) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if fv.currentHeight == currentHeight {
		return
	}
	fv.currentHeight = currentHeight
	fv.clear()
}

func (fv *ForkValidator) HasValidatedState(hash common.Hash) bool {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	return fv.candidates.Contains(hash)
}

func (fv *ForkValidator) MergeExtendingFork(ctx context.Context, tx kv.TemporalTx, sd *execctx.SharedDomains, hash common.Hash, target *Accumulation) error {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	candidate, ok := fv.candidates.Get(hash)
	if !ok {
		return fmt.Errorf("validated state unavailable for %s", hash)
	}
	start := time.Now()
	if err := candidate.domains.FlushPendingUpdates(ctx, tx); err != nil {
		return err
	}
	sdTxNum, _, err := sd.SeekCommitment(ctx, tx)
	if err != nil {
		return err
	}
	otherTxNum, _, err := candidate.domains.SeekCommitment(ctx, tx)
	if err != nil {
		return err
	}
	if overlay := candidate.domains.BlockOverlay(); overlay != nil {
		sequence, err := tx.ReadSequence(kv.EthTx)
		if err != nil {
			return err
		}
		candidateSequence, err := overlay.NewReadView(tx).ReadSequence(kv.EthTx)
		if err != nil {
			return err
		}
		// Later insertions may have allocated transaction IDs since validation.
		if err := overlay.ResetSequence(kv.EthTx, max(sequence, candidateSequence)); err != nil {
			return err
		}
	}
	if err := sd.Merge(ctx, sdTxNum, candidate.domains, otherTxNum); err != nil {
		return err
	}
	timings, _ := fv.timingsCache.Get(hash)
	timings[BlockTimingsFlushExtendingFork] = time.Since(start)
	fv.timingsCache.Add(hash, timings)
	candidate.notifications.CopyAndReset(target)
	fv.candidates.Remove(hash)
	candidate.domains.Close()
	return nil
}

type HasDiff interface {
	Diff() (*membatchwithdb.MemoryDiff, error)
}

// ValidatePayload checks a payload against canonical state. It validates a
// fork after staging an unwind to the common canonical ancestor and accepts a
// payload when required chain data is unavailable. Before a fork unwind it
// invokes ensureReadAheadSuspended, which must idempotently acquire a
// caller-owned suspension lasting until validation stops reading staged state;
// an acquisition error aborts validation before the unwind.
func (fv *ForkValidator) ValidatePayload(ctx context.Context, sd *execctx.SharedDomains, tx kv.TemporalRwTx, header *types.Header, body *types.RawBody, ensureReadAheadSuspended func() error, logger log.Logger) (status engine_types.EngineStatus, latestValidHash common.Hash, validationError error, criticalError error) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	retained := false
	defer func() {
		if !retained && sd != nil {
			sd.Close()
		}
	}()
	if fv.executor == nil {
		status = engine_types.AcceptedStatus
		return
	}
	hash := header.Hash()
	number := header.Number.Uint64()

	// If the block is stored within the side fork it means it was already validated.
	_, known := fv.validHashes.Get(hash)
	if _, ok := fv.candidates.Get(hash); known || ok {
		status = engine_types.ValidStatus
		latestValidHash = hash
		return
	}

	// if the block is not in range of maxReorgDepth from head then we do not validate it.
	if math.AbsoluteDifference(fv.currentHeight, header.Number.Uint64()) > fv.maxReorgDepth {
		status = engine_types.AcceptedStatus
		return
	}
	var foundCanonical bool
	foundCanonical, criticalError = fv.blockReader.IsCanonical(fv.ctx, tx, hash, number)
	if criticalError != nil {
		return
	}
	if foundCanonical {
		status = engine_types.ValidStatus
		latestValidHash = header.Hash()
		return
	}
	// Let's assemble the side fork backwards
	currentHash := header.ParentHash
	unwindPoint := number - 1
	foundCanonical, criticalError = fv.blockReader.IsCanonical(fv.ctx, tx, currentHash, unwindPoint)
	if criticalError != nil {
		return
	}

	logger.Debug("Execution ForkValidator.ValidatePayload", "foundCanonical", foundCanonical, "currentHash", currentHash, "unwindPoint", unwindPoint)

	var bodiesChain []*types.RawBody
	var headersChain []*types.Header
	for !foundCanonical {
		var (
			header *types.Header
			body   *types.Body
		)
		header, criticalError = fv.blockReader.Header(fv.ctx, tx, currentHash, unwindPoint)
		if criticalError != nil {
			return
		}
		if header == nil {
			// We miss some components so we did not check validity.
			status = engine_types.AcceptedStatus
			return
		}
		body, criticalError = fv.blockReader.BodyWithTransactions(fv.ctx, tx, currentHash, unwindPoint)
		if criticalError != nil {
			return
		}
		if body == nil {
			criticalError = fmt.Errorf("found chain gap in block body at hash %s, number %d", currentHash, unwindPoint)
			return
		}

		headersChain = append([]*types.Header{header}, headersChain...)
		bodiesChain = append([]*types.RawBody{body.RawBody()}, bodiesChain...)

		currentHash = header.ParentHash
		unwindPoint = header.Number.Uint64() - 1
		foundCanonical, criticalError = fv.blockReader.IsCanonical(fv.ctx, tx, currentHash, unwindPoint)
		if criticalError != nil {
			return
		}
		logger.Debug("Execution ForkValidator.ValidatePayload", "foundCanonical", foundCanonical, "currentHash", currentHash, "unwindPoint", unwindPoint)
	}
	// Do not set an unwind point if we are already there.
	if unwindPoint == fv.currentHeight {
		unwindPoint = 0
	}
	if unwindPoint != 0 {
		if criticalError = ensureReadAheadSuspended(); criticalError != nil {
			return
		}
	}
	notifications := fv.executor.ValidationNotifications()
	notifications.Accumulator.Reset(0)
	notifications.RecentReceipts.Clear()
	status, latestValidHash, validationError, criticalError =
		fv.validateAndStorePayload(fv.ctx, sd, tx, header, body, unwindPoint, headersChain, bodiesChain)
	if status == engine_types.ValidStatus && criticalError == nil && validationError == nil {
		owned := NewAccumulation()
		notifications.Accumulator.CopyAndReset(owned.Accumulator)
		notifications.RecentReceipts.CopyAndReset(owned.RecentReceipts)
		if fv.candidates.Len() == retainedCandidateLimit {
			_, oldest, _ := fv.candidates.RemoveOldest()
			oldest.domains.Close()
		}
		if overlay := sd.BlockOverlay(); overlay != nil {
			overlay.DetachDB()
		}
		fv.candidates.Add(hash, &validatedCandidate{domains: sd, notifications: owned})
		retained = true
	}

	return
}

func (fv *ForkValidator) clear() {
	for _, candidate := range fv.candidates.Values() {
		candidate.domains.Close()
	}
	fv.candidates.Purge()
}

// ClearWithUnwind releases all retained candidate states.
func (fv *ForkValidator) ClearWithUnwind() {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	fv.clear()
}

// validateAndStorePayload validate and store a payload fork chain if such chain results valid.
func (fv *ForkValidator) validateAndStorePayload(ctx context.Context, sd *execctx.SharedDomains, tx kv.TemporalRwTx, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody,
) (status engine_types.EngineStatus, latestValidHash common.Hash, validationError error, criticalError error) {
	start := time.Now()
	headersChain = append(headersChain, header)
	bodiesChain = append(bodiesChain, body)
	hash := header.Hash()
	number := header.Number.Uint64()
	var beforeIO blockmetrics.Sample
	if fv.blockMetricsCache != nil {
		sd.TakeCommitmentTime() // discard anything left by an earlier block
		beforeIO = blockmetrics.Take(sd.Metrics(), sd.NonExecMetrics())
	}
	if err := fv.executor.ValidateBlock(ctx, sd, tx, unwindPoint, headersChain, bodiesChain); err != nil {
		if errors.Is(err, rules.ErrInvalidBlock) {
			validationError = err
		} else {
			criticalError = fmt.Errorf("validateAndStorePayload: %w", err)
			return
		}
	}
	validation := time.Since(start)
	fv.timingsCache.Add(hash, BlockTimings{validation, 0})
	fv.recordBlockMetrics(sd, header, body, hash, &beforeIO, len(headersChain), validation, fv.executor.lastValidationExecStageTiming())

	latestValidHash = hash
	if validationError != nil {
		var latestValidNumber uint64
		latestValidNumber, criticalError = stages.GetStageProgress(tx, stages.Execution)

		if criticalError != nil {
			return
		}
		var ok bool
		latestValidHash, ok, criticalError = fv.blockReader.CanonicalHash(fv.ctx, tx, latestValidNumber)
		if criticalError != nil {
			return
		}
		if !ok {
			criticalError = fmt.Errorf("canonical hash not found: %d", latestValidNumber)
			return
		}
		status = engine_types.InvalidStatus
		return
	}
	fv.validHashes.Add(hash, true)

	_, criticalError = rawdb.WriteRawBodyIfNotExists(tx, hash, number, body)
	if criticalError != nil {
		return //nolint:nilnesserr
	}

	status = engine_types.ValidStatus
	return
}

// GetTimings returns the timings of the last block validation.
func (fv *ForkValidator) GetTimings(hash common.Hash) BlockTimings {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if timings, ok := fv.timingsCache.Get(hash); ok {
		return timings
	}
	return BlockTimings{}
}

func (fv *ForkValidator) recordBlockMetrics(sd *execctx.SharedDomains, header *types.Header, body *types.RawBody, hash common.Hash, beforeIO *blockmetrics.Sample, blocksValidated int, validation, execStage time.Duration) {
	if fv.blockMetricsCache == nil {
		return
	}
	// One call validates the whole fork; the header describes only the tip.
	if blocksValidated != 1 {
		sd.TakeCommitmentTime()
		return
	}
	stateHash := sd.TakeCommitmentTime()
	rec := &blockmetrics.Record{
		Number:     header.Number.Uint64(),
		Hash:       hash,
		GasUsed:    header.GasUsed,
		StateHash:  stateHash,
		Validation: validation,
	}
	if body != nil {
		rec.TxCount = len(body.Transactions)
	}
	rec.Accounts, rec.Storage, rec.Code, rec.CountersValid = blockmetrics.Take(sd.Metrics(), sd.NonExecMetrics()).Since(*beforeIO)
	rec.Execution = max(execStage-stateHash, 0)
	fv.blockMetricsCache.Add(hash, rec)
}

func (fv *ForkValidator) TakeBlockMetrics(hash common.Hash) *blockmetrics.Record {
	if fv.blockMetricsCache == nil {
		return nil
	}
	if rec, ok := fv.blockMetricsCache.Get(hash); ok {
		fv.blockMetricsCache.Remove(hash)
		return rec
	}
	return nil
}

func (fv *ForkValidator) ValidatedState(hash common.Hash) *execctx.SharedDomains {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	candidate, ok := fv.candidates.Peek(hash)
	if !ok {
		return nil
	}
	return candidate.domains
}
