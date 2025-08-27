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

package engine_helpers

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
)

type BlockTimings [2]time.Duration

const (
	BlockTimingsValidationIndex    = 0
	BlockTimingsFlushExtendingFork = 1
)

const timingsCacheSize = 16

// the maximum point from the current head, past which side forks are not validated anymore.
const maxForkDepth = 32 // 32 slots is the duration of an epoch thus there cannot be side forks in PoS deeper than 32 blocks from head.

type validatePayloadFunc func(wrap.TxContainer, *types.Header, *types.RawBody, uint64, []*types.Header, []*types.RawBody, *shards.Notifications) error

type ForkValidator struct {
	// current memory batch containing chain head that extend canonical fork.
	sharedDom *state.SharedDomains
	// notifications accumulated for the extending fork
	extendingForkNotifications *shards.Notifications
	// hash of chain head that extend canonical fork.
	extendingForkHeadHash common.Hash
	extendingForkNumber   uint64
	// this is the function we use to perform payload validation.
	validatePayload validatePayloadFunc
	blockReader     services.FullBlockReader
	// this is the current point where we processed the chain so far.
	currentHeight uint64
	tmpDir        string
	// block hashes that are deemed valid
	validHashes *lru.Cache[common.Hash, bool]

	ctx context.Context

	// we want fork validator to be thread safe so let
	lock sync.Mutex

	timingsCache *lru.Cache[common.Hash, BlockTimings]
}

func NewForkValidatorMock(currentHeight uint64) *ForkValidator {
	validHashes, err := lru.New[common.Hash, bool]("validHashes", maxForkDepth*8)
	if err != nil {
		panic(err)
	}
	timingsCache, err := lru.New[common.Hash, BlockTimings]("timingsCache", timingsCacheSize)
	if err != nil {
		panic(err)
	}
	return &ForkValidator{
		currentHeight: currentHeight,
		validHashes:   validHashes,
		timingsCache:  timingsCache,
	}
}

func NewForkValidator(ctx context.Context, currentHeight uint64, validatePayload validatePayloadFunc, tmpDir string, blockReader services.FullBlockReader) *ForkValidator {
	validHashes, err := lru.New[common.Hash, bool]("validHashes", maxForkDepth*8)
	if err != nil {
		panic(err)
	}

	timingsCache, err := lru.New[common.Hash, BlockTimings]("timingsCache", timingsCacheSize)
	if err != nil {
		panic(err)
	}
	return &ForkValidator{
		validatePayload: validatePayload,
		currentHeight:   currentHeight,
		tmpDir:          tmpDir,
		blockReader:     blockReader,
		ctx:             ctx,
		validHashes:     validHashes,
		timingsCache:    timingsCache,
	}
}

// ExtendingForkHeadHash return the fork head hash of the fork that extends the canonical chain.
func (fv *ForkValidator) ExtendingForkHeadHash() common.Hash {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	return fv.extendingForkHeadHash
}

// NotifyCurrentHeight is to be called at the end of the stage cycle and represent the last processed block.
func (fv *ForkValidator) NotifyCurrentHeight(currentHeight uint64) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if fv.currentHeight == currentHeight {
		return
	}
	fv.currentHeight = currentHeight
	// If the head changed,e previous assumptions on head are incorrect now.
	if fv.sharedDom != nil {
		fv.sharedDom.Close()
	}
	fv.sharedDom = nil
	fv.extendingForkNotifications = nil
	fv.extendingForkNumber = 0
	fv.extendingForkHeadHash = common.Hash{}
}

// FlushExtendingFork flush the current extending fork if fcu chooses its head hash as the its forkchoice.
func (fv *ForkValidator) FlushExtendingFork(tx kv.RwTx, accumulator *shards.Accumulator, recentLogs *shards.RecentLogs) error {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	start := time.Now()
	// Flush changes to db.
	if fv.sharedDom != nil {
		_, err := fv.sharedDom.ComputeCommitment(context.Background(), true, fv.sharedDom.BlockNum(), fv.sharedDom.TxNum(), "flush-commitment")
		if err != nil {
			return err
		}
		if err := fv.sharedDom.Flush(fv.ctx, tx); err != nil {
			return err
		}
		fv.sharedDom.Close()
		if err := stages.SaveStageProgress(tx, stages.Execution, fv.extendingForkNumber); err != nil {
			return err
		}
	}
	timings, _ := fv.timingsCache.Get(fv.extendingForkHeadHash)
	timings[BlockTimingsFlushExtendingFork] = time.Since(start)
	fv.timingsCache.Add(fv.extendingForkHeadHash, timings)
	fv.extendingForkNotifications.Accumulator.CopyAndReset(accumulator)
	fv.extendingForkNotifications.RecentLogs.CopyAndReset(recentLogs)
	// Clean extending fork data
	fv.sharedDom = nil

	fv.extendingForkHeadHash = common.Hash{}
	fv.extendingForkNumber = 0
	fv.extendingForkNotifications = nil
	return nil
}

type HasDiff interface {
	Diff() (*membatchwithdb.MemoryDiff, error)
}

// ValidatePayload returns whether a payload is valid or invalid, or if cannot be determined, it will be accepted.
// if the payload extends the canonical chain, then we stack it in extendingFork without any unwind.
// if the payload is a fork then we unwind to the point where the fork meets the canonical chain, and there we check whether it is valid.
// if for any reason none of the actions above can be performed due to lack of information, we accept the payload and avoid validation.
func (fv *ForkValidator) ValidatePayload(tx kv.RwTx, header *types.Header, body *types.RawBody, logger log.Logger) (status engine_types.EngineStatus, latestValidHash common.Hash, validationError error, criticalError error) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if fv.validatePayload == nil {
		status = engine_types.AcceptedStatus
		return
	}
	hash := header.Hash()
	number := header.Number.Uint64()

	// If the block is stored within the side fork it means it was already validated.
	if _, ok := fv.validHashes.Get(hash); ok {
		status = engine_types.ValidStatus
		latestValidHash = hash
		return
	}

	// if the block is not in range of maxForkDepth from head then we do not validate it.
	if math.AbsoluteDifference(fv.currentHeight, header.Number.Uint64()) > maxForkDepth {
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
	if fv.sharedDom != nil {
		fv.sharedDom.Close()
	}

	temporalTx := tx.(kv.TemporalTx)
	fv.sharedDom, criticalError = state.NewSharedDomains(temporalTx, logger)
	if criticalError != nil {
		criticalError = fmt.Errorf("failed to create shared domains: %w", criticalError)
		return
	}
	txc := wrap.NewTxContainer(tx, fv.sharedDom)

	fv.extendingForkNotifications = shards.NewNotifications(nil)
	return fv.validateAndStorePayload(txc, header, body, unwindPoint, headersChain, bodiesChain, fv.extendingForkNotifications)
}

// Clear wipes out current extending fork data, this method is called after fcu is called,
// because fcu decides what the head is and after the call is done all the non-chosen forks are
// to be considered obsolete.
func (fv *ForkValidator) clear() {
	fv.extendingForkHeadHash = common.Hash{}
	fv.extendingForkNumber = 0
	if fv.sharedDom != nil {
		fv.sharedDom.Close()
	}
	fv.sharedDom = nil
}

// Clear wipes out current extending fork data.
func (fv *ForkValidator) ClearWithUnwind(accumulator *shards.Accumulator, c shards.StateChangeConsumer) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	fv.clear()
}

// validateAndStorePayload validate and store a payload fork chain if such chain results valid.
func (fv *ForkValidator) validateAndStorePayload(txc wrap.TxContainer, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody,
	notifications *shards.Notifications) (status engine_types.EngineStatus, latestValidHash common.Hash, validationError error, criticalError error) {
	start := time.Now()
	if err := fv.validatePayload(txc, header, body, unwindPoint, headersChain, bodiesChain, notifications); err != nil {
		if errors.Is(err, consensus.ErrInvalidBlock) {
			validationError = err
		} else {
			criticalError = fmt.Errorf("validateAndStorePayload: %w", err)
			return
		}
	}
	fv.timingsCache.Add(header.Hash(), BlockTimings{time.Since(start), 0})

	latestValidHash = header.Hash()
	fv.extendingForkHeadHash = header.Hash()
	fv.extendingForkNumber = header.Number.Uint64()
	if validationError != nil {
		var latestValidNumber uint64
		latestValidNumber, criticalError = stages.GetStageProgress(txc.Tx, stages.Execution)

		if criticalError != nil {
			return
		}
		var ok bool
		latestValidHash, ok, criticalError = fv.blockReader.CanonicalHash(fv.ctx, txc.Tx, latestValidNumber)
		if criticalError != nil {
			return
		}
		if !ok {
			criticalError = fmt.Errorf("canonical hash not found: %d", latestValidNumber)
			return
		}
		status = engine_types.InvalidStatus
		if fv.sharedDom != nil {
			fv.sharedDom.Close()
		}
		fv.sharedDom = nil
		fv.extendingForkHeadHash = common.Hash{}
		fv.extendingForkNumber = 0
		return
	}
	fv.validHashes.Add(header.Hash(), true)

	// If we do not have the body we can recover it from the batch.
	if body != nil {
		if _, criticalError = rawdb.WriteRawBodyIfNotExists(txc.Tx, header.Hash(), header.Number.Uint64(), body); criticalError != nil {
			return //nolint:nilnesserr
		}
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
