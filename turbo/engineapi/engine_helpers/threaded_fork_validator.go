/*
   Copyright 2022 Erigon contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package engine_helpers

import (
	"context"
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
	"github.com/ledgerwatch/erigon/turbo/services"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

type ThreadedForkValidator struct {
	// current memory batch containing chain head that extend canonical fork.
	extendingFork *memdb.MemoryMutation
	db            kv.RwDB
	// hash of chain head that extend canonical fork.
	extendingForkHeadHash libcommon.Hash
	extendingForkNumber   uint64
	// this is the function we use to perform payload validation.
	validatePayload validatePayloadFunc
	blockReader     services.FullBlockReader
	// this is the current point where we processed the chain so far.
	tmpDir string
	// block hashes that are deemed valid
	validHashes *lru.Cache[libcommon.Hash, bool]

	ctx context.Context

	// channels for communication with shared thread for mdbx in-memory transactions.
	extendingForkOperationsCh        chan interface{}
	finishedExtendingForkOperationCh chan interface{}
	// we want fork validator to be thread safe so let
	lock sync.Mutex
}

func NewThreadedForkValidator(ctx context.Context, db kv.RwDB, validatePayload validatePayloadFunc, tmpDir string, blockReader services.FullBlockReader) *ThreadedForkValidator {
	validHashes, err := lru.New[libcommon.Hash, bool]("validHashes", maxForkDepth*8)
	if err != nil {
		panic(err)
	}
	t := &ThreadedForkValidator{
		validatePayload:                  validatePayload,
		tmpDir:                           tmpDir,
		blockReader:                      blockReader,
		ctx:                              ctx,
		validHashes:                      validHashes,
		db:                               db,
		extendingForkOperationsCh:        make(chan interface{}),
		finishedExtendingForkOperationCh: make(chan interface{}),
	}
	go t.loop()
	return t
}

// ExtendingForkHeadHash return the fork head hash of the fork that extends the canonical chain.
func (fv *ThreadedForkValidator) ExtendingForkHeadHash() libcommon.Hash {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	return fv.extendingForkHeadHash
}

// FlushExtendingFork flush the current extending fork if fcu chooses its head hash as the its forkchoice.
func (fv *ThreadedForkValidator) FlushExtendingFork() error {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	fv.extendingForkOperationsCh <- flushOp{}
	errInterface := <-fv.finishedExtendingForkOperationCh
	return errInterface.(error)
}

// ValidatePayload returns whether a payload is valid or invalid, or if cannot be determined, it will be accepted.
// if the payload extend the canonical chain, then we stack it in extendingFork without any unwind.
// if the payload is a fork then we unwind to the point where the fork meet the canonical chain and we check if it is valid or not from there.
// if for any reasons none of the action above can be performed due to lack of information, we accept the payload and avoid validation.
func (fv *ThreadedForkValidator) ValidatePayload(tx kv.Tx, header *types.Header, body *types.RawBody, extendCanonical bool) (status engine_types.EngineStatus, latestValidHash libcommon.Hash, validationError error, criticalError error) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if fv.validatePayload == nil {
		status = engine_types.AcceptedStatus
		return
	}

	// If the block is stored within the side fork it means it was already validated.
	if _, ok := fv.validHashes.Get(header.Hash()); ok {
		status = engine_types.ValidStatus
		latestValidHash = header.Hash()
		return
	}

	if extendCanonical {
		fv.extendingForkOperationsCh <- validateOp{header: header, body: body}
		result := <-fv.finishedExtendingForkOperationCh
		validatedResult := result.(validateResult)
		return validatedResult.status, validatedResult.latestValidHash, validatedResult.validationError, validatedResult.criticalError
	}

	currentHeight := rawdb.ReadCurrentBlockNumber(tx)
	// if the block is not in range of maxForkDepth from head then we do not validate it.
	if math.AbsoluteDifference(*currentHeight, header.Number.Uint64()) > maxForkDepth {
		status = engine_types.AcceptedStatus
		return
	}
	// Let's assemble the side fork backwards
	var foundCanonical bool
	currentHash := header.ParentHash
	unwindPoint := header.Number.Uint64() - 1
	foundCanonical, criticalError = rawdb.IsCanonicalHash(tx, currentHash, unwindPoint)
	if criticalError != nil {
		return
	}

	var bodiesChain []*types.RawBody
	var headersChain []*types.Header
	for !foundCanonical {
		var (
			header *types.Header
			body   *types.Body
		)
		body, criticalError = fv.blockReader.BodyWithTransactions(fv.ctx, tx, currentHash, unwindPoint)
		if criticalError != nil {
			return
		}
		header, criticalError = fv.blockReader.Header(fv.ctx, tx, currentHash, unwindPoint)
		if criticalError != nil {
			return
		}
		if header == nil {
			// We miss some components so we did not check validity.
			status = engine_types.AcceptedStatus
			return
		}
		headersChain = append([]*types.Header{header}, headersChain...)
		if body == nil {
			bodiesChain = append([]*types.RawBody{nil}, bodiesChain...)
		} else {
			bodiesChain = append([]*types.RawBody{body.RawBody()}, bodiesChain...)

		}

		currentHash = header.ParentHash
		unwindPoint = header.Number.Uint64() - 1
		foundCanonical, criticalError = rawdb.IsCanonicalHash(tx, currentHash, unwindPoint)
		if criticalError != nil {
			return
		}
	}
	// Do not set an unwind point if we are already there.
	if unwindPoint == *currentHeight {
		unwindPoint = 0
	}
	batch := memdb.NewMemoryBatch(tx, fv.tmpDir)
	notifications := &shards.Notifications{
		Events:      shards.NewEvents(),
		Accumulator: shards.NewAccumulator(),
	}
	defer batch.Rollback()
	return fv.validateAndStorePayload(batch, header, body, unwindPoint, headersChain, bodiesChain, notifications)
}

// Clear wipes out current extending fork data, this method is called after fcu is called,
// because fcu decides what the head is and after the call is done all the non-chosed forks are
// to be considered obsolete.
func (fv *ThreadedForkValidator) Clear() {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	fv.clear()
}

func (fv *ThreadedForkValidator) clear() {
	fv.extendingForkOperationsCh <- clearOp{}
	<-fv.finishedExtendingForkOperationCh
}

// validateAndStorePayload validate and store a payload fork chain if such chain results valid.
func (fv *ThreadedForkValidator) validateAndStorePayload(tx kv.RwTx, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody,
	notifications *shards.Notifications) (status engine_types.EngineStatus, latestValidHash libcommon.Hash, validationError error, criticalError error) {
	validationError = fv.validatePayload(tx, header, body, unwindPoint, headersChain, bodiesChain, notifications)
	latestValidHash = header.Hash()
	if validationError != nil {
		var latestValidNumber uint64
		latestValidNumber, criticalError = stages.GetStageProgress(tx, stages.IntermediateHashes)
		if criticalError != nil {
			return
		}
		latestValidHash, criticalError = rawdb.ReadCanonicalHash(tx, latestValidNumber)
		if criticalError != nil {
			return
		}
		status = engine_types.InvalidStatus
		fv.clear()
		return
	}
	fv.validHashes.Add(header.Hash(), true)

	// If we do not have the body we can recover it from the batch.
	if body != nil {
		if _, criticalError = rawdb.WriteRawBodyIfNotExists(tx, header.Hash(), header.Number.Uint64(), body); criticalError != nil {
			return
		}
	}

	status = engine_types.ValidStatus
	return
}

type validateResult struct {
	status          engine_types.EngineStatus
	latestValidHash libcommon.Hash
	validationError error
	criticalError   error
}

type validateOp struct {
	header *types.Header
	body   *types.RawBody
}
type clearOp struct{}
type flushOp struct{}

func (fv *ThreadedForkValidator) loop() {
	for {
		select {
		case <-fv.ctx.Done():
			return
		case op := <-fv.extendingForkOperationsCh:
			fv.finishedExtendingForkOperationCh <- fv.handleExtendingForkOperation(op)
		}
	}
}

func (fv *ThreadedForkValidator) handleExtendingForkOperation(op interface{}) interface{} {
	switch obj := op.(type) {
	case validateOp:
		tx, err := fv.db.BeginRo(fv.ctx)
		if err != nil {
			return validateResult{criticalError: err}
		}
		defer tx.Rollback()
		header := obj.header
		body := obj.body
		// easier this way.
		if fv.extendingFork != nil {
			fv.extendingFork.Rollback()
		}
		notifications := &shards.Notifications{
			Events:      shards.NewEvents(),
			Accumulator: shards.NewAccumulator(),
		}
		fv.extendingFork = memdb.NewMemoryBatch(tx, fv.tmpDir)
		// Update fork head hash.
		fv.extendingForkHeadHash = header.Hash()
		fv.extendingForkNumber = header.Number.Uint64()
		status, latestValidHash, validationError, criticalError := fv.validateAndStorePayload(fv.extendingFork, header, body, 0, nil, nil, notifications)
		return validateResult{status: status, latestValidHash: latestValidHash, validationError: validationError, criticalError: criticalError}
	case flushOp:
		tx, err := fv.db.BeginRw(fv.ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		// Flush changes to db.
		if err := fv.extendingFork.Flush(tx); err != nil {
			return err
		}
		// Clean extending fork data
		fv.extendingFork.Rollback()
		fv.extendingForkHeadHash = libcommon.Hash{}
		fv.extendingForkNumber = 0
		fv.extendingFork = nil
		return tx.Commit()
	case clearOp:
		if fv.extendingFork != nil {
			fv.extendingFork.Rollback()
		}
		fv.extendingForkHeadHash = libcommon.Hash{}
		fv.extendingForkNumber = 0
		fv.extendingFork = nil
		return struct{}{}
	}
	panic("invalid op")
}
