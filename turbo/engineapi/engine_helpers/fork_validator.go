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
	"fmt"
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

// the maximum point from the current head, past which side forks are not validated anymore.
const maxForkDepth = 32 // 32 slots is the duration of an epoch thus there cannot be side forks in PoS deeper than 32 blocks from head.

type validatePayloadFunc func(kv.RwTx, *types.Header, *types.RawBody, uint64, []*types.Header, []*types.RawBody, *shards.Notifications) error

type ForkValidator struct {
	// current memory batch containing chain head that extend canonical fork.
	extendingFork *memdb.MemoryMutation
	// notifications accumulated for the extending fork
	extendingForkNotifications *shards.Notifications
	// hash of chain head that extend canonical fork.
	extendingForkHeadHash libcommon.Hash
	extendingForkNumber   uint64
	// this is the function we use to perform payload validation.
	validatePayload validatePayloadFunc
	blockReader     services.FullBlockReader
	// this is the current point where we processed the chain so far.
	currentHeight uint64
	tmpDir        string
	// block hashes that are deemed valid
	validHashes *lru.Cache[libcommon.Hash, bool]

	ctx context.Context

	// we want fork validator to be thread safe so let
	lock sync.Mutex
}

func NewForkValidatorMock(currentHeight uint64) *ForkValidator {
	validHashes, err := lru.New[libcommon.Hash, bool]("validHashes", maxForkDepth*8)
	if err != nil {
		panic(err)
	}
	return &ForkValidator{
		currentHeight: currentHeight,
		validHashes:   validHashes,
	}
}

func NewForkValidator(ctx context.Context, currentHeight uint64, validatePayload validatePayloadFunc, tmpDir string, blockReader services.FullBlockReader) *ForkValidator {
	validHashes, err := lru.New[libcommon.Hash, bool]("validHashes", maxForkDepth*8)
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
	}
}

// ExtendingForkHeadHash return the fork head hash of the fork that extends the canonical chain.
func (fv *ForkValidator) ExtendingForkHeadHash() libcommon.Hash {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	return fv.extendingForkHeadHash
}

func (fv *ForkValidator) notifyTxPool(to uint64, accumulator *shards.Accumulator, c shards.StateChangeConsumer) error {
	hash, err := fv.blockReader.CanonicalHash(context.Background(), fv.extendingFork, to)
	if err != nil {
		return fmt.Errorf("read canonical hash of unwind point: %w", err)
	}
	header, _ := fv.blockReader.Header(context.Background(), fv.extendingFork, hash, to)
	if header == nil {
		return fmt.Errorf("could not find header for block: %d", to)
	}

	txs, err := fv.blockReader.RawTransactions(context.Background(), fv.extendingFork, to, to+1)
	if err != nil {
		return err
	}
	// Start the changes
	accumulator.Reset(0)
	accumulator.StartChange(to, hash, txs, true)
	accumulator.SendAndReset(context.Background(), c, header.BaseFee.Uint64(), header.GasLimit)
	log.Info("Transaction pool notified of discard side fork.")
	return nil
}

// NotifyCurrentHeight is to be called at the end of the stage cycle and repressent the last processed block.
func (fv *ForkValidator) NotifyCurrentHeight(currentHeight uint64) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if fv.currentHeight == currentHeight {
		return
	}
	fv.currentHeight = currentHeight
	// If the head changed,e previous assumptions on head are incorrect now.
	if fv.extendingFork != nil {
		fv.extendingFork.Rollback()
	}
	fv.extendingFork = nil
	fv.extendingForkNotifications = nil
	fv.extendingForkNumber = 0
	fv.extendingForkHeadHash = libcommon.Hash{}
}

// FlushExtendingFork flush the current extending fork if fcu chooses its head hash as the its forkchoice.
func (fv *ForkValidator) FlushExtendingFork(tx kv.RwTx, accumulator *shards.Accumulator) error {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	// Flush changes to db.
	if err := fv.extendingFork.Flush(tx); err != nil {
		return err
	}
	fv.extendingForkNotifications.Accumulator.CopyAndReset(accumulator)
	// Clean extending fork data
	fv.extendingFork.Rollback()
	fv.extendingForkHeadHash = libcommon.Hash{}
	fv.extendingForkNumber = 0
	fv.extendingFork = nil
	fv.extendingForkNotifications = nil
	return nil
}

// ValidatePayload returns whether a payload is valid or invalid, or if cannot be determined, it will be accepted.
// if the payload extend the canonical chain, then we stack it in extendingFork without any unwind.
// if the payload is a fork then we unwind to the point where the fork meet the canonical chain and we check if it is valid or not from there.
// if for any reasons none of the action above can be performed due to lack of information, we accept the payload and avoid validation.
func (fv *ForkValidator) ValidatePayload(tx kv.Tx, header *types.Header, body *types.RawBody, extendCanonical bool) (status engine_types.EngineStatus, latestValidHash libcommon.Hash, validationError error, criticalError error) {
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
		// If the new block extends the canonical chain we update extendingFork.
		if fv.extendingFork == nil {
			fv.extendingFork = memdb.NewMemoryBatch(tx, fv.tmpDir)
			fv.extendingForkNotifications = &shards.Notifications{
				Events:      shards.NewEvents(),
				Accumulator: shards.NewAccumulator(),
			}
		} else {
			fv.extendingFork.UpdateTxn(tx)
		}
		// Update fork head hash.
		fv.extendingForkHeadHash = header.Hash()
		fv.extendingForkNumber = header.Number.Uint64()
		return fv.validateAndStorePayload(fv.extendingFork, header, body, 0, nil, nil, fv.extendingForkNotifications)
	}

	// if the block is not in range of maxForkDepth from head then we do not validate it.
	if math.AbsoluteDifference(fv.currentHeight, header.Number.Uint64()) > maxForkDepth {
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
	if unwindPoint == fv.currentHeight {
		unwindPoint = 0
	}
	batch := memdb.NewMemoryBatch(tx, fv.tmpDir)
	defer batch.Rollback()
	notifications := &shards.Notifications{
		Events:      shards.NewEvents(),
		Accumulator: shards.NewAccumulator(),
	}
	return fv.validateAndStorePayload(batch, header, body, unwindPoint, headersChain, bodiesChain, notifications)
}

// Clear wipes out current extending fork data, this method is called after fcu is called,
// because fcu decides what the head is and after the call is done all the non-chosed forks are
// to be considered obsolete.
func (fv *ForkValidator) clear() {
	if fv.extendingFork != nil {
		fv.extendingFork.Rollback()
	}
	fv.extendingForkHeadHash = libcommon.Hash{}
	fv.extendingForkNumber = 0
	fv.extendingFork = nil
	//fv.sideForksBlock = map[libcommon.Hash]forkSegment{}
}

// Clear wipes out current extending fork data and notify txpool.
func (fv *ForkValidator) ClearWithUnwind(tx kv.RwTx, accumulator *shards.Accumulator, c shards.StateChangeConsumer) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	// If we did not flush the fork state, then we need to notify the txpool through unwind.
	if fv.extendingFork != nil && accumulator != nil && fv.extendingForkHeadHash != (libcommon.Hash{}) {
		fv.extendingFork.UpdateTxn(tx)
		// this will call unwind of extending fork to notify txpool of reverting transactions.
		if err := fv.notifyTxPool(fv.extendingForkNumber-1, accumulator, c); err != nil {
			log.Warn("could not notify txpool of invalid side fork", "err", err)
		}
		fv.extendingFork.Rollback()
	}
	fv.clear()
}

// validateAndStorePayload validate and store a payload fork chain if such chain results valid.
func (fv *ForkValidator) validateAndStorePayload(tx kv.RwTx, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody,
	notifications *shards.Notifications) (status engine_types.EngineStatus, latestValidHash libcommon.Hash, validationError error, criticalError error) {
	validationError = fv.validatePayload(tx, header, body, unwindPoint, headersChain, bodiesChain, notifications)
	latestValidHash = header.Hash()
	if validationError != nil {
		latestValidHash = header.ParentHash
		status = engine_types.InvalidStatus
		if fv.extendingFork != nil {
			fv.extendingFork.Rollback()
			fv.extendingFork = nil
		}
		fv.extendingForkHeadHash = libcommon.Hash{}
		fv.extendingForkNumber = 0
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
