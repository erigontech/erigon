// Copyright 2024 The Erigon Authors
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
	"math/big"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

var ErrMissingChainSegment = errors.New("missing chain segment")

var inMemHistoryReads = dbg.EnvBool("ERIGON_IN_MEM_HISTORY", true)

func makeErrMissingChainSegment(blockHash common.Hash) error {
	return errors.Join(ErrMissingChainSegment, errors.New("block hash: "+blockHash.String()))
}

func GetBlockHashFromMissingSegmentError(err error) (common.Hash, bool) {
	if !errors.Is(err, ErrMissingChainSegment) {
		return common.Hash{}, false
	}
	// Otherwise, we assume the error is a joined error from makeErrMissingChainSegment.
	// We define an interface to get access to the underlying errors.
	type unwrapper interface {
		Unwrap() []error
	}
	uw, ok := err.(unwrapper)
	if !ok {
		return common.Hash{}, false
	}

	// iterate through suberrors to find one that contains the block hash info.
	var hashStr string
	const prefix = "block hash: "
	for _, subErr := range uw.Unwrap() {
		msg := subErr.Error()
		if after, ok := strings.CutPrefix(msg, prefix); ok {
			hashStr = after
			break
		}
	}
	if hashStr == "" {
		return common.Hash{}, false
	}

	// Convert the extracted string into a common.Hash.
	// This assumes the existence of common.ParseHash.
	return common.HexToHash(hashStr), true
}

// Cache bridges RPC reads to the execution module's in-memory state via
// SharedDomains. When View is called, it grabs the current SD under a read
// lock so that domain reads (accounts, storage, code) see uncommitted writes
// from the pipeline, falling through to the caller's DB tx for committed data.
//
// OnNewBlock is intentionally a no-op: in the embedded (non-remote) rpcdaemon
// the SD is the authoritative source, so the coherent cache's state-tracking
// machinery is unnecessary.
//
// This shim predates SharedDomains' current capabilities and will be simplified
// as part of #19623 (2-cache IBS rationalization) once the StateReader/CacheView
// interfaces stabilize. See also #19798 (event stream extraction) and #19855
// (TransactionState/BlockState separation).
type Cache struct {
	execModule  *ExecModule
	publishedSD func() *execctx.SharedDomains // returns the latest published SD from Events (for background commit)
}

// SetPublishedSD wires the Cache to fall back to the published SD from Events
// when the exec module's currentContext is nil (e.g. during background commit).
func (c *Cache) SetPublishedSD(provider func() *execctx.SharedDomains) {
	c.publishedSD = provider
}

var _ kvcache.Cache = (*Cache)(nil)         // compile-time interface check
var _ kvcache.CacheView = (*CacheView)(nil) // compile-time interface check

func (c *Cache) View(_ context.Context, tx kv.TemporalTx) (kvcache.CacheView, error) {
	var context *execctx.SharedDomains
	if c.execModule != nil {
		c.execModule.lock.RLock()
		context = c.execModule.currentContext
		c.execModule.lock.RUnlock()
	}
	// Fall back to the published SD from Events during background commits
	// (currentContext is nil but the SD is still valid in memory).
	if context == nil && c.publishedSD != nil {
		context = c.publishedSD()
	}

	return &CacheView{context: context, tx: tx}, nil
}
func (c *Cache) OnNewBlock(sc *remoteproto.StateChangeBatch) {}
func (c *Cache) Evict() int                                  { return 0 }
func (c *Cache) Len() int                                    { return 0 }
func (c *Cache) ValidateCurrentRoot(_ context.Context, _ kv.TemporalTx) (*kvcache.CacheValidationResult, error) {
	return &kvcache.CacheValidationResult{Enabled: false}, nil
}

type CacheView struct {
	context *execctx.SharedDomains
	tx      kv.TemporalTx
}

func (c *CacheView) Get(k []byte) ([]byte, error) {
	var getter kv.TemporalGetter = c.tx
	if c.context != nil {
		getter = c.context.AsGetter(c.tx)
	}
	if len(k) == 20 {
		v, _, err := getter.GetLatest(kv.AccountsDomain, k)
		return v, err
	}
	v, _, err := getter.GetLatest(kv.StorageDomain, k)
	return v, err
}
func (c *CacheView) GetCode(k []byte) ([]byte, error) {
	var getter kv.TemporalGetter = c.tx
	if c.context != nil {
		getter = c.context.AsGetter(c.tx)
	}
	v, _, err := getter.GetLatest(kv.CodeDomain, k)
	return v, err
}

func (c *CacheView) GetAsOf(key []byte, ts uint64) (v []byte, ok bool, err error) {
	if c.context != nil {
		if len(key) == 20 {
			return c.context.GetAsOf(kv.AccountsDomain, key, ts)
		}
		return c.context.GetAsOf(kv.StorageDomain, key, ts)
	}
	return nil, false, nil
}

func (c *CacheView) HasStorage(address common.Address) (bool, error) {
	var getter kv.TemporalGetter = c.tx
	if c.context != nil {
		getter = c.context.AsGetter(c.tx)
	}
	_, _, hasStorage, err := getter.HasPrefix(kv.StorageDomain, address[:])
	return hasStorage, err
}

type ExecModule struct {
	bacgroundCtx context.Context
	// Snapshots + MDBX
	blockReader services.FullBlockReader

	// MDBX database
	db               kv.TemporalRwDB // main database
	semaphore        *semaphore.Weighted
	forkValidator    *ForkValidator
	pipelineExecutor *PipelineExecutor

	logger log.Logger
	// Block building
	nextPayloadId  uint64
	lastParameters *builder.Parameters
	builderFunc    builder.BlockBuilderFunc
	builders       map[uint64]*builder.BlockBuilder

	// Changes accumulator
	hook  *stageloop.Hook
	accum *Accumulation

	// configuration
	config  *chain.Config
	syncCfg ethconfig.Sync
	// rules engine
	engine rules.Engine

	fcuBackgroundPrune      bool
	fcuBackgroundCommit     bool
	onlySnapDownloadOnStart bool
	// metrics for average mgas/sec
	avgMgasSec float64

	lock           sync.RWMutex
	currentContext *execctx.SharedDomains
	publishedSD    func() *execctx.SharedDomains // fallback for background commit

	// stateCache is a cache for state data (accounts, storage, code)
	stateCache *cache.StateCache

	stopNode func() error
}

var _ ExecutionModule = (*ExecModule)(nil) // compile-time interface check

func NewExecModule(
	ctx context.Context,
	blockReader services.FullBlockReader,
	db kv.TemporalRwDB,
	pipelineExecutor *PipelineExecutor,
	currentBlockNumber uint64,
	config *chain.Config,
	builderFunc builder.BlockBuilderFunc,
	hook *stageloop.Hook,
	accum *Accumulation,
	stateCache *Cache,
	logger log.Logger,
	engine rules.Engine,
	syncCfg ethconfig.Sync,
	fcuBackgroundPrune bool,
	fcuBackgroundCommit bool,
	onlySnapDownloadOnStart bool,
	stopNode func() error,
) *ExecModule {
	domainCache := cache.NewDefaultStateCache()
	forkValidator := newForkValidator(ctx, currentBlockNumber, pipelineExecutor, blockReader, syncCfg.MaxReorgDepth)

	em := &ExecModule{
		blockReader:             blockReader,
		db:                      db,
		logger:                  logger,
		forkValidator:           forkValidator,
		pipelineExecutor:        pipelineExecutor,
		builders:                make(map[uint64]*builder.BlockBuilder),
		builderFunc:             builderFunc,
		config:                  config,
		semaphore:               semaphore.NewWeighted(1),
		hook:                    hook,
		accum:                   accum,
		engine:                  engine,
		syncCfg:                 syncCfg,
		bacgroundCtx:            ctx,
		fcuBackgroundPrune:      fcuBackgroundPrune,
		fcuBackgroundCommit:     fcuBackgroundCommit,
		onlySnapDownloadOnStart: onlySnapDownloadOnStart,
		stateCache:              domainCache,
		stopNode:                stopNode,
	}

	if stateCache != nil {
		stateCache.execModule = em
	}
	return em
}

// WaitIdle blocks until any in-flight updateForkChoice goroutine finishes.
// Call before closing the database to avoid waitTxsAllDoneOnClose hangs.
func (e *ExecModule) WaitIdle(ctx context.Context) {
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		return // context cancelled — best effort
	}
	e.semaphore.Release(1)
}

// ForkValidator returns the fork validator owned by this module.
func (e *ExecModule) ForkValidator() *ForkValidator { return e.forkValidator }

// SetPublishedSD wires the ExecModule to fall back to the published SD from Events
// when currentContext is nil (e.g. during background commit).
func (e *ExecModule) SetPublishedSD(provider func() *execctx.SharedDomains) {
	e.publishedSD = provider
}

func (e *ExecModule) getHeader(ctx context.Context, tx kv.Tx, blockHash common.Hash, blockNumber uint64) (*types.Header, error) {
	if e.blockReader == nil {
		return rawdb.ReadHeader(tx, blockHash, blockNumber), nil
	}

	return e.blockReader.Header(ctx, tx, blockHash, blockNumber)
}

func (e *ExecModule) getTD(_ context.Context, tx kv.Tx, blockHash common.Hash, blockNumber uint64) (*big.Int, error) {
	return rawdb.ReadTd(tx, blockHash, blockNumber)

}

func (e *ExecModule) getBody(ctx context.Context, tx kv.Tx, blockHash common.Hash, blockNumber uint64) (*types.Body, error) {
	if e.blockReader == nil {
		body, _, _ := rawdb.ReadBody(tx, blockHash, blockNumber)
		return body, nil
	}
	return e.blockReader.BodyWithTransactions(ctx, tx, blockHash, blockNumber)
}

func (e *ExecModule) canonicalHash(ctx context.Context, tx kv.Tx, blockNumber uint64) (common.Hash, error) {
	var canonical common.Hash
	var err error
	if e.blockReader == nil {
		canonical, err = rawdb.ReadCanonicalHash(tx, blockNumber)
		if err != nil {
			return common.Hash{}, err
		}
	} else {
		var ok bool
		canonical, ok, err = e.blockReader.CanonicalHash(ctx, tx, blockNumber)
		if err != nil {
			return common.Hash{}, err
		}
		if !ok {
			return common.Hash{}, nil
		}
	}

	return canonical, nil
}

func (e *ExecModule) unwindToCommonCanonical(sd *execctx.SharedDomains, tx kv.TemporalRwTx, header *types.Header) error {
	currentHeader := header
	for isCanonical, err := e.isCanonicalHash(e.bacgroundCtx, tx, currentHeader.Hash()); !isCanonical && err == nil; isCanonical, err = e.isCanonicalHash(e.bacgroundCtx, tx, currentHeader.Hash()) {
		parentBlockHash, parentBlockNum := currentHeader.ParentHash, currentHeader.Number.Uint64()-1
		currentHeader, err = e.getHeader(e.bacgroundCtx, tx, parentBlockHash, parentBlockNum)
		if err != nil {
			return err
		}
		if currentHeader == nil {
			return makeErrMissingChainSegment(parentBlockHash)
		}
	}
	// Check if you can skip unwind by comparing the current header number with the progress of all stages.
	// If they are equal, then we are safely already at the common canonical and can skip unwind.
	unwindPoint := currentHeader.Number.Uint64()
	commonProgress, allEqual, err := stages.GetStageProgressIfAllEqual(tx,
		stages.Headers, stages.Senders, stages.Execution)
	if err != nil {
		return err
	}
	if allEqual && commonProgress == unwindPoint {
		return nil
	}

	if err := e.hook.BeforeRun(tx, true); err != nil {
		return err
	}

	if err := e.pipelineExecutor.UnwindTo(unwindPoint, stagedsync.ExecUnwind, tx); err != nil {
		return err
	}
	if err := e.pipelineExecutor.RunUnwind(sd, tx); err != nil {
		return err
	}
	return nil
}

func (e *ExecModule) ValidateChain(ctx context.Context, blockHash common.Hash, blockNumber uint64) (ValidationResult, error) {
	if !e.semaphore.TryAcquire(1) {
		e.logger.Trace("ethereumExecutionModule.ValidateChain: ExecutionStatus_Busy")
		return ValidationResult{
			ValidationStatus: ExecutionStatusBusy,
		}, nil
	}
	defer e.semaphore.Release(1)

	e.hook.LastNewBlockSeen(blockNumber) // used by eth_syncing
	e.currentContext.ResetPendingUpdates()
	e.forkValidator.ClearWithUnwind()
	e.logger.Debug("[execmodule] validating chain", "number", blockNumber, "hash", blockHash)
	var (
		header             *types.Header
		body               *types.Body
		currentBlockNumber *uint64
		err                error
	)
	// Read header/body from the block overlay on currentContext if available
	// (block data written by InsertBlocks hasn't been flushed to DB yet),
	// falling back to a plain DB read otherwise.
	if e.currentContext != nil && e.currentContext.BlockOverlay() != nil {
		overlay := e.currentContext.BlockOverlay()
		roTx, err := e.db.BeginTemporalRo(ctx)
		if err != nil {
			return ValidationResult{}, err
		}
		defer roTx.Rollback()
		overlay.UpdateTxn(roTx)
		header, err = e.blockReader.Header(ctx, overlay, blockHash, blockNumber)
		if err != nil {
			return ValidationResult{}, err
		}
		body, err = e.blockReader.BodyWithTransactions(ctx, overlay, blockHash, blockNumber)
		if err != nil {
			return ValidationResult{}, err
		}
		exec.AddHeaderAndBodyToGlobalReadAheader(ctx, e.db, header, body)
		currentBlockNumber = rawdb.ReadCurrentBlockNumber(overlay)
	} else {
		if err := e.db.View(ctx, func(tx kv.Tx) error {
			header, err = e.blockReader.Header(ctx, tx, blockHash, blockNumber)
			if err != nil {
				return err
			}

			body, err = e.blockReader.BodyWithTransactions(ctx, tx, blockHash, blockNumber)
			if err != nil {
				return err
			}
			exec.AddHeaderAndBodyToGlobalReadAheader(ctx, e.db, header, body)
			currentBlockNumber = rawdb.ReadCurrentBlockNumber(tx)
			return nil
		}); err != nil {
			return ValidationResult{}, err
		}
	}
	if header == nil || body == nil {
		return ValidationResult{
			LatestValidHash:  common.Hash{},
			ValidationStatus: ExecutionStatusMissingSegment,
		}, nil
	}

	if math.AbsoluteDifference(*currentBlockNumber, blockNumber) >= e.syncCfg.MaxReorgDepth {
		return ValidationResult{
			ValidationStatus: ExecutionStatusTooFarAway,
			LatestValidHash:  common.Hash{},
		}, nil
	}

	tx, err := e.db.BeginTemporalRwNosync(ctx)
	if err != nil {
		return ValidationResult{}, err
	}
	defer tx.Rollback()

	// Flush block overlay data (headers, bodies, TDs from InsertBlocks) into
	// this RW tx so that unwindToCommonCanonical and ValidatePayload can see
	// the block data without it being committed to DB. This tx will be rolled
	// back, so the flush is temporary — the overlay retains all data.
	if e.currentContext != nil && e.currentContext.BlockOverlay() != nil {
		if err := e.currentContext.BlockOverlay().Flush(ctx, tx); err != nil {
			return ValidationResult{}, fmt.Errorf("ValidateChain: flush overlay to validation tx: %w", err)
		}
	}
	doms, err := execctx.NewSharedDomains(ctx, tx, e.logger)
	if err != nil {
		return ValidationResult{}, err
	}
	doms.SetInMemHistoryReads(inMemHistoryReads)

	// Set state cache in SharedDomains for use during state reading
	doms.SetStateCache(e.stateCache)
	if err = e.unwindToCommonCanonical(doms, tx, header); err != nil {
		doms.Close()
		return ValidationResult{}, err
	}

	status, lvh, validationError, criticalError := e.forkValidator.ValidatePayload(ctx, doms, tx, header, body.RawBody(), e.logger)
	if criticalError != nil {
		return ValidationResult{}, criticalError
	}

	// Clear state cache on invalid block
	isInvalid := status == engine_types.InvalidStatus || status == engine_types.InvalidBlockHashStatus || validationError != nil
	if e.stateCache != nil && isInvalid {
		e.stateCache.ClearWithHash(header.ParentHash)
	}

	// Throw away the tx and start a new one (do not persist changes to the canonical chain)
	tx.Rollback()
	tx, err = e.db.BeginTemporalRwNosync(ctx)
	if err != nil {
		return ValidationResult{}, err
	}
	defer tx.Rollback()

	// if the block is deemed invalid then we delete it. perhaps we want to keep bad blocks and just keep an index of bad ones.
	validationStatus := ExecutionStatusSuccess
	if status == engine_types.AcceptedStatus {
		validationStatus = ExecutionStatusMissingSegment
	}
	isInvalidChain := status == engine_types.InvalidStatus || status == engine_types.InvalidBlockHashStatus || validationError != nil
	if isInvalidChain && (lvh != common.Hash{}) && lvh != blockHash {
		if err := e.purgeBadChain(ctx, tx, lvh, blockHash); err != nil {
			return ValidationResult{}, err
		}
	}
	if isInvalidChain {
		e.logger.Warn("ethereumExecutionModule.ValidateChain: chain is invalid", "hash", blockHash)
		validationStatus = ExecutionStatusBadBlock
		// Discard the block overlay — it may contain the bad block's data.
		if e.currentContext != nil && e.currentContext.BlockOverlay() != nil {
			e.currentContext.BlockOverlay().Close()
		}
	}
	result := ValidationResult{
		ValidationStatus: validationStatus,
		LatestValidHash:  lvh,
	}
	if validationError != nil {
		result.ValidationError = validationError.Error()
	}
	return result, tx.Commit()
}

func (e *ExecModule) purgeBadChain(ctx context.Context, tx kv.RwTx, latestValidHash, headHash common.Hash) error {
	tip, err := e.blockReader.HeaderNumber(ctx, tx, headHash)
	if err != nil {
		return err
	}
	if tip == nil {
		// Block only existed in the overlay (not yet committed to DB) — nothing to purge.
		return nil
	}

	dbHeadHash := rawdb.ReadHeadBlockHash(tx)

	currentHash := headHash
	currentNumber := *tip
	for currentHash != latestValidHash {
		currentHeader, err := e.getHeader(ctx, tx, currentHash, currentNumber)
		if err != nil {
			return err
		}

		// TODO: find a better way to handle this
		if currentHash == dbHeadHash {
			// We can't delete the head block stored in the database as that is our canonical reconnection point.
			return nil
		}

		rawdb.DeleteHeader(tx, currentHash, currentNumber)
		currentHash = currentHeader.ParentHash
		currentNumber--
	}
	return nil
}

func (e *ExecModule) Start(ctx context.Context, hook *stageloop.Hook) {
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		if !errors.Is(err, context.Canceled) {
			e.logger.Error("Could not start execution service", "err", err)
		}
		return
	}
	defer e.semaphore.Release(1)

	if err := e.pipelineExecutor.ProcessFrozenBlocks(ctx, hook, e.onlySnapDownloadOnStart); err != nil {
		if !errors.Is(err, context.Canceled) {
			e.logger.Error("Could not start execution service", "err", err)
		}
		// During parallel execution, an invalid block in initial sync (ProcessFrozenBlocks)
		// is unrecoverable: the parallel executor cannot unwind and retrying will hit the
		// same block forever, pushing Caplin's backward target further back.
		// Exit the process so the operator can investigate.
		if dbg.Exec3Parallel && errors.Is(err, rules.ErrInvalidBlock) {
			e.logger.Error("Invalid block during parallel initial sync — halting process")
			go func() {
				if stopErr := e.stopNode(); stopErr != nil {
					e.logger.Error("Could not stop node on invalid block", "err", stopErr)
				}
			}()
			return
		}
	}
	// Notify the fork validator of the current execution height after startup sync.
	if err := e.db.View(ctx, func(tx kv.Tx) error {
		progress, err := stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return err
		}
		e.forkValidator.NotifyCurrentHeight(progress)
		return nil
	}); err != nil {
		e.logger.Warn("Could not notify fork validator of current height", "err", err)
	}
}

func (e *ExecModule) Ready(ctx context.Context) (bool, error) {
	// setup a timeout for the context to avoid waiting indefinitely
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	if err := <-e.blockReader.Ready(ctxWithTimeout); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			e.logger.Trace("ethereumExecutionModule.Ready: context deadline exceeded")
			return false, nil
		}
		return false, err
	}

	if !e.semaphore.TryAcquire(1) {
		e.logger.Trace("ethereumExecutionModule.Ready: ExecutionStatus_Busy")
		return false, nil
	}
	defer e.semaphore.Release(1)
	return true, nil
}

func (e *ExecModule) HasBlock(ctx context.Context, blockHash *common.Hash, _ *uint64) (bool, error) {
	if blockHash == nil {
		return false, errors.New("block hash is nil, HasBlock supports lookup by hash only")
	}
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	num, _ := e.blockReader.HeaderNumber(ctx, tx, *blockHash)
	if num == nil {
		return false, nil
	}
	if *num <= e.blockReader.FrozenBlocks() {
		return true, nil
	}
	has, err := tx.Has(kv.Headers, dbutils.HeaderKey(*num, *blockHash))
	if err != nil {
		return false, err
	}
	if !has {
		return false, nil
	}
	has, err = tx.Has(kv.BlockBody, dbutils.HeaderKey(*num, *blockHash))
	if err != nil {
		return false, err
	}
	return has, nil
}
