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
	"math/big"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/types/known/emptypb"

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
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/shards"
)

var ErrMissingChainSegment = errors.New("missing chain segment")

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

type Cache struct {
	execModule *EthereumExecutionModule
}

var _ kvcache.Cache = (*Cache)(nil)         // compile-time interface check
var _ kvcache.CacheView = (*CacheView)(nil) // compile-time interface check

func (c *Cache) View(_ context.Context, tx kv.TemporalTx) (kvcache.CacheView, error) {
	var context *execctx.SharedDomains
	if c.execModule != nil {
		c.execModule.lock.RLock()
		defer c.execModule.lock.RUnlock()
		context = c.execModule.currentContext
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

// EthereumExecutionModule describes ethereum execution logic and indexing.
type EthereumExecutionModule struct {
	bacgroundCtx context.Context
	// Snapshots + MDBX
	blockReader services.FullBlockReader

	// MDBX database
	db                kv.TemporalRwDB // main database
	semaphore         *semaphore.Weighted
	executionPipeline *stagedsync.Sync
	forkValidator     *engine_helpers.ForkValidator

	logger log.Logger
	// Block building
	nextPayloadId  uint64
	lastParameters *builder.Parameters
	builderFunc    builder.BlockBuilderFunc
	builders       map[uint64]*builder.BlockBuilder

	// Changes accumulator
	hook                *stageloop.Hook
	accumulator         *shards.Accumulator
	recentReceipts      *shards.RecentReceipts
	stateChangeConsumer shards.StateChangeConsumer

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

	// stateCache is a cache for state data (accounts, storage, code)
	stateCache *cache.StateCache

	executionproto.UnimplementedExecutionServer
}

func NewEthereumExecutionModule(ctx context.Context, blockReader services.FullBlockReader, db kv.TemporalRwDB,
	executionPipeline *stagedsync.Sync, forkValidator *engine_helpers.ForkValidator,
	config *chain.Config, builderFunc builder.BlockBuilderFunc,
	hook *stageloop.Hook, accumulator *shards.Accumulator,
	recentReceipts *shards.RecentReceipts,
	stateCache *Cache, stateChangeConsumer shards.StateChangeConsumer,
	logger log.Logger, engine rules.Engine,
	syncCfg ethconfig.Sync,
	fcuBackgroundPrune bool,
	fcuBackgroundCommit bool,
	onlySnapDownloadOnStart bool,
) *EthereumExecutionModule {
	domainCache := cache.NewDefaultStateCache()

	em := &EthereumExecutionModule{
		blockReader:             blockReader,
		db:                      db,
		executionPipeline:       executionPipeline,
		logger:                  logger,
		forkValidator:           forkValidator,
		builders:                make(map[uint64]*builder.BlockBuilder),
		builderFunc:             builderFunc,
		config:                  config,
		semaphore:               semaphore.NewWeighted(1),
		hook:                    hook,
		accumulator:             accumulator,
		recentReceipts:          recentReceipts,
		stateChangeConsumer:     stateChangeConsumer,
		engine:                  engine,
		syncCfg:                 syncCfg,
		bacgroundCtx:            ctx,
		fcuBackgroundPrune:      fcuBackgroundPrune,
		fcuBackgroundCommit:     fcuBackgroundCommit,
		onlySnapDownloadOnStart: onlySnapDownloadOnStart,
		stateCache:              domainCache,
	}

	if stateCache != nil {
		stateCache.execModule = em
	}
	return em
}

func (e *EthereumExecutionModule) getHeader(ctx context.Context, tx kv.Tx, blockHash common.Hash, blockNumber uint64) (*types.Header, error) {
	if e.blockReader == nil {
		return rawdb.ReadHeader(tx, blockHash, blockNumber), nil
	}

	return e.blockReader.Header(ctx, tx, blockHash, blockNumber)
}

func (e *EthereumExecutionModule) getTD(_ context.Context, tx kv.Tx, blockHash common.Hash, blockNumber uint64) (*big.Int, error) {
	return rawdb.ReadTd(tx, blockHash, blockNumber)

}

func (e *EthereumExecutionModule) getBody(ctx context.Context, tx kv.Tx, blockHash common.Hash, blockNumber uint64) (*types.Body, error) {
	if e.blockReader == nil {
		body, _, _ := rawdb.ReadBody(tx, blockHash, blockNumber)
		return body, nil
	}
	return e.blockReader.BodyWithTransactions(ctx, tx, blockHash, blockNumber)
}

func (e *EthereumExecutionModule) canonicalHash(ctx context.Context, tx kv.Tx, blockNumber uint64) (common.Hash, error) {
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

func (e *EthereumExecutionModule) unwindToCommonCanonical(sd *execctx.SharedDomains, tx kv.TemporalRwTx, header *types.Header) error {
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

	if err := e.executionPipeline.UnwindTo(unwindPoint, stagedsync.ExecUnwind, tx); err != nil {
		return err
	}
	if err := e.executionPipeline.RunUnwind(sd, tx); err != nil {
		return err
	}
	return nil
}

func (e *EthereumExecutionModule) ValidateChain(ctx context.Context, req *executionproto.ValidationRequest) (*executionproto.ValidationReceipt, error) {
	// Wait up to 2 seconds for the semaphore instead of failing instantly.
	// With parallel state flushing, the previous FcU's background commit may still
	// hold the semaphore. Instant failure causes SYNCING responses to the CL.
	semaCtx, semaCancel := context.WithTimeout(ctx, 2*time.Second)
	defer semaCancel()
	if err := e.semaphore.Acquire(semaCtx, 1); err != nil {
		e.logger.Trace("ethereumExecutionModule.ValidateChain: ExecutionStatus_Busy", "wait", "2s")
		return &executionproto.ValidationReceipt{
			LatestValidHash:  gointerfaces.ConvertHashToH256(common.Hash{}),
			ValidationStatus: executionproto.ExecutionStatus_Busy,
		}, nil
	}
	defer e.semaphore.Release(1)

	e.hook.LastNewBlockSeen(req.Number) // used by eth_syncing
	e.currentContext.ResetPendingUpdates()
	e.forkValidator.ClearWithUnwind(e.accumulator, e.stateChangeConsumer)
	blockHash := gointerfaces.ConvertH256ToHash(req.Hash)
	e.logger.Debug("[execmodule] validating chain", "number", req.Number, "hash", common.Hash(blockHash))
	var (
		header             *types.Header
		body               *types.Body
		currentBlockNumber *uint64
		err                error
	)
	if err := e.db.View(ctx, func(tx kv.Tx) error {
		header, err = e.blockReader.Header(ctx, tx, blockHash, req.Number)
		if err != nil {
			return err
		}

		body, err = e.blockReader.BodyWithTransactions(ctx, tx, blockHash, req.Number)
		if err != nil {
			return err
		}
		exec.AddHeaderAndBodyToGlobalReadAheader(ctx, e.db, header, body)
		currentBlockNumber = rawdb.ReadCurrentBlockNumber(tx)
		return nil
	}); err != nil {
		return nil, err
	}
	if header == nil || body == nil {
		return &executionproto.ValidationReceipt{
			LatestValidHash:  gointerfaces.ConvertHashToH256(common.Hash{}),
			ValidationStatus: executionproto.ExecutionStatus_MissingSegment,
		}, nil
	}

	if math.AbsoluteDifference(*currentBlockNumber, req.Number) >= e.syncCfg.MaxReorgDepth {
		return &executionproto.ValidationReceipt{
			ValidationStatus: executionproto.ExecutionStatus_TooFarAway,
			LatestValidHash:  gointerfaces.ConvertHashToH256(common.Hash{}),
		}, nil
	}

	tx, err := e.db.BeginTemporalRwNosync(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	doms, err := execctx.NewSharedDomains(ctx, tx, e.logger)
	if err != nil {
		return nil, err
	}

	// Set state cache in SharedDomains for use during state reading
	if e.stateCache != nil && dbg.UseStateCache {
		doms.SetStateCache(e.stateCache)
	}
	if err = e.unwindToCommonCanonical(doms, tx, header); err != nil {
		doms.Close()
		return nil, err
	}

	status, lvh, validationError, criticalError := e.forkValidator.ValidatePayload(ctx, doms, tx, header, body.RawBody(), e.logger)
	if criticalError != nil {
		return nil, criticalError
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
		return nil, err
	}
	defer tx.Rollback()

	// if the block is deemed invalid then we delete it. perhaps we want to keep bad blocks and just keep an index of bad ones.
	validationStatus := executionproto.ExecutionStatus_Success
	if status == engine_types.AcceptedStatus {
		validationStatus = executionproto.ExecutionStatus_MissingSegment
	}
	isInvalidChain := status == engine_types.InvalidStatus || status == engine_types.InvalidBlockHashStatus || validationError != nil
	if isInvalidChain && (lvh != common.Hash{}) && lvh != blockHash {
		if err := e.purgeBadChain(ctx, tx, lvh, blockHash); err != nil {
			return nil, err
		}
	}
	if isInvalidChain {
		e.logger.Warn("ethereumExecutionModule.ValidateChain: chain is invalid", "hash", common.Hash(blockHash))
		validationStatus = executionproto.ExecutionStatus_BadBlock
	}
	validationReceipt := &executionproto.ValidationReceipt{
		ValidationStatus: validationStatus,
		LatestValidHash:  gointerfaces.ConvertHashToH256(lvh),
	}
	if validationError != nil {
		validationReceipt.ValidationError = validationError.Error()
	}
	return validationReceipt, tx.Commit()
}

func (e *EthereumExecutionModule) purgeBadChain(ctx context.Context, tx kv.RwTx, latestValidHash, headHash common.Hash) error {
	tip, err := e.blockReader.HeaderNumber(ctx, tx, headHash)
	if err != nil {
		return err
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

func (e *EthereumExecutionModule) Start(ctx context.Context, hook *stageloop.Hook) {
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		if !errors.Is(err, context.Canceled) {
			e.logger.Error("Could not start execution service", "err", err)
		}
		return
	}
	defer e.semaphore.Release(1)

	if err := stageloop.ProcessFrozenBlocks(ctx, e.db, e.blockReader, e.executionPipeline, hook, e.onlySnapDownloadOnStart, e.logger); err != nil {
		if !errors.Is(err, context.Canceled) {
			e.logger.Error("Could not start execution service", "err", err)
		}
	}
}

func (e *EthereumExecutionModule) Ready(ctx context.Context, _ *emptypb.Empty) (*executionproto.ReadyResponse, error) {

	// setup a timeout for the context to avoid waiting indefinitely
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	if err := <-e.blockReader.Ready(ctxWithTimeout); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			e.logger.Trace("ethereumExecutionModule.Ready: context deadline exceeded")
			return &executionproto.ReadyResponse{Ready: false}, nil
		}
		return &executionproto.ReadyResponse{Ready: false}, err
	}

	if !e.semaphore.TryAcquire(1) {
		e.logger.Trace("ethereumExecutionModule.Ready: ExecutionStatus_Busy")
		return &executionproto.ReadyResponse{Ready: false}, nil
	}
	defer e.semaphore.Release(1)
	return &executionproto.ReadyResponse{Ready: true}, nil
}

func (e *EthereumExecutionModule) HasBlock(ctx context.Context, in *executionproto.GetSegmentRequest) (*executionproto.HasBlockResponse, error) {
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if in.BlockHash == nil {
		return nil, errors.New("block hash is nil, hasBlock support only by hash")
	}
	blockHash := gointerfaces.ConvertH256ToHash(in.BlockHash)

	num, _ := e.blockReader.HeaderNumber(ctx, tx, blockHash)
	if num == nil {
		return &executionproto.HasBlockResponse{HasBlock: false}, nil
	}
	if *num <= e.blockReader.FrozenBlocks() {
		return &executionproto.HasBlockResponse{HasBlock: true}, nil
	}
	has, err := tx.Has(kv.Headers, dbutils.HeaderKey(*num, blockHash))
	if err != nil {
		return nil, err
	}
	if !has {
		return &executionproto.HasBlockResponse{HasBlock: false}, nil
	}
	has, err = tx.Has(kv.BlockBody, dbutils.HeaderKey(*num, blockHash))
	if err != nil {
		return nil, err
	}
	return &executionproto.HasBlockResponse{HasBlock: has}, nil
}
