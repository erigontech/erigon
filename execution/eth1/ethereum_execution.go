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

package eth1

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/gointerfaces"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
)

const maxBlocksLookBehind = 32

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
	lastParameters *core.BlockBuilderParameters
	builderFunc    builder.BlockBuilderFunc
	builders       map[uint64]*builder.BlockBuilder

	// Changes accumulator
	hook                *stages.Hook
	accumulator         *shards.Accumulator
	recentLogs          *shards.RecentLogs
	stateChangeConsumer shards.StateChangeConsumer

	// configuration
	config  *chain.Config
	syncCfg ethconfig.Sync
	// consensus
	engine consensus.Engine

	doingPostForkchoice atomic.Bool

	// metrics for average mgas/sec
	avgMgasSec float64

	execution.UnimplementedExecutionServer
}

func NewEthereumExecutionModule(blockReader services.FullBlockReader, db kv.TemporalRwDB,
	executionPipeline *stagedsync.Sync, forkValidator *engine_helpers.ForkValidator,
	config *chain.Config, builderFunc builder.BlockBuilderFunc,
	hook *stages.Hook, accumulator *shards.Accumulator,
	recentLogs *shards.RecentLogs,
	stateChangeConsumer shards.StateChangeConsumer,
	logger log.Logger, engine consensus.Engine,
	syncCfg ethconfig.Sync,
	ctx context.Context,
) *EthereumExecutionModule {
	return &EthereumExecutionModule{
		blockReader:         blockReader,
		db:                  db,
		executionPipeline:   executionPipeline,
		logger:              logger,
		forkValidator:       forkValidator,
		builders:            make(map[uint64]*builder.BlockBuilder),
		builderFunc:         builderFunc,
		config:              config,
		semaphore:           semaphore.NewWeighted(1),
		hook:                hook,
		accumulator:         accumulator,
		recentLogs:          recentLogs,
		stateChangeConsumer: stateChangeConsumer,
		engine:              engine,
		syncCfg:             syncCfg,
		bacgroundCtx:        ctx,
	}
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

func (e *EthereumExecutionModule) unwindToCommonCanonical(tx kv.RwTx, header *types.Header) error {
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
	if err := e.hook.BeforeRun(tx, true); err != nil {
		return err
	}
	if err := e.executionPipeline.UnwindTo(currentHeader.Number.Uint64(), stagedsync.ExecUnwind, tx); err != nil {
		return err
	}
	if err := e.executionPipeline.RunUnwind(nil, wrap.NewTxContainer(tx, nil)); err != nil {
		return err
	}
	return nil
}

func (e *EthereumExecutionModule) ValidateChain(ctx context.Context, req *execution.ValidationRequest) (*execution.ValidationReceipt, error) {
	if !e.semaphore.TryAcquire(1) {
		e.logger.Trace("ethereumExecutionModule.ValidateChain: ExecutionStatus_Busy")
		return &execution.ValidationReceipt{
			LatestValidHash:  gointerfaces.ConvertHashToH256(common.Hash{}),
			ValidationStatus: execution.ExecutionStatus_Busy,
		}, nil
	}
	defer e.semaphore.Release(1)

	e.hook.LastNewBlockSeen(req.Number) // used by eth_syncing
	e.forkValidator.ClearWithUnwind(e.accumulator, e.stateChangeConsumer)
	blockHash := gointerfaces.ConvertH256ToHash(req.Hash)

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
		currentBlockNumber = rawdb.ReadCurrentBlockNumber(tx)
		return nil
	}); err != nil {
		return nil, err
	}
	if header == nil || body == nil {
		return &execution.ValidationReceipt{
			LatestValidHash:  gointerfaces.ConvertHashToH256(common.Hash{}),
			ValidationStatus: execution.ExecutionStatus_MissingSegment,
		}, nil
	}

	if math.AbsoluteDifference(*currentBlockNumber, req.Number) >= maxBlocksLookBehind {
		return &execution.ValidationReceipt{
			ValidationStatus: execution.ExecutionStatus_TooFarAway,
			LatestValidHash:  gointerfaces.ConvertHashToH256(common.Hash{}),
		}, nil
	}

	if err := e.db.Update(ctx, func(tx kv.RwTx) error {
		return e.unwindToCommonCanonical(tx, header)
	}); err != nil {
		return nil, err
	}

	tx, err := e.db.BeginRwNosync(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	status, lvh, validationError, criticalError := e.forkValidator.ValidatePayload(tx, header, body.RawBody(), e.logger)
	if criticalError != nil {
		return nil, criticalError
	}
	// Throw away the tx and start a new one (do not persist changes to the canonical chain)
	tx.Rollback()
	tx, err = e.db.BeginRwNosync(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// if the block is deemed invalid then we delete it. perhaps we want to keep bad blocks and just keep an index of bad ones.
	validationStatus := execution.ExecutionStatus_Success
	if status == engine_types.AcceptedStatus {
		validationStatus = execution.ExecutionStatus_MissingSegment
	}
	isInvalidChain := status == engine_types.InvalidStatus || status == engine_types.InvalidBlockHashStatus || validationError != nil
	if isInvalidChain && (lvh != common.Hash{}) && lvh != blockHash {
		if err := e.purgeBadChain(ctx, tx, lvh, blockHash); err != nil {
			return nil, err
		}
	}
	if isInvalidChain {
		e.logger.Warn("ethereumExecutionModule.ValidateChain: chain is invalid", "hash", common.Hash(blockHash))
		validationStatus = execution.ExecutionStatus_BadBlock
	}
	validationReceipt := &execution.ValidationReceipt{
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

func (e *EthereumExecutionModule) Start(ctx context.Context) {
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		if !errors.Is(err, context.Canceled) {
			e.logger.Error("Could not start execution service", "err", err)
		}
		return
	}
	defer e.semaphore.Release(1)

	if err := stages.ProcessFrozenBlocks(ctx, e.db, e.blockReader, e.executionPipeline, nil); err != nil {
		if !errors.Is(err, context.Canceled) {
			e.logger.Error("Could not start execution service", "err", err)
		}
	}
}

func (e *EthereumExecutionModule) Ready(ctx context.Context, _ *emptypb.Empty) (*execution.ReadyResponse, error) {

	// setup a timeout for the context to avoid waiting indefinitely
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	if err := <-e.blockReader.Ready(ctxWithTimeout); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			e.logger.Trace("ethereumExecutionModule.Ready: context deadline exceeded")
			return &execution.ReadyResponse{Ready: false}, nil
		}
		return &execution.ReadyResponse{Ready: false}, err
	}

	if !e.semaphore.TryAcquire(1) {
		e.logger.Trace("ethereumExecutionModule.Ready: ExecutionStatus_Busy")
		return &execution.ReadyResponse{Ready: false}, nil
	}
	defer e.semaphore.Release(1)
	return &execution.ReadyResponse{Ready: true}, nil
}

func (e *EthereumExecutionModule) HasBlock(ctx context.Context, in *execution.GetSegmentRequest) (*execution.HasBlockResponse, error) {
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
		return &execution.HasBlockResponse{HasBlock: false}, nil
	}
	if *num <= e.blockReader.FrozenBlocks() {
		return &execution.HasBlockResponse{HasBlock: true}, nil
	}
	has, err := tx.Has(kv.Headers, dbutils.HeaderKey(*num, blockHash))
	if err != nil {
		return nil, err
	}
	if !has {
		return &execution.HasBlockResponse{HasBlock: false}, nil
	}
	has, err = tx.Has(kv.BlockBody, dbutils.HeaderKey(*num, blockHash))
	if err != nil {
		return nil, err
	}
	return &execution.HasBlockResponse{HasBlock: has}, nil
}
