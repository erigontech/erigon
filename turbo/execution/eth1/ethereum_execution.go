package eth1

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/semaphore"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/turbo/builder"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/stages"
)

// EthereumExecutionModule describes ethereum execution logic and indexing.
type EthereumExecutionModule struct {
	// Snapshots + MDBX
	blockReader services.FullBlockReader

	// MDBX database
	db                kv.RwDB // main database
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
	stateChangeConsumer shards.StateChangeConsumer

	// configuration
	config    *chain.Config
	historyV3 bool

	execution.UnimplementedExecutionServer
}

func NewEthereumExecutionModule(blockReader services.FullBlockReader, db kv.RwDB, executionPipeline *stagedsync.Sync, forkValidator *engine_helpers.ForkValidator,
	config *chain.Config, builderFunc builder.BlockBuilderFunc, hook *stages.Hook, accumulator *shards.Accumulator, stateChangeConsumer shards.StateChangeConsumer, logger log.Logger, historyV3 bool) *EthereumExecutionModule {
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
		stateChangeConsumer: stateChangeConsumer,
	}
}

func (e *EthereumExecutionModule) getHeader(ctx context.Context, tx kv.Tx, blockHash libcommon.Hash, blockNumber uint64) (*types.Header, error) {
	if e.blockReader == nil {
		return rawdb.ReadHeader(tx, blockHash, blockNumber), nil
	}
	return e.blockReader.Header(ctx, tx, blockHash, blockNumber)
}

func (e *EthereumExecutionModule) getTD(ctx context.Context, tx kv.Tx, blockHash libcommon.Hash, blockNumber uint64) (*big.Int, error) {
	return rawdb.ReadTd(tx, blockHash, blockNumber)

}

func (e *EthereumExecutionModule) getBody(ctx context.Context, tx kv.Tx, blockHash libcommon.Hash, blockNumber uint64) (*types.Body, error) {
	if e.blockReader == nil {
		body, _, _ := rawdb.ReadBody(tx, blockHash, blockNumber)
		return body, nil
	}
	return e.blockReader.BodyWithTransactions(ctx, tx, blockHash, blockNumber)
}

func (e *EthereumExecutionModule) canonicalHash(ctx context.Context, tx kv.Tx, blockNumber uint64) (libcommon.Hash, error) {
	if e.blockReader == nil {
		return rawdb.ReadCanonicalHash(tx, blockNumber)
	}
	return e.blockReader.CanonicalHash(ctx, tx, blockNumber)
}

// Remaining

func (e *EthereumExecutionModule) ValidateChain(ctx context.Context, req *execution.ValidationRequest) (*execution.ValidationReceipt, error) {
	if !e.semaphore.TryAcquire(1) {
		return &execution.ValidationReceipt{
			LatestValidHash:  gointerfaces.ConvertHashToH256(libcommon.Hash{}),
			ValidationStatus: execution.ExecutionStatus_Busy,
		}, nil
	}
	defer e.semaphore.Release(1)
	tx, err := e.db.BeginRw(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	e.forkValidator.ClearWithUnwind(tx, e.accumulator, e.stateChangeConsumer)
	blockHash := gointerfaces.ConvertH256ToHash(req.Hash)
	header, err := e.blockReader.Header(ctx, tx, blockHash, req.Number)
	if err != nil {
		return nil, err
	}

	body, err := e.blockReader.BodyWithTransactions(ctx, tx, blockHash, req.Number)
	if err != nil {
		return nil, err
	}

	if header == nil || body == nil {
		return &execution.ValidationReceipt{
			LatestValidHash:  gointerfaces.ConvertHashToH256(libcommon.Hash{}),
			MissingHash:      req.Hash,
			ValidationStatus: execution.ExecutionStatus_MissingSegment,
		}, nil
	}

	status, lvh, validationError, criticalError := e.forkValidator.ValidatePayload(tx, header, body.RawBody(), false)
	if criticalError != nil {
		return nil, criticalError
	}

	// if the block is deemed invalid then we delete it. perhaps we want to keep bad blocks and just keep an index of bad ones.
	validationStatus := execution.ExecutionStatus_Success
	if status == engine_types.AcceptedStatus {
		validationStatus = execution.ExecutionStatus_MissingSegment
	}
	if status == engine_types.InvalidStatus || status == engine_types.InvalidBlockHashStatus || validationError != nil {
		e.logger.Warn("ethereumExecutionModule.ValidateChain: chain %x is invalid. reason %s", blockHash, err)
		validationStatus = execution.ExecutionStatus_BadBlock
		rawdb.DeleteHeader(tx, blockHash, header.Number.Uint64())
	}
	return &execution.ValidationReceipt{
		ValidationStatus: validationStatus,
		LatestValidHash:  gointerfaces.ConvertHashToH256(lvh),
		MissingHash:      gointerfaces.ConvertHashToH256(libcommon.Hash{}), // TODO: implement
	}, tx.Commit()
}
