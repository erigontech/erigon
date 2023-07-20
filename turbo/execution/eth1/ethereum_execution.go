package eth1

import (
	"context"

	"github.com/anacrolix/sync"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/services"
)

// EthereumExecutionModule describes ethereum execution logic and indexing.
type EthereumExecutionModule struct {
	// Snapshots + MDBX
	blockReader services.FullBlockReader

	// MDBX database
	db                kv.RwDB // main database
	lock              sync.Mutex
	executionPipeline *stagedsync.Sync

	logger log.Logger

	execution.UnimplementedExecutionServer
}

func NewEthereumExecutionModule(blockReader services.FullBlockReader, db kv.RwDB, executionPipeline *stagedsync.Sync, logger log.Logger) *EthereumExecutionModule {
	return &EthereumExecutionModule{
		blockReader:       blockReader,
		db:                db,
		executionPipeline: executionPipeline,
		logger:            logger,
	}
}

func (e *EthereumExecutionModule) getHeader(ctx context.Context, tx kv.Tx, blockHash libcommon.Hash, blockNumber uint64) (*types.Header, error) {
	if e.blockReader == nil {
		return rawdb.ReadHeader(tx, blockHash, blockNumber), nil
	}
	return e.blockReader.Header(ctx, tx, blockHash, blockNumber)
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

func (e *EthereumExecutionModule) UpdateForkChoice(ctx context.Context, req *types2.H256) (*execution.ForkChoiceReceipt, error) {
	type canonicalEntry struct {
		hash   libcommon.Hash
		number uint64
	}
	e.lock.Lock()
	defer e.lock.Unlock()
	tx, err := e.db.BeginRw(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockHash := gointerfaces.ConvertH256ToHash(req)
	// Step one, find reconnection point, and mark all of those headers as canonical.
	fcuHeader, err := e.blockReader.HeaderByHash(ctx, tx, blockHash)
	if err != nil {
		return nil, err
	}
	// If we dont have it, too bad
	if fcuHeader == nil {
		return &execution.ForkChoiceReceipt{
			Status:          execution.ValidationStatus_MissingSegment,
			LatestValidHash: &types2.H256{},
		}, nil
	}
	currentParentHash := fcuHeader.ParentHash
	currentParentNumber := fcuHeader.Number.Uint64() - 1
	isCanonicalHash, err := rawdb.IsCanonicalHash(tx, currentParentHash, currentParentNumber)
	if err != nil {
		return nil, err
	}
	// Find such point, and collect all hashes
	newCanonicals := make([]*canonicalEntry, 0, 2048)
	newCanonicals = append(newCanonicals, &canonicalEntry{
		hash:   fcuHeader.Hash(),
		number: fcuHeader.Number.Uint64(),
	})
	for !isCanonicalHash {
		newCanonicals = append(newCanonicals, &canonicalEntry{
			hash:   currentParentHash,
			number: currentParentNumber,
		})
		currentHeader, err := e.blockReader.Header(ctx, tx, currentParentHash, currentParentNumber)
		if err != nil {
			return nil, err
		}
		if currentHeader == nil {
			return &execution.ForkChoiceReceipt{
				Status:          execution.ValidationStatus_MissingSegment,
				LatestValidHash: &types2.H256{},
			}, nil
		}
		currentParentHash = currentHeader.ParentHash
		currentParentNumber = currentHeader.Number.Uint64() - 1
		isCanonicalHash, err = rawdb.IsCanonicalHash(tx, currentParentHash, currentParentNumber)
		if err != nil {
			return nil, err
		}
	}
	if currentParentNumber != fcuHeader.Number.Uint64()-1 {
		e.executionPipeline.UnwindTo(currentParentNumber, libcommon.Hash{})
	}
	// Run the unwind
	if err := e.executionPipeline.RunUnwind(e.db, tx); err != nil {
		return nil, err
	}
	// Mark all new canonicals as canonicals
	for _, canonicalSegment := range newCanonicals {
		if err := rawdb.WriteCanonicalHash(tx, canonicalSegment.hash, canonicalSegment.number); err != nil {
			return nil, err
		}
	}
	// Set Progress for headers and bodies accordingly.
	if err := stages.SaveStageProgress(tx, stages.Headers, fcuHeader.Number.Uint64()); err != nil {
		return nil, err
	}
	if err := stages.SaveStageProgress(tx, stages.Bodies, fcuHeader.Number.Uint64()); err != nil {
		return nil, err
	}
	if err = rawdb.WriteHeadHeaderHash(tx, blockHash); err != nil {
		return nil, err
	}
	// Run the forkchoice
	if err := e.executionPipeline.Run(e.db, tx, false); err != nil {
		return nil, err
	}
	// if head hash was set then success otherwise no
	headHash := rawdb.ReadHeadBlockHash(tx)
	headNumber := rawdb.ReadHeaderNumber(tx, headHash)
	if headNumber != nil && e.logger != nil {
		e.logger.Info("Current forkchoice", "hash", headHash, "number", *headNumber)
	}
	status := execution.ValidationStatus_Success
	if headHash != blockHash {
		status = execution.ValidationStatus_BadBlock
	}
	return &execution.ForkChoiceReceipt{
		LatestValidHash: gointerfaces.ConvertHashToH256(headHash),
		Status:          status,
	}, tx.Commit()
}

// Missing: NewPayload, AssembleBlock
