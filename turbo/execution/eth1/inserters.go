package eth1

import (
	"context"
	"fmt"
	"math/big"
	"reflect"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/metrics"
	execution "github.com/ledgerwatch/erigon-lib/gointerfaces/executionproto"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_utils"
)

func (s *EthereumExecutionModule) validatePayloadBlobs(expectedBlobHashes []libcommon.Hash, transactions []types.Transaction, blobGasUsed uint64) error {
	if expectedBlobHashes == nil {
		return &rpc.InvalidParamsError{Message: "nil blob hashes array"}
	}
	actualBlobHashes := []libcommon.Hash{}
	for _, txn := range transactions {
		actualBlobHashes = append(actualBlobHashes, txn.GetBlobHashes()...)
	}
	if len(actualBlobHashes) > int(s.config.GetMaxBlobsPerBlock()) || blobGasUsed > s.config.GetMaxBlobGasPerBlock() {
		return nil
	}
	if !reflect.DeepEqual(actualBlobHashes, expectedBlobHashes) {
		s.logger.Warn("[NewPayload] mismatch in blob hashes",
			"expectedBlobHashes", expectedBlobHashes, "actualBlobHashes", actualBlobHashes)
		return nil
	}
	return nil
}

func (e *EthereumExecutionModule) InsertBlocks(ctx context.Context, req *execution.InsertBlocksRequest) (*execution.InsertionResult, error) {
	if !e.semaphore.TryAcquire(1) {
		e.logger.Trace("ethereumExecutionModule.InsertBlocks: ExecutionStatus_Busy")
		return &execution.InsertionResult{
			Result: execution.ExecutionStatus_Busy,
		}, nil
	}
	defer e.semaphore.Release(1)
	e.forkValidator.ClearWithUnwind(e.accumulator, e.stateChangeConsumer)
	frozenBlocks := e.blockReader.FrozenBlocks()

	tx, err := e.db.BeginRw(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: could not begin transaction: %s", err)
	}
	defer tx.Rollback()

	for _, block := range req.Blocks {
		// Skip frozen blocks.
		if block.Header.BlockNumber < frozenBlocks {
			continue
		}
		header, err := eth1_utils.HeaderRpcToHeader(block.Header)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: cannot convert headers: %s", err)
		}
		body, err := eth1_utils.ConvertRawBlockBodyFromRpc(block.Body)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: cannot convert body: %s", err)
		}
		var parentTd *big.Int
		height := header.Number.Uint64()
		if height > 0 {
			// Parent's total difficulty
			parentTd, err = rawdb.ReadTd(tx, header.ParentHash, height-1)
			if err != nil || parentTd == nil {
				return nil, fmt.Errorf("parent's total difficulty not found with hash %x and height %d: %v", header.ParentHash, height-1, err)
			}
		} else {
			parentTd = big.NewInt(0)
		}

		metrics.UpdateBlockConsumerHeaderDownloadDelay(header.Time, height-1, e.logger)
		metrics.UpdateBlockConsumerBodyDownloadDelay(header.Time, height-1, e.logger)

		// Sum TDs.
		td := parentTd.Add(parentTd, header.Difficulty)
		if err := rawdb.WriteHeader(tx, header); err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertHeaders: writeHeader: %s", err)
		}
		if err := rawdb.WriteTd(tx, header.Hash(), height, td); err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertHeaders: writeTd: %s", err)
		}
		if _, err := rawdb.WriteRawBodyIfNotExists(tx, header.Hash(), height, body); err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeBody: %s", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertHeaders: could not commit: %s", err)
	}

	return &execution.InsertionResult{
		Result: execution.ExecutionStatus_Success,
	}, nil
}
