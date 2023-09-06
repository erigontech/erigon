package eth1

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_utils"
)

func (e *EthereumExecutionModule) InsertBlocks(ctx context.Context, req *execution.InsertBlocksRequest) (*execution.InsertionResult, error) {
	if !e.semaphore.TryAcquire(1) {
		return &execution.InsertionResult{
			Result: execution.ExecutionStatus_Busy,
		}, nil
	}
	defer e.semaphore.Release(1)
	tx, err := e.db.BeginRw(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: could not begin transaction: %s", err)
	}
	defer tx.Rollback()
	e.forkValidator.ClearWithUnwind(e.accumulator, e.stateChangeConsumer)

	for _, block := range req.Blocks {
		header, err := eth1_utils.HeaderRpcToHeader(block.Header)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: cannot convert headers: %s", err)
		}
		body := eth1_utils.ConvertRawBlockBodyFromRpc(block.Body)
		// Parent's total difficulty
		parentTd, err := rawdb.ReadTd(tx, header.ParentHash, header.Number.Uint64()-1)
		if err != nil || parentTd == nil {
			return nil, fmt.Errorf("parent's total difficulty not found with hash %x and height %d: %v", header.ParentHash, header.Number.Uint64()-1, err)
		}
		// Sum TDs.
		td := parentTd.Add(parentTd, header.Difficulty)
		if err := rawdb.WriteHeader(tx, header); err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertHeaders: could not insert: %s", err)
		}
		if err := rawdb.WriteTd(tx, header.Hash(), header.Number.Uint64(), td); err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertHeaders: could not insert: %s", err)
		}
		if _, err := rawdb.WriteRawBodyIfNotExists(tx, header.Hash(), header.Number.Uint64(), body); err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: could not insert: %s", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertHeaders: could not commit: %s", err)
	}

	return &execution.InsertionResult{
		Result: execution.ExecutionStatus_Success,
	}, tx.Commit()
}
