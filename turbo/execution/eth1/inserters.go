package eth1

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_utils"
)

func (e *EthereumExecutionModule) InsertBodies(ctx context.Context, req *execution.InsertBodiesRequest) (*execution.InsertionResult, error) {
	if !e.semaphore.TryAcquire(1) {
		return &execution.InsertionResult{
			Result: execution.ValidationStatus_Busy,
		}, nil
	}
	defer e.semaphore.Release(1)
	tx, err := e.db.BeginRw(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertBodies: could not begin transaction: %s", err)
	}
	defer tx.Rollback()
	for _, grpcBody := range req.Bodies {
		var ok bool
		if ok, err = rawdb.WriteRawBodyIfNotExists(tx, gointerfaces.ConvertH256ToHash(grpcBody.BlockHash), grpcBody.BlockNumber, eth1_utils.ConvertRawBlockBodyFromRpc(grpcBody)); err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBodies: could not insert: %s", err)
		}
		if e.historyV3 && ok {
			if err := rawdb.AppendCanonicalTxNums(tx, grpcBody.BlockNumber); err != nil {
				return nil, fmt.Errorf("ethereumExecutionModule.InsertBodies: could not insert: %s", err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertBodies: could not commit: %s", err)
	}

	return &execution.InsertionResult{
		Result: execution.ValidationStatus_Success,
	}, tx.Commit()
}

func (e *EthereumExecutionModule) InsertHeaders(ctx context.Context, req *execution.InsertHeadersRequest) (*execution.InsertionResult, error) {
	if !e.semaphore.TryAcquire(1) {
		return &execution.InsertionResult{
			Result: execution.ValidationStatus_Busy,
		}, nil
	}
	defer e.semaphore.Release(1)
	tx, err := e.db.BeginRw(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertHeaders: could not begin transaction: %s", err)
	}
	defer tx.Rollback()
	for _, grpcHeader := range req.Headers {
		header, err := eth1_utils.HeaderRpcToHeader(grpcHeader)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertHeaders: cannot convert headers: %s", err)
		}
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
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertHeaders: could not commit: %s", err)
	}

	return &execution.InsertionResult{
		Result: execution.ValidationStatus_Success,
	}, tx.Commit()
}
