package eth1

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon/core/rawdb"
)

func (e *EthereumExecutionModule) InsertBodies(ctx context.Context, req *execution.InsertBodiesRequest) (*execution.EmptyMessage, error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	tx, err := e.db.BeginRw(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertBodies: could not begin transaction: %s", err)
	}
	defer tx.Rollback()
	for _, grpcBody := range req.Bodies {
		if _, err := rawdb.WriteRawBodyIfNotExists(tx, gointerfaces.ConvertH256ToHash(grpcBody.BlockHash), grpcBody.BlockNumber, ConvertRawBlockBodyFromRpc(grpcBody)); err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBodies: could not insert: %s", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertBodies: could not commit: %s", err)
	}

	return &execution.EmptyMessage{}, tx.Commit()
}

func (e *EthereumExecutionModule) InsertHeaders(ctx context.Context, req *execution.InsertHeadersRequest) (*execution.EmptyMessage, error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	tx, err := e.db.BeginRw(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertHeaders: could not begin transaction: %s", err)
	}
	defer tx.Rollback()
	for _, grpcHeader := range req.Headers {
		header, err := HeaderRpcToHeader(grpcHeader)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertHeaders: cannot convert headers: %s", err)
		}
		if err := rawdb.WriteHeader(tx, header); err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertHeaders: could not insert: %s", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertHeaders: could not commit: %s", err)
	}

	return &execution.EmptyMessage{}, tx.Commit()
}
