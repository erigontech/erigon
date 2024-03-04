package sync

import (
	"context"

	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/core/types"
)

type ExecutionClient interface {
	InsertBlocks(ctx context.Context, headers []*types.Header) error
	UpdateForkChoice(ctx context.Context, tip *types.Header) error
	CurrentHeader(ctx context.Context) (*types.Header, error)
}

type executionClient struct {
	engine execution_client.ExecutionEngine
}

func NewExecutionClient(engine execution_client.ExecutionEngine) ExecutionClient {
	return &executionClient{engine}
}

func (e *executionClient) InsertBlocks(ctx context.Context, headers []*types.Header) error {
	//TODO implement me
	panic("implement me")
}

func (e *executionClient) UpdateForkChoice(ctx context.Context, tip *types.Header) error {
	//TODO implement me
	panic("implement me")
}

func (e *executionClient) CurrentHeader(ctx context.Context) (*types.Header, error) {
	panic("implement me")
}
