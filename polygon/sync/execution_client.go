package sync

import (
	"context"

	executionclient "github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/core/types"
)

type ExecutionClient interface {
	InsertBlocks(ctx context.Context, blocks []*types.Block) error
	UpdateForkChoice(ctx context.Context, tip *types.Header, finalizedHeader *types.Header) error
	CurrentHeader(ctx context.Context) (*types.Header, error)
}

type executionClient struct {
	engine executionclient.ExecutionEngine
}

func NewExecutionClient(engine executionclient.ExecutionEngine) ExecutionClient {
	return &executionClient{engine}
}

func (e *executionClient) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
	return e.engine.InsertBlocks(ctx, blocks, true)
}

func (e *executionClient) UpdateForkChoice(_ context.Context, _ *types.Header, _ *types.Header) error {
	// TODO - not ready for execution - missing state sync event and span data - uncomment once ready
	//return e.engine.ForkChoiceUpdate(ctx, finalizedHeader.Hash(), tip.Hash())
	return nil
}

func (e *executionClient) CurrentHeader(ctx context.Context) (*types.Header, error) {
	return e.engine.CurrentHeader(ctx)
}
