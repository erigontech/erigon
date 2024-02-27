package sync

import (
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/core/types"
)

type ExecutionClient interface {
	InsertBlocks(headers []*types.Header) error
	UpdateForkChoice(tip *types.Header) error
}

type executionClient struct {
	engine execution_client.ExecutionEngine
}

func NewExecutionClient(engine execution_client.ExecutionEngine) ExecutionClient {
	return &executionClient{engine}
}

func (e *executionClient) InsertBlocks(headers []*types.Header) error {
	//TODO implement me
	panic("implement me")
}

func (e *executionClient) UpdateForkChoice(tip *types.Header) error {
	//TODO implement me
	panic("implement me")
}
