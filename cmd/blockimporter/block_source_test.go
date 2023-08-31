package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/stretchr/testify/require"
)

type BlocksOrError struct {
	blocks []types.Block
	err    error
}

func NewBlocks(blocks []types.Block) BlocksOrError {
	return BlocksOrError{
		blocks: blocks,
	}
}

func NewError(err error) BlocksOrError {
	return BlocksOrError{
		err: err,
	}
}

type MockBlockSource struct {
	results []BlocksOrError
}

func (blockSource *MockBlockSource) PollBlocks(fromBlock uint64) ([]types.Block, error) {
	if len(blockSource.results) == 0 {
		return nil, nil
	}

	blockOrErr := blockSource.results[0]
	blockSource.results = blockSource.results[1:]
	return blockOrErr.blocks, blockOrErr.err
}

func (blockSource *MockBlockSource) GetInitialBalances() ([]BalanceEntry, error) {
	return nil, nil
}

func TestRetry(t *testing.T) {
	blockSource := MockBlockSource{
		results: []BlocksOrError{
			NewBlocks(nil),
			NewError(fmt.Errorf("first error")),
			NewError(fmt.Errorf("second error")),
			NewBlocks([]types.Block{{}}),
			NewError(fmt.Errorf("first error")),
			NewError(fmt.Errorf("second error")),
			NewError(fmt.Errorf("third error")),
		},
	}

	wrappedSource := WithRetries(&blockSource, 3, time.Millisecond, make(chan struct{}))

	blocks, err := wrappedSource.PollBlocks(0)
	require.Empty(t, err)
	require.Empty(t, blocks)

	blocks, err = wrappedSource.PollBlocks(0)
	require.Empty(t, err)
	require.Equal(t, []types.Block{{}}, blocks)

	blocks, err = wrappedSource.PollBlocks(0)
	require.Equal(t, fmt.Errorf("third error"), err)
	require.Empty(t, blocks)
}

func TestPoll(t *testing.T) {
	blockSource := MockBlockSource{
		results: []BlocksOrError{
			NewBlocks(nil),
			NewBlocks([]types.Block{{}}),
			NewBlocks([]types.Block{}),
			NewBlocks(nil),
			NewBlocks([]types.Block{{}, {}}),
			NewError(fmt.Errorf("some error")),
		},
	}

	wrappedSource := WithPollInterval(&blockSource, time.Millisecond, make(chan struct{}))

	blocks, err := wrappedSource.PollBlocks(0)
	require.Empty(t, err)
	require.Equal(t, []types.Block{{}}, blocks)

	blocks, err = wrappedSource.PollBlocks(0)
	require.Empty(t, err)
	require.Equal(t, []types.Block{{}, {}}, blocks)

	blocks, err = wrappedSource.PollBlocks(0)
	require.Equal(t, fmt.Errorf("some error"), err)
	require.Empty(t, blocks)
}
