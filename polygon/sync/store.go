// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package sync

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
)

//go:generate mockgen -typed=true -destination=./store_mock.go -package=sync . Store
type Store interface {
	// InsertBlocks queues blocks for writing into the local canonical chain.
	InsertBlocks(ctx context.Context, blocks []*types.Block) error
	// Flush makes sure that all queued blocks have been written.
	Flush(ctx context.Context) error
	// Run performs the block writing.
	Run(ctx context.Context) error
}

type executionStore interface {
	InsertBlocks(ctx context.Context, blocks []*types.Block) error
	CurrentHeader(ctx context.Context) (*types.Header, error)
	GetHeader(ctx context.Context, blockNum uint64) (*types.Header, error)
}

type bridgeStore interface {
	ProcessNewBlocks(ctx context.Context, blocks []*types.Block) error
	InitialBlockReplayNeeded(ctx context.Context) (uint64, bool, error)
	ReplayInitialBlock(ctx context.Context, block *types.Block) error
}

type executionClientStore struct {
	logger         log.Logger
	executionStore executionStore
	bridgeStore    bridgeStore
	queue          chan []*types.Block
	// tasksCount includes both tasks pending in the queue and a task that was taken and hasn't finished yet
	tasksCount atomic.Int32
	// tasksDoneSignal gets sent a value when tasksCount becomes 0
	tasksDoneSignal chan bool
	blockReplayDone bool
}

func NewStore(logger log.Logger, executionStore executionStore, bridgeStore bridgeStore) Store {
	return &executionClientStore{
		logger:          logger,
		executionStore:  executionStore,
		bridgeStore:     bridgeStore,
		queue:           make(chan []*types.Block),
		tasksDoneSignal: make(chan bool, 1),
	}
}

func (s *executionClientStore) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
	s.tasksCount.Add(1)
	select {
	case s.queue <- blocks:
		return nil
	case <-ctx.Done():
		// compensate since a task has not enqueued
		s.tasksCount.Add(-1)
		return ctx.Err()
	}
}

func (s *executionClientStore) Flush(ctx context.Context) error {
	for s.tasksCount.Load() > 0 {
		select {
		case _, ok := <-s.tasksDoneSignal:
			if !ok {
				return errors.New("executionClientStore.Flush failed because ExecutionClient.InsertBlocks failed")
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (s *executionClientStore) Run(ctx context.Context) error {
	s.logger.Debug(syncLogPrefix("running execution client store component"))

	for {
		select {
		case blocks := <-s.queue:
			if err := s.insertBlocks(ctx, blocks); err != nil {
				close(s.tasksDoneSignal)
				return err
			}
			if s.tasksCount.Load() == 0 {
				select {
				case s.tasksDoneSignal <- true:
				default:
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *executionClientStore) insertBlocks(ctx context.Context, blocks []*types.Block) error {
	defer s.tasksCount.Add(-1)
	insertStartTime := time.Now()

	if !s.blockReplayDone {
		if err := s.bridgeReplayInitialBlockIfNeeded(ctx); err != nil {
			return err
		}

		s.blockReplayDone = true
	}

	err := s.executionStore.InsertBlocks(ctx, blocks)
	if err != nil {
		return err
	}

	err = s.bridgeStore.ProcessNewBlocks(ctx, blocks)
	if err != nil {
		return err
	}

	if len(blocks) > 0 {
		s.logger.Debug(syncLogPrefix("inserted blocks"), "from", blocks[0].NumberU64(), "to", blocks[len(blocks)-1].NumberU64(), "duration", time.Since(insertStartTime))
	}

	return nil
}

// bridgeReplayInitialBlockIfNeeded is only needed on very first node startup and just before
// the first call to ProcessNewBlocks.
//
// The Bridge needs that to set internal state which it cannot fully infer on its own
// since it has no access to the block information via the execution engine. This is
// a conscious design decision.
//
// The bridge store is in control of determining whether and which block need replaying.
func (s *executionClientStore) bridgeReplayInitialBlockIfNeeded(ctx context.Context) error {
	initialBlockNum, replayNeeded, err := s.bridgeStore.InitialBlockReplayNeeded(ctx)
	if err != nil {
		return err
	}
	if !replayNeeded {
		return nil
	}

	initialHeader, err := s.executionStore.GetHeader(ctx, initialBlockNum)
	if err != nil {
		return err
	}

	s.logger.Debug(
		syncLogPrefix("replaying initial block for bridge store"),
		"blockNum", initialHeader.Number.Uint64(),
	)

	if err = s.bridgeStore.ReplayInitialBlock(ctx, types.NewBlockWithHeader(initialHeader)); err != nil {
		return err
	}

	// in case execution tip is ahead of the bridge tip, replay blocks to fill gaps
	executionTip, err := s.executionStore.CurrentHeader(ctx)
	if err != nil {
		return err
	}

	executionTipNum := executionTip.Number.Uint64()
	if executionTipNum <= initialBlockNum {
		return nil
	}

	blocksCount := executionTipNum - initialBlockNum
	s.logger.Debug(
		syncLogPrefix("replaying post initial blocks for bridge store to fill gap with execution"),
		"blocks", blocksCount,
		"executionTip", executionTipNum,
	)

	blocks := make([]*types.Block, 0, blocksCount)
	for blockNum := initialBlockNum + 1; blockNum <= executionTipNum; blockNum++ {
		header, err := s.executionStore.GetHeader(ctx, blockNum)
		if err != nil {
			return err
		}

		blocks = append(blocks, types.NewBlockWithHeader(header))
	}

	return s.bridgeStore.ProcessNewBlocks(ctx, blocks)
}
