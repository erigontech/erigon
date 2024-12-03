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
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
)

//go:generate mockgen -typed=true -destination=./store_mock.go -package=sync . Store
type Store interface {
	// Prepare runs initialisation of the store
	Prepare(ctx context.Context) error
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

func NewStore(logger log.Logger, executionStore executionStore, bridgeStore bridgeStore) *ExecutionClientStore {
	return &ExecutionClientStore{
		logger:          logger,
		executionStore:  executionStore,
		bridgeStore:     bridgeStore,
		queue:           make(chan []*types.Block),
		tasksDoneSignal: make(chan bool, 1),
	}
}

type ExecutionClientStore struct {
	logger         log.Logger
	executionStore executionStore
	bridgeStore    bridgeStore
	queue          chan []*types.Block
	// tasksCount includes both tasks pending in the queue and a task that was taken and hasn't finished yet
	tasksCount atomic.Int32
	// tasksDoneSignal gets sent a value when tasksCount becomes 0
	tasksDoneSignal chan bool
	prepared        bool
	lastQueuedBlock uint64
}

func (s *ExecutionClientStore) Prepare(ctx context.Context) error {
	if s.prepared {
		return nil
	}

	executionTip, err := s.executionStore.CurrentHeader(ctx)
	if err != nil {
		return err
	}

	executionTipBlockNum := executionTip.Number.Uint64()
	if err := s.bridgeReplayInitialBlockIfNeeded(ctx, executionTipBlockNum); err != nil {
		return err
	}

	s.lastQueuedBlock = executionTipBlockNum
	s.prepared = true
	return nil
}

func (s *ExecutionClientStore) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
	if len(blocks) == 0 {
		return nil
	}

	if blocks[0].NumberU64() > s.lastQueuedBlock+1 {
		return fmt.Errorf("block gap inserted: expected: %d, have: %d", s.lastQueuedBlock+1, blocks[0].NumberU64())
	}

	if lastInserted := blocks[len(blocks)-1].NumberU64(); lastInserted > s.lastQueuedBlock {
		s.lastQueuedBlock = lastInserted
	}

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

func (s *ExecutionClientStore) Flush(ctx context.Context) error {
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

func (s *ExecutionClientStore) Run(ctx context.Context) error {
	s.logger.Info(syncLogPrefix("running execution client store component"))

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

func (s *ExecutionClientStore) insertBlocks(ctx context.Context, blocks []*types.Block) error {
	defer s.tasksCount.Add(-1)
	insertStartTime := time.Now()
	err := s.executionStore.InsertBlocks(ctx, blocks)
	if err != nil {
		return err
	}

	err = s.bridgeStore.ProcessNewBlocks(ctx, blocks)
	if err != nil {
		return err
	}

	if len(blocks) > 0 {
		s.logger.Debug(
			syncLogPrefix("inserted blocks"),
			"from", blocks[0].NumberU64(),
			"to", blocks[len(blocks)-1].NumberU64(),
			"blocks", len(blocks),
			"duration", time.Since(insertStartTime),
			"blks/sec", float64(len(blocks))/math.Max(time.Since(insertStartTime).Seconds(), 0.0001))
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
func (s *ExecutionClientStore) bridgeReplayInitialBlockIfNeeded(ctx context.Context, executionTipBlockNum uint64) error {
	initialBlockNum, initialReplayNeeded, err := s.bridgeStore.InitialBlockReplayNeeded(ctx)
	if err != nil {
		return err
	}

	if initialReplayNeeded {
		initialHeader, err := s.executionStore.GetHeader(ctx, initialBlockNum)
		if err != nil {
			return err
		}

		s.logger.Info(
			syncLogPrefix("replaying initial block for bridge store"),
			"blockNum", initialHeader.Number.Uint64(),
		)

		if err = s.bridgeStore.ReplayInitialBlock(ctx, types.NewBlockWithHeader(initialHeader)); err != nil {
			return err
		}
	}

	if executionTipBlockNum <= initialBlockNum {
		return nil
	}

	start, end := initialBlockNum+1, executionTipBlockNum
	blocksCount := executionTipBlockNum - initialBlockNum
	s.logger.Info(
		syncLogPrefix("replaying post initial blocks for bridge store to fill gap with execution"),
		"start", start,
		"end", end,
		"blocks", blocksCount,
	)

	progressLogTicker := time.NewTicker(30 * time.Second)
	defer progressLogTicker.Stop()

	blocksBatchSize := 10_000
	blocks := make([]*types.Block, 0, blocksBatchSize)
	for blockNum := start; blockNum <= end; blockNum++ {
		header, err := s.executionStore.GetHeader(ctx, blockNum)
		if err != nil {
			return err
		}
		if header == nil {
			return fmt.Errorf("unexpected block header missing when replaying blocks for bridge, "+
				"likely due to a gap in snapshot files and db data after restart, "+
				"rm -rf datadir/chaindata can fix this "+
				"(note this is quick to recover from and not equivalent to a full re-sync): %d", blockNum)
		}

		select {
		case <-progressLogTicker.C:
			s.logger.Info(
				syncLogPrefix("replaying blocks for bridge periodic progress"),
				"current", blockNum,
				"start", start,
				"end", end,
			)
		default:
			// carry-on
		}

		blocks = append(blocks, types.NewBlockWithHeader(header))
		if len(blocks) < blocksBatchSize && blockNum != end {
			continue
		}

		if err := s.bridgeStore.ProcessNewBlocks(ctx, blocks); err != nil {
			return err
		}

		blocks = nil
	}

	return nil
}
