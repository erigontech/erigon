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
	"github.com/erigontech/erigon/polygon/bridge"
)

//go:generate mockgen -typed=true -destination=./store_mock.go -package=sync . Store
type Store interface {
	// InsertBlocks queues blocks for writing into the local canonical chain.
	InsertBlocks(ctx context.Context, blocks []*types.Block) error
	// Flush makes sure that all queued blocks have been written.
	Flush(ctx context.Context, newTip *types.Header) error
	// Run performs the block writing.
	Run(ctx context.Context) error
}

type executionClientStore struct {
	logger        log.Logger
	execution     ExecutionClient
	polygonBridge bridge.Service
	queue         chan []*types.Block

	// tasksCount includes both tasks pending in the queue and a task that was taken and hasn't finished yet
	tasksCount atomic.Int32

	// tasksDoneSignal gets sent a value when tasksCount becomes 0
	tasksDoneSignal chan bool
}

func NewStore(logger log.Logger, execution ExecutionClient, polygonBridge bridge.Service) Store {
	return &executionClientStore{
		logger:          logger,
		execution:       execution,
		polygonBridge:   polygonBridge,
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

func (s *executionClientStore) Flush(ctx context.Context, newTip *types.Header) error {
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

	err := s.polygonBridge.Synchronize(ctx, newTip)
	if err != nil {
		return err
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
	err := s.execution.InsertBlocks(ctx, blocks)
	if err != nil {
		return err
	}

	err = s.polygonBridge.Synchronize(ctx, blocks[len(blocks)-1].Header())
	if err != nil {
		return err
	}

	err = s.polygonBridge.ProcessNewBlocks(ctx, blocks)
	if err != nil {
		return err
	}

	s.logger.Debug(syncLogPrefix("inserted blocks"), "len", len(blocks), "duration", time.Since(insertStartTime))

	return nil
}
