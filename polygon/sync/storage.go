package sync

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/types"
)

//go:generate mockgen -typed=true -destination=./storage_mock.go -package=sync . Storage
type Storage interface {
	// InsertBlocks queues blocks for writing into the local canonical chain.
	InsertBlocks(ctx context.Context, blocks []*types.Block) error
	// Flush makes sure that all queued blocks have been written.
	Flush(ctx context.Context) error
	// Run performs the block writing.
	Run(ctx context.Context) error
}

type executionClientStorage struct {
	logger    log.Logger
	execution ExecutionClient
	queue     chan []*types.Block

	// tasksCount includes both tasks pending in the queue and a task that was taken and hasn't finished yet
	tasksCount atomic.Int32

	// tasksDoneSignal gets sent a value when tasksCount becomes 0
	tasksDoneSignal chan bool
}

func NewStorage(logger log.Logger, execution ExecutionClient, queueCapacity int) Storage {
	return &executionClientStorage{
		logger:    logger,
		execution: execution,
		queue:     make(chan []*types.Block, queueCapacity),

		tasksDoneSignal: make(chan bool, 1),
	}
}

func (s *executionClientStorage) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
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

func (s *executionClientStorage) Flush(ctx context.Context) error {
	for s.tasksCount.Load() > 0 {
		select {
		case _, ok := <-s.tasksDoneSignal:
			if !ok {
				return errors.New("executionClientStorage.Flush failed because ExecutionClient.InsertBlocks failed")
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (s *executionClientStorage) Run(ctx context.Context) error {
	s.logger.Debug(syncLogPrefix("running execution client storage component"))

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

func (s *executionClientStorage) insertBlocks(ctx context.Context, blocks []*types.Block) error {
	defer s.tasksCount.Add(-1)

	insertStartTime := time.Now()
	err := s.execution.InsertBlocks(ctx, blocks)
	if err != nil {
		return err
	}

	s.logger.Debug(syncLogPrefix("inserted blocks"), "len", len(blocks), "duration", time.Since(insertStartTime))

	return nil
}
