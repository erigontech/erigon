package sync

import (
	"context"
	"sync"
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
	waitGroup sync.WaitGroup
}

func NewStorage(logger log.Logger, execution ExecutionClient, queueCapacity int) Storage {
	return &executionClientStorage{
		logger:    logger,
		execution: execution,
		queue:     make(chan []*types.Block, queueCapacity),
	}
}

func (s *executionClientStorage) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
	s.waitGroup.Add(1)
	select {
	case s.queue <- blocks:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *executionClientStorage) Flush(ctx context.Context) error {
	waitCtx, waitCancel := context.WithCancel(ctx)
	defer waitCancel()

	go func() {
		s.waitGroup.Wait()
		waitCancel()
	}()

	<-waitCtx.Done()
	return ctx.Err()
}

func (s *executionClientStorage) Run(ctx context.Context) error {
	s.logger.Debug(syncLogPrefix("running execution client storage component"))

	for {
		select {
		case blocks := <-s.queue:
			if err := s.insertBlocks(ctx, blocks); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *executionClientStorage) insertBlocks(ctx context.Context, blocks []*types.Block) error {
	defer s.waitGroup.Done()

	insertStartTime := time.Now()
	err := s.execution.InsertBlocks(ctx, blocks)
	if err != nil {
		return err
	}

	s.logger.Debug(syncLogPrefix("inserted blocks"), "len", len(blocks), "duration", time.Since(insertStartTime))

	return nil
}
