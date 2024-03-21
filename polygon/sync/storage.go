package sync

import (
	"context"
	"sync"

	"github.com/ledgerwatch/erigon/core/types"
)

//go:generate mockgen -destination=./storage_mock.go -package=sync . Storage
type Storage interface {
	// InsertBlocks queues blocks for writing into the local canonical chain.
	InsertBlocks(ctx context.Context, blocks []*types.Block) error
	// Flush makes sure that all queued blocks have been written.
	Flush(ctx context.Context) error
	// Run performs the block writing.
	Run(ctx context.Context) error
}

type executionClientStorage struct {
	execution ExecutionClient
	queue     chan []*types.Block
	waitGroup sync.WaitGroup
}

func NewStorage(execution ExecutionClient, queueCapacity int) Storage {
	return &executionClientStorage{
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
	for {
		select {
		case blocks := <-s.queue:
			err := s.execution.InsertBlocks(ctx, blocks)
			s.waitGroup.Done()
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
