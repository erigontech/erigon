package sync

import (
	"context"
	"sync"

	"github.com/ledgerwatch/erigon/core/types"
)

type Storage interface {
	// PutHeaders queues blocks for writing into the local canonical chain.
	PutHeaders(ctx context.Context, headers []*types.Header) error
	// Flush makes sure that all queued blocks have been written.
	Flush(ctx context.Context) error
	// Run performs the block writing.
	Run(ctx context.Context) error
}

type executionClientStorage struct {
	execution ExecutionClient
	queue     chan []*types.Header
	waitGroup sync.WaitGroup
}

func NewStorage(execution ExecutionClient, queueCapacity int) Storage {
	return &executionClientStorage{
		execution: execution,
		queue:     make(chan []*types.Header, queueCapacity),
	}
}

func (s *executionClientStorage) PutHeaders(ctx context.Context, headers []*types.Header) error {
	s.waitGroup.Add(1)
	select {
	case s.queue <- headers:
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
		case headers := <-s.queue:
			err := s.execution.InsertBlocks(ctx, headers)
			s.waitGroup.Done()
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
