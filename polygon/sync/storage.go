package sync

import (
	"context"

	"github.com/ledgerwatch/erigon/core/types"
)

type Storage interface {
	// GetHeadersInRange reads blocks from the local canonical chain.
	GetHeadersInRange(ctx context.Context, start uint64, end uint64) ([]*types.Header, error)
	// PutHeaders writes blocks to the local canonical chain.
	PutHeaders(ctx context.Context, headers []*types.Header) error
	TipBlockNumber(ctx context.Context) (uint64, error)
	TipHeader(ctx context.Context) (*types.Header, error)
}

type storageStub struct {
}

func NewStorage() Storage {
	return &storageStub{}
}

func (s *storageStub) GetHeadersInRange(ctx context.Context, start uint64, end uint64) ([]*types.Header, error) {
	//TODO implement me
	panic("implement me")
}

func (s *storageStub) PutHeaders(ctx context.Context, headers []*types.Header) error {
	//TODO implement me
	panic("implement me")
}

func (s *storageStub) TipBlockNumber(ctx context.Context) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (s *storageStub) TipHeader(ctx context.Context) (*types.Header, error) {
	//TODO implement me
	panic("implement me")
}
