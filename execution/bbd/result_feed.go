package bbd

import (
	"context"

	"github.com/erigontech/erigon-lib/types"
)

type ResultFeed struct {
	ch chan BatchResult
}

func (rf ResultFeed) Next(ctx context.Context) ([]*types.Block, bool, error) {
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case batch, ok := <-rf.ch:
		if !ok {
			return nil, false, nil
		}
		return batch.Blocks, batch.HasNext, batch.Err
	}
}

type BatchResult struct {
	Blocks  []*types.Block
	HasNext bool
	Err     error
}
