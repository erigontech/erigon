// Copyright 2025 The Erigon Authors
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

package p2p

import (
	"context"

	"github.com/erigontech/erigon/execution/types"
)

type BbdResultFeed struct {
	ch chan BlockBatchResult
}

func (rf BbdResultFeed) Next(ctx context.Context) ([]*types.Block, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case batch, ok := <-rf.ch:
		if !ok {
			return nil, nil
		}
		return batch.Blocks, batch.Err
	}
}

func (rf BbdResultFeed) consumeData(ctx context.Context, blocks []*types.Block) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case rf.ch <- BlockBatchResult{Blocks: blocks}:
		return nil
	}
}

func (rf BbdResultFeed) consumeErr(ctx context.Context, err error) {
	select {
	case <-ctx.Done():
		return
	case rf.ch <- BlockBatchResult{Err: err}:
	}
}

func (rf BbdResultFeed) close() {
	close(rf.ch)
}

type BlockBatchResult struct {
	Blocks []*types.Block
	Err    error
}
