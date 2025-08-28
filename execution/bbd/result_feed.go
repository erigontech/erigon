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

package bbd

import (
	"context"

	"github.com/erigontech/erigon/execution/types"
)

type ResultFeed struct {
	ch chan BatchResult
}

func (rf ResultFeed) Next(ctx context.Context) ([]*types.Block, error) {
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

func (rf ResultFeed) consumeData(ctx context.Context, blocks []*types.Block) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case rf.ch <- BatchResult{Blocks: blocks}:
		return nil
	}
}

func (rf ResultFeed) consumeErr(ctx context.Context, err error) {
	select {
	case <-ctx.Done():
		return
	case rf.ch <- BatchResult{Err: err}:
	}
}

func (rf ResultFeed) close() {
	close(rf.ch)
}

type BatchResult struct {
	Blocks []*types.Block
	Err    error
}
