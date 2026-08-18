// Copyright 2026 The Erigon Authors
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

package builder

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types"
)

func TestBlockBuilderStopPrefersFinishedPayload(t *testing.T) {
	done := make(chan struct{})
	close(done)
	want := &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}
	b := &BlockBuilder{done: done, result: want}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	for range 100 {
		result, err := b.Stop(ctx)
		require.NoError(t, err)
		require.Same(t, want, result)
	}
}

type completeWhenCancellationIsChecked struct {
	context.Context
	cancelled chan struct{}
	complete  func()
	once      sync.Once
}

func (c *completeWhenCancellationIsChecked) Done() <-chan struct{} {
	c.once.Do(c.complete)
	return c.cancelled
}

func (c *completeWhenCancellationIsChecked) Err() error { return context.Canceled }

func TestBlockBuilderStopPrefersPayloadCompletedDuringSelection(t *testing.T) {
	for range 100 {
		done := make(chan struct{})
		cancelled := make(chan struct{})
		close(cancelled)
		want := &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}
		b := &BlockBuilder{done: done, result: want}
		ctx := &completeWhenCancellationIsChecked{
			Context:   t.Context(),
			cancelled: cancelled,
			complete:  func() { close(done) },
		}

		result, err := b.Stop(ctx)
		require.NoError(t, err)
		require.Same(t, want, result)
	}
}

func TestBlockBuilderStopMarksAbandonedWait(t *testing.T) {
	b := &BlockBuilder{done: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	result, err := b.Stop(ctx)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrStopAbandoned)
	require.ErrorIs(t, err, context.Canceled)
}
