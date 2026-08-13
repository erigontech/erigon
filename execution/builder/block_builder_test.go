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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types"
)

func TestBlockBuilderRunningHasNotFailed(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	b := NewBlockBuilder(func(_ *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		<-release
		return nil, errors.New("builder stopped")
	}, &Parameters{}, time.Minute)

	require.Never(t, b.Failed, 50*time.Millisecond, 5*time.Millisecond)
}

func TestBlockBuilderStoppedForItsPayloadHasNotFailed(t *testing.T) {
	t.Parallel()

	b := NewBlockBuilder(func(_ *Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		for !interrupt.Load() {
			time.Sleep(time.Millisecond)
		}
		return &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}, nil
	}, &Parameters{}, time.Minute)

	_, err := b.Stop(t.Context())
	require.NoError(t, err)

	// Collecting the payload is what a proposal does. Reading that as failure would make a repeated
	// request rebuild from scratch instead of being handed the block that was just built.
	require.False(t, b.Failed())
}

func TestBlockBuilderHasFailedOnceItErrors(t *testing.T) {
	t.Parallel()

	b := NewBlockBuilder(func(_ *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		return nil, errors.New("build failed")
	}, &Parameters{}, time.Minute)

	require.Eventually(t, b.Failed, time.Second, time.Millisecond)
}

func TestBlockBuilderStaysReusableOnceItFillsTheBlock(t *testing.T) {
	t.Parallel()

	built := make(chan struct{})
	b := NewBlockBuilder(func(_ *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		defer close(built)
		return &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}, nil
	}, &Parameters{}, time.Minute)

	<-built
	// A builder that ran out of room holds a complete payload, so its id is still worth reusing.
	require.Never(t, b.Failed, 50*time.Millisecond, 5*time.Millisecond)
}
