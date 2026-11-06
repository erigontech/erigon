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

package execmodule_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// A forkchoice update hides a block stored in the overlay that it commits until the commit ends.
// GetHeaderIfIdle must report the module busy for that whole time instead of a missing block.
func TestGetHeaderIfIdleReportsBusyWhileAForkchoiceUpdateCommits(t *testing.T) {
	var armed atomic.Bool
	published, proceed := make(chan struct{}), make(chan struct{})
	m := execmoduletester.New(t, execmoduletester.WithStateTransitionObserver(func(ctx context.Context, point execmodule.StateTransitionPoint) {
		if point != execmodule.StateTransitionOverlayPublished || !armed.CompareAndSwap(true, false) {
			return
		}
		close(published)
		select {
		case <-proceed:
		case <-ctx.Done():
		}
	}))
	chainPack, err := m.GenerateChain(2, func(int, *blockgen.BlockGen) {})
	require.NoError(t, err)
	parent, block := chainPack.Blocks[0], chainPack.Blocks[1]
	ctx := t.Context()
	_, err = m.InsertBlocks(ctx, []*types.Block{parent})
	require.NoError(t, err)
	validation, err := m.ValidateChain(ctx, parent.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)
	_, err = m.InsertBlocks(ctx, []*types.Block{block})
	require.NoError(t, err)

	armed.Store(true)
	forkchoiceDone := make(chan error, 1)
	go func() {
		_, err := m.UpdateForkChoice(ctx, parent.Header())
		forkchoiceDone <- err
	}()
	release := sync.OnceFunc(func() { close(proceed) })
	defer release()
	select {
	case <-published:
	case <-time.After(time.Minute):
		t.Fatal("the forkchoice update did not publish its overlay")
	}

	hash, number := block.Hash(), block.NumberU64()
	hidden, err := m.ExecModule.GetHeader(ctx, &hash, &number)
	require.NoError(t, err)
	require.Nil(t, hidden, "the paused forkchoice update must hide the block from a lookup")
	header, idle, err := m.ExecModule.GetHeaderIfIdle(ctx, &hash, &number)
	require.NoError(t, err)
	require.False(t, idle)
	require.Nil(t, header)

	release()
	require.NoError(t, <-forkchoiceDone)
	m.ExecModule.WaitIdle(ctx)
	header, idle, err = m.ExecModule.GetHeaderIfIdle(ctx, &hash, &number)
	require.NoError(t, err)
	require.True(t, idle)
	require.Equal(t, hash, header.Hash())
}
