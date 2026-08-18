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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
)

type recordingSealEngine struct {
	rules.Engine
	called atomic.Bool
}

type cancelOnSecondCheckContext struct {
	context.Context
	cancel context.CancelFunc
	checks atomic.Uint32
}

func (c *cancelOnSecondCheckContext) Err() error {
	if c.checks.Add(1) == 2 {
		c.cancel()
	}
	return c.Context.Err()
}

func (e *recordingSealEngine) Seal(rules.ChainHeaderReader, *types.BlockWithReceipts, chan<- *types.BlockWithReceipts, <-chan struct{}) error {
	e.called.Store(true)
	return nil
}

func TestFinishBlockDoesNotSealAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	engine := &recordingSealEngine{}
	store := NewLatestBlockBuiltStore()
	cfg := BuilderFinishCfg{
		engine:                engine,
		latestBlockBuiltStore: store,
	}

	err := finishBlock(ctx, nil, cfg, log.Root())

	require.ErrorIs(t, err, context.Canceled)
	require.False(t, engine.called.Load())
	require.Nil(t, store.BlockBuilt())
}

func TestFinishBlockDoesNotSealWhenCanceledDuringAssembly(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	testCtx := &cancelOnSecondCheckContext{Context: ctx, cancel: cancel}
	engine := &recordingSealEngine{}
	store := NewLatestBlockBuiltStore()
	cfg := BuilderFinishCfg{
		engine: engine,
		builderState: BuilderState{
			BuiltBlock: &exec.AssembledBlock{Header: &types.Header{}},
		},
		latestBlockBuiltStore: store,
	}

	err := finishBlock(testCtx, nil, cfg, log.Root())

	require.ErrorIs(t, err, context.Canceled)
	require.False(t, engine.called.Load())
	require.Nil(t, store.BlockBuilt())
	// Exactly two checks means the cancellation fired at the mid-assembly check: an extra Err()
	// call added upstream would silently turn this into a copy of the entry-check test.
	require.Equal(t, uint32(2), testCtx.checks.Load())
}
