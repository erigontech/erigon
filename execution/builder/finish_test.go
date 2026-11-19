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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
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
	engine := &recordingSealEngine{Engine: ethash.NewFaker()}
	store := NewLatestBlockBuiltStore()
	cfg := BuilderFinishCfg{
		chainConfig: chain.AllProtocolChanges,
		engine:      engine,
		builderState: BuilderState{
			BuiltBlock: &exec.AssembledBlock{Header: &types.Header{}},
		},
		latestBlockBuiltStore: store,
	}

	err := finishBlock(testCtx, nil, cfg, log.Root())

	require.ErrorIs(t, err, context.Canceled)
	require.False(t, engine.called.Load())
	require.Nil(t, store.BlockBuilt())
}

func TestFinishBlockValidatesBlockAccessListBeforeSealing(t *testing.T) {
	engine := &recordingSealEngine{}
	store := NewLatestBlockBuiltStore()
	cfg := BuilderFinishCfg{
		chainConfig: &chain.Config{AmsterdamTime: common.NewUint64(0)},
		engine:      engine,
		builderState: BuilderState{
			BuiltBlock: &exec.AssembledBlock{
				Header: &types.Header{GasLimit: types.BalItemCost - 1},
				BlockAccessList: types.BlockAccessList{{
					Address: common.Address{1},
				}},
			},
		},
		latestBlockBuiltStore: store,
	}

	err := finishBlock(t.Context(), nil, cfg, log.Root())

	require.ErrorIs(t, err, types.ErrInvalidBlockAccessList)
	require.False(t, engine.called.Load())
	require.Nil(t, store.BlockBuilt())
}

func TestFinishBlockRejectsInvalidExecutionResults(t *testing.T) {
	for _, tc := range []struct {
		name    string
		block   exec.AssembledBlock
		wantErr string
	}{
		{
			name:    "block gas",
			block:   exec.AssembledBlock{Header: &types.Header{GasUsed: 1}},
			wantErr: "gas used by execution: 0, in header: 1",
		},
		{
			name:    "blob gas with preset nonce",
			block:   exec.AssembledBlock{Header: &types.Header{BlobGasUsed: common.NewUint64(1), Nonce: types.EncodeNonce(1)}},
			wantErr: "blobGasUsed by execution: 0, in header: 1",
		},
		{
			name: "receipt bloom disagrees with logs",
			block: exec.AssembledBlock{
				Header:   &types.Header{},
				Txns:     types.Transactions{&types.LegacyTx{}},
				Receipts: types.Receipts{{Bloom: types.Bloom{1}}},
			},
			wantErr: "receiptHash mismatch",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := &recordingSealEngine{Engine: ethash.NewFaker()}
			store := NewLatestBlockBuiltStore()
			state := NewBuilderState(nil)
			tc.block.Header.Number.SetUint64(1)
			state.BuiltBlock = &tc.block
			cfg := StageBuilderFinishCfg(chain.AllProtocolChanges, engine, state, nil, nil, store)

			err := finishBlock(t.Context(), nil, cfg, log.Root())

			require.ErrorContains(t, err, tc.wantErr)
			require.False(t, engine.called.Load())
			require.Nil(t, store.BlockBuilt())
			require.Empty(t, state.PendingResultCh)
			require.Empty(t, state.BuilderResultCh)
		})
	}
}
