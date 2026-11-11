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

package execmoduletester

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

func TestRetriedCatchupResynchronizesCoherentCache(t *testing.T) {
	m, canonical, _ := newReorgChains(t, 100, 1, WithStepSize(16), WithLoopBlockLimit(60))
	sender := m.Address
	ctx := t.Context()
	waitForBatchWithHead := subscribeStateChangeHeads(t, m)
	cache := kvcache.New(kvcache.DefaultCoherentConfig)

	compare := func() (stored, cached []byte, id uint64) {
		require.NoError(t, m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
			var err error
			id, err = tx.ReadSequence(string(kv.PlainStateVersion))
			require.NoError(t, err)
			view, err := cache.View(ctx, tx)
			require.NoError(t, err)
			cached, err = view.Get(sender[:])
			require.NoError(t, err)
			v, _, err := tx.GetLatest(kv.AccountsDomain, sender[:], kv.GetLatestOptions{})
			require.NoError(t, err)
			stored = bytes.Clone(v)
			cached = bytes.Clone(cached)
			return nil
		}))
		return stored, cached, id
	}

	var lastVersion uint64
	for _, b := range canonical.Blocks[:2] {
		require.NoError(t, m.InsertValidateAndUfc1By1(ctx, []*types.Block{b}))
		m.ExecModule.WaitIdle(ctx)
		batch := waitForBatchWithHead(b.Hash())
		cache.OnNewBlock(batch)
		lastVersion = batch.StateVersionId
	}
	stored, cached, id := compare()
	require.Equal(t, lastVersion, id)
	require.Equal(t, stored, cached)

	header := types.CopyHeader(canonical.TopBlock.HeaderNoCopy())
	header.Root[0] ^= 1
	badTip := types.NewBlockFromNetwork(header, canonical.TopBlock.Body(), canonical.TopBlock.BlockAccessListSidecar())
	blocks := append([]*types.Block(nil), canonical.Blocks[2:]...)
	blocks[len(blocks)-1] = badTip
	status, err := m.InsertBlocks(ctx, blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	fcu, err := m.UpdateForkChoice(ctx, badTip.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBadBlock, fcu.Status)
	m.ExecModule.WaitIdle(ctx)

	var committed uint64
	require.NoError(t, m.DB.View(ctx, func(tx kv.Tx) error {
		var err error
		committed, err = stages.GetStageProgress(tx, stages.Finish)
		return err
	}))
	require.Greater(t, committed, uint64(2))
	require.Less(t, committed, badTip.NumberU64())

	accepted := canonical.Blocks[committed-1]
	fcu, err = m.UpdateForkChoice(ctx, accepted.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	m.ExecModule.WaitIdle(ctx)
	batch := waitForBatchWithHead(accepted.Hash())
	require.Equal(t, lastVersion+1, batch.StateVersionId, "consecutive versions must not hide incomplete state changes")
	cache.OnNewBlock(batch)
	stored, cached, id = compare()
	require.Equal(t, batch.StateVersionId, id)
	require.Equal(t, stored, cached, "a Coherent cache fed only by the stream must agree with the committed state")
}
