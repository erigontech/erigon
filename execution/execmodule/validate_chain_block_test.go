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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
)

func TestInvalidPayloadPurgeVsBodyGetters(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	m.ExecModule.SetPublishedSD(m.Notifications.Events.LatestSD)
	pack, err := m.GenerateChain(8, sendTo(t, m, key, common.Address{0x0a}, 1))
	require.NoError(t, err)
	hashes := make([]common.Hash, 0, len(pack.Blocks))
	for _, block := range pack.Blocks {
		hashes = append(hashes, block.Hash())
	}

	ctx := t.Context()
	var stop atomic.Bool
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := g; !stop.Load(); i++ {
				var err error
				if i%2 == 0 {
					_, _, err = m.ExecModule.FrozenBlocks(ctx)
				} else {
					_, err = m.ExecModule.GetBodiesByHashes(ctx, hashes)
				}
				if err != nil {
					t.Errorf("getter: %v", err)
					return
				}
			}
		})
	}
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()

	parent := m.Genesis.Hash()
	for _, block := range pack.Blocks {
		bad := tamperBlockGasUsed(t, block, 21_000)
		_, err := m.InsertBlocks(ctx, []*types.Block{bad})
		require.NoError(t, err)
		result, err := m.ValidateChain(ctx, bad.Header())
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusBadBlock, result.ValidationStatus)
		require.Equal(t, parent, result.LatestValidHash)

		_, err = m.InsertBlocks(ctx, []*types.Block{block})
		require.NoError(t, err)
		valid, err := m.ValidateChain(ctx, block.Header())
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, valid.ValidationStatus)
		fcu, err := m.UpdateForkChoice(ctx, block.Header())
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
		m.ExecModule.WaitIdle(ctx)
		parent = block.Hash()
	}
}
