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

package jsonrpc

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

// TestReplayTxnsParallelRejectsReorgedBlock pins that the workers fail, instead of replaying
// another block's txNums, when the block is no longer canonical in their own view.
func TestReplayTxnsParallelRejectsReorgedBlock(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	withReplayCPUs(t, 16)

	var replayed atomic.Bool
	g, _ := replayTxnsParallel(m.Ctx, m.DB, m.BlockReader, 1, common.Hash{0xff}, 0, 4,
		func(context.Context, kv.TemporalTx) (func(int) error, error) {
			return func(int) error {
				replayed.Store(true)
				return nil
			}, nil
		})
	require.ErrorIs(t, g.Wait(), errBlockNotCanonical)
	require.False(t, replayed.Load())
}
