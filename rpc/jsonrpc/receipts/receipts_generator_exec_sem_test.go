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

package receipts

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

// TestGetReceiptsAnswersAnEmptyBlockWithoutAnExecSlot pins that a block with no
// transactions is answered before the execution semaphore: it derives nothing, so a
// slot spent on it is a slot kept from a block that has work to do. The semaphore is
// full and the context already cancelled, so any path reaching the semaphore reports
// the cancellation instead of the empty result.
func TestGetReceiptsAnswersAnEmptyBlockWithoutAnExecSlot(t *testing.T) {
	t.Parallel()

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	const blockNum = uint64(1)
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, blockNum))
	block := types.NewBlockWithHeader(&types.Header{Number: *uint256.NewInt(blockNum)}, nil)

	g := newTestGenerator(t)
	g.blockExecMutex = &loaderMutex[common.Hash]{}
	g.execSem = make(chan struct{}, 1)
	g.execSem <- struct{}{}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	got, err := g.GetReceipts(ctx, chain.TestChainBerlinConfig, tx, block, eth.ReceiptsOpts{})
	require.NoError(t, err)
	require.Empty(t, got)
}
