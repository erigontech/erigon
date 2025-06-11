// Copyright 2024 The Erigon Authors
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

package stagedsync_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon-lib/kv"
	state2 "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/eth/stagedsync"
)

func TestCustomTraceReceiptDomain(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	ctx := context.Background()

	m, _, _ := rpcdaemontest.CreateTestSentry(t)

	stageCfg := stagedsync.StageCustomTraceCfg([]string{"receipt"}, m.DB, m.Dirs, m.BlockReader, m.ChainConfig, m.Engine, m.Cfg().Genesis, m.Cfg().Sync)
	err := stagedsync.StageCustomTraceReset(ctx, m.DB, stageCfg.Produce)
	require.NoError(err)

	err = stagedsync.SpawnCustomTrace(stageCfg, ctx, m.Log)
	require.NoError(err)

	err = m.DB.ViewTemporal(ctx, func(rtx kv.TemporalTx) error {
		ac := state2.AggTx(rtx)
		progress := ac.HistoryProgress(kv.ReceiptDomain, rtx)
		assert.Greater(progress, uint64(0), "Receipt domain progress should be greater than 0")

		cumGasUsed, _, logIndex, err := rawtemporaldb.ReceiptAsOf(rtx, 3)
		require.NoError(err)
		assert.Equal(0, int(cumGasUsed))
		assert.Equal(0, int(logIndex))

		cumGasUsed, _, logIndex, err = rawtemporaldb.ReceiptAsOf(rtx, 4)
		require.NoError(err)
		assert.Equal(21_000, int(cumGasUsed))
		assert.Equal(0, int(logIndex))

		cumGasUsed, _, logIndex, err = rawtemporaldb.ReceiptAsOf(rtx, 5)
		require.NoError(err)
		assert.Equal(0, int(cumGasUsed))
		assert.Equal(0, int(logIndex))

		return nil
	})
	require.NoError(err)
}
