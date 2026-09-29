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

package rawtemporaldb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state"
)

func TestCanUnwindBlockFloorUsesConversionPoint(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(10_000))
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, tx.Put(kv.ChangeSets3, dbutils.BlockBodyKey(4, common.Hash{1}), []byte{1}))
	conversionBlock := uint64(5)
	conversionTx := uint64(53)
	require.NoError(t, state.WriteErigonDBSettings(tx.Debug().Dirs(), &state.ErigonDBSettings{
		ConversionBlockNum: &conversionBlock,
		ConversionTxNum:    &conversionTx,
	}))

	got, err := rawtemporaldb.CanUnwindToBlockNum(tx)
	require.NoError(t, err)
	require.Equal(t, conversionBlock, got, "the conversion block must be the minimum block unwind point")

	got, ok, err := rawtemporaldb.CanUnwindBeforeBlockNum(conversionBlock-1, tx)
	require.NoError(t, err)
	require.False(t, ok, "a block below the conversion point must be refused")
	require.Equal(t, conversionBlock, got)

	got, ok, err = rawtemporaldb.CanUnwindBeforeBlockNum(conversionBlock, tx)
	require.NoError(t, err)
	require.True(t, ok, "the conversion block itself must remain unwindable")
	require.Equal(t, conversionBlock, got)
}

func TestCanUnwindBlockFloorWithoutConversionPoint(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(10_000))
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, tx.Put(kv.ChangeSets3, dbutils.BlockBodyKey(4, common.Hash{1}), []byte{1}))

	got, err := rawtemporaldb.CanUnwindToBlockNum(tx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), got, "an unset conversion point must not create a floor at block zero")
}
