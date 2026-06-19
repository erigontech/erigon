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

package state

import (
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

// The merge goroutine BuildFiles2 spawns must register on wg before the build
// goroutine's wg.Done, so wg.Wait (what Close does) never races that Add from zero.
func TestAggregatorCloseWaitsForBackgroundMerge(t *testing.T) {
	logger := log.New()
	for range 64 {
		dirs := datadir.New(t.TempDir())
		db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
		agg := NewTest(dirs).StepSize(16).Logger(logger).MustOpen(t.Context(), db)
		require.NoError(t, agg.OpenFolder())

		require.NoError(t, agg.BuildFiles2(t.Context(), 0, 0, true))
		agg.wg.Wait()
		agg.Close()
		db.Close()
	}
}
