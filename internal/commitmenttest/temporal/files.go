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

package temporal

import (
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/stretchr/testify/require"
)

func Open(tb testing.TB, stepSize uint64) (kv.TemporalRwDB, *state.Aggregator) {
	tb.Helper()
	logger := log.New()
	dirs := datadir.New(tb.TempDir())
	db := mdbxtest.InMem(tb, mdbx.New(dbcfg.ChainDB, logger), dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	tb.Cleanup(db.Close)

	agg := state.NewTest(dirs).StepSize(stepSize).Logger(logger).MustOpen(tb.Context())
	tb.Cleanup(agg.Close)
	err := agg.OpenFolder(db)
	require.NoError(tb, err)
	tdb, err := dbtemporal.New(db, agg, nil)
	require.NoError(tb, err)
	tb.Cleanup(tdb.Close)
	return tdb, agg
}
