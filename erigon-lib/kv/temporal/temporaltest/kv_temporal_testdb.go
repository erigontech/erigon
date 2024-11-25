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

package temporaltest

import (
	"context"
	"testing"

	"github.com/erigontech/erigon/erigon-lib/common/datadir"
	"github.com/erigontech/erigon/erigon-lib/config3"
	"github.com/erigontech/erigon/erigon-lib/kv"
	"github.com/erigontech/erigon/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon/erigon-lib/log/v3"
	"github.com/erigontech/erigon/erigon-lib/state"
)

// nolint:thelper
func NewTestDB(tb testing.TB, dirs datadir.Dirs) (db kv.RwDB, agg *state.Aggregator) {
	if tb != nil {
		tb.Helper()
	}

	if tb != nil {
		db = memdb.NewTestDB(tb, kv.ChainDB)
	} else {
		db = memdb.New(dirs.DataDir, kv.ChainDB)
	}

	var err error
	agg, err = state.NewAggregator(context.Background(), dirs, config3.HistoryV3AggregationStep, db, log.New())
	if err != nil {
		panic(err)
	}
	if err := agg.OpenFolder(); err != nil {
		panic(err)
	}
	if tb != nil {
		tb.Cleanup(agg.Close)
	}

	db, err = temporal.New(db, agg)
	if err != nil {
		panic(err)
	}
	if tb != nil {
		tb.Cleanup(agg.Close)
	}
	return db, agg
}
