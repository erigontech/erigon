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

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
)

// nolint:thelper
func NewTestDB(tb testing.TB, dirs datadir.Dirs) (kv.TemporalRwDB, *state.Aggregator) {
	if tb != nil {
		tb.Helper()
	}

	var rawDB kv.RwDB
	if tb != nil {
		rawDB = memdb.NewTestDB(tb, kv.ChainDB)
	} else {
		rawDB = memdb.New(dirs.DataDir, kv.ChainDB)
	}

	agg, err := state.NewAggregator(context.Background(), dirs, config3.DefaultStepSize, rawDB, log.New())
	if err != nil {
		panic(err)
	}
	if err := agg.OpenFolder(); err != nil {
		panic(err)
	}
	if tb != nil {
		tb.Cleanup(agg.Close)
	}

	db, err := temporal.New(rawDB, agg)
	if err != nil {
		panic(err)
	}
	if tb != nil {
		tb.Cleanup(agg.Close)
	}
	return db, agg
}
