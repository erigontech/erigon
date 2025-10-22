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

	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
)

// nolint:thelper
func NewTestDB(tb testing.TB, dirs datadir.Dirs) kv.TemporalRwDB {
	return NewTestDBWithStepSize(tb, dirs, config3.DefaultStepSize)
}

func NewTestDBWithStepSize(tb testing.TB, dirs datadir.Dirs, stepSize uint64) kv.TemporalRwDB {
	if tb != nil {
		tb.Helper()
	}

	//TODO: create set of funcs for non-test code. Assert(tb == nil)

	var rawDB kv.RwDB
	ctx := context.Background()
	if tb != nil {
		ctx = tb.Context()
		rawDB = memdb.NewTestDB(tb, dbcfg.ChainDB)
	} else {
		rawDB = memdb.New(nil, dirs.DataDir, dbcfg.ChainDB)
	}

	agg := state.NewTest(dirs).StepSize(stepSize).MustOpen(ctx, rawDB)
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
		tb.Cleanup(db.Close)
	}
	return db
}
