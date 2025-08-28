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

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
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

	var rawDB kv.RwDB
	if tb != nil {
		rawDB = memdb.NewTestDB(tb, kv.ChainDB)
	} else {
		rawDB = memdb.New(dirs.DataDir, kv.ChainDB)
	}

	salt, err := state.GetStateIndicesSalt(dirs, true, log.New())
	if err != nil {
		panic(err)
	}
	agg, err := state.NewAggregator2(context.Background(), dirs, stepSize, salt, rawDB, log.New())
	if err != nil {
		panic(err)
	}
	agg.DisableFsync()
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
