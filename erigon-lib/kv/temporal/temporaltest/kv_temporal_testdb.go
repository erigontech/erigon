package temporaltest

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/config3"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/kv/temporal"
	"github.com/ledgerwatch/erigon-lib/log/v3"
	"github.com/ledgerwatch/erigon-lib/state"
)

// nolint:thelper
func NewTestDB(tb testing.TB, dirs datadir.Dirs) (db kv.RwDB, agg *state.Aggregator) {
	if tb != nil {
		tb.Helper()
	}
	logger := log.New()

	if tb != nil {
		db = memdb.NewTestDB(tb)
	} else {
		db = memdb.New(dirs.DataDir)
	}

	var err error
	agg, err = state.NewAggregator(context.Background(), dirs, config3.HistoryV3AggregationStep, db, nil, logger)
	if err != nil {
		panic(err)
	}
	if err := agg.OpenFolder(); err != nil {
		panic(err)
	}

	db, err = temporal.New(db, agg)
	if err != nil {
		panic(err)
	}
	return db, agg
}
