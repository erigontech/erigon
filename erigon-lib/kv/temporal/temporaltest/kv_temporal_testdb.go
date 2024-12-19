package temporaltest

import (
	"context"
	"testing"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/kvcfg"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
)

//nolint:thelper
func NewTestDB(tb testing.TB, dirs datadir.Dirs) (histV3 bool, db kv.RwDB, agg *state.Aggregator) {
	historyV3 := config3.EnableHistoryV3InTest
	logger := log.New()
	ctx := context.Background()

	if tb != nil {
		db = memdb.NewTestDB(tb)
	} else {
		db = memdb.New(dirs.DataDir)
	}
	_ = db.UpdateNosync(context.Background(), func(tx kv.RwTx) error {
		_, _ = kvcfg.HistoryV3.WriteOnce(tx, historyV3)
		return nil
	})

	if historyV3 {
		var err error
		dir.MustExist(dirs.SnapHistory)
		agg, err = state.NewAggregator(ctx, dirs.SnapHistory, dirs.Tmp, config3.HistoryV3AggregationStep, db, logger)
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
	}
	return historyV3, db, agg
}
