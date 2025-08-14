package test

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/types"
)

type Num = state.Num
type RootNum = state.RootNum
type ForkableId = kv.ForkableId
type MarkedTxI = state.MarkedTxI
type UnmarkedTxI = state.UnmarkedTxI

func registerEntity(dirs datadir.Dirs, name string) state.ForkableId {
	stepSize := uint64(10)
	return registerEntityWithSnapshotConfig(dirs, name, state.NewSnapshotConfig(&state.SnapshotCreationConfig{
		RootNumPerStep: 10,
		MergeStages:    []uint64{20, 40},
		MinimumSize:    10,
		SafetyMargin:   5,
	}, state.NewE2SnapSchemaWithStep(dirs, name, []string{name}, stepSize)))

}

func registerEntityWithSnapshotConfig(dirs datadir.Dirs, name string, cfg *state.SnapshotConfig) state.ForkableId {
	return state.RegisterForkable(name, dirs, nil, state.WithSnapshotConfig(cfg))
}

func setup(tb testing.TB) (datadir.Dirs, kv.RwDB, log.Logger) {
	tb.Helper()
	logger := log.New()
	dirs := datadir.New(tb.TempDir())
	db := mdbx.New(kv.ChainDB, logger).InMem(dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	return dirs, db, logger
}

func setupHeader(t *testing.T, log log.Logger, dirs datadir.Dirs, db kv.RoDB) (ForkableId, *state.Forkable[state.MarkedTxI]) {
	headerId := registerEntity(dirs, "headers")
	require.Equal(t, state.ForkableId(0), headerId)

	// create marked forkable
	freezer := snaptype2.NewHeaderFreezer(kv.HeaderCanonical, kv.Headers, log)

	builder := state.NewSimpleAccessorBuilder(state.NewAccessorArgs(true, true), headerId, log,
		state.WithIndexKeyFactory(&snaptype2.HeaderAccessorIndexKeyFactory{}))

	ma, err := state.NewMarkedForkable(headerId, kv.Headers, kv.HeaderCanonical, state.IdentityRootRelationInstance, log,
		state.App_WithFreezer(freezer),
		state.App_WithPruneFrom(Num(1)),
		state.App_WithIndexBuilders(builder),
		state.App_WithUpdateCanonical(),
	)
	require.NoError(t, err)

	cleanup(t, ma.ProtoForkable, db, dirs)
	return headerId, ma
}

func cleanup(t *testing.T, p *state.ProtoForkable, db kv.RoDB, dirs datadir.Dirs) {
	t.Cleanup(func() {
		p.Close()
		p.RecalcVisibleFiles(0)

		state.Cleanup()
		db.Close()
		dir.RemoveAll(dirs.Snap)
		dir.RemoveAll(dirs.Chaindata)
		dir.RemoveAll(dirs.SnapIdx)
	})
}

// TESTS BEGIN HERE

// test marked forkable
func TestMarkedForkableRegistration(t *testing.T) {
	// just registration goes fine
	t.Cleanup(func() {
		state.Cleanup()
	})
	dirs := datadir.New(t.TempDir())
	blockId := registerEntity(dirs, "blocks")
	require.Equal(t, state.ForkableId(0), blockId)
	headerId := registerEntity(dirs, "headers")
	require.Equal(t, state.ForkableId(1), headerId)
}

func TestMarked_PutToDb(t *testing.T) {
	/*
		put, get, getnc on db-only data
	*/
	dir, db, log := setup(t)
	_, ma := setupHeader(t, log, dir, db)

	ma_tx := ma.BeginTemporalTx()
	defer ma_tx.Close()
	rwtx, err := db.BeginRw(context.Background())
	defer rwtx.Rollback()
	require.NoError(t, err)

	num := Num(1)
	hash := common.HexToHash("0x1234").Bytes()
	value := []byte{1, 2, 3, 4, 5}

	err = ma_tx.Put(num, hash, value, rwtx)
	require.NoError(t, err)
	returnv, err := ma_tx.Get(num, rwtx)
	require.NoError(t, err)
	require.Equal(t, value, returnv)

	returnv, err = ma_tx.DebugDb().GetDb(num, hash, rwtx)
	require.NoError(t, err)
	require.Equal(t, value, returnv)

	returnv, err = ma_tx.DebugDb().GetDb(num, []byte{1}, rwtx)
	require.NoError(t, err)
	require.True(t, returnv == nil) // Equal fails

	require.Equal(t, ma_tx.Type(), kv.Marked)
}

func TestPrune(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// prune
	for pruneTo := RootNum(0); ; pruneTo++ {
		var entries_count uint64
		t.Run(fmt.Sprintf("prune to %d", pruneTo), func(t *testing.T) {
			dir, db, log := setup(t)
			headerId, ma := setupHeader(t, log, dir, db)

			ctx := context.Background()
			cfg := state.Registry.SnapshotConfig(headerId)
			extras_count := uint64(5) // in db
			entries_count = cfg.MinimumSize + cfg.SafetyMargin + extras_count

			ma_tx := ma.BeginTemporalTx()
			defer ma_tx.Close()
			rwtx, err := db.BeginRw(ctx)
			defer rwtx.Rollback()
			require.NoError(t, err)

			buffer := &bytes.Buffer{}

			getData := func(i int) (num Num, hash []byte, value []byte) {
				header := &types.Header{
					Number: big.NewInt(int64(i)),
					Extra:  []byte("test header"),
				}
				buffer.Reset()
				err = header.EncodeRLP(buffer)
				require.NoError(t, err)

				return Num(i), header.Hash().Bytes(), buffer.Bytes()
			}

			for i := range int(entries_count) {
				num, hash, value := getData(i)
				err = ma_tx.Put(num, hash, value, rwtx)
				require.NoError(t, err)
			}

			require.NoError(t, rwtx.Commit())
			ma_tx.Close()

			built := true
			from := RootNum(0)
			var df *state.FilesItem
			ps := background.NewProgressSet()

			for built {
				df, built, err = ma.BuildFile(ctx, from, RootNum(entries_count), db, 1, ps)
				require.NoError(t, err)
				if df != nil {
					ma.IntegrateDirtyFile(df)
					_, endTxNum := df.Range()
					from = RootNum(endTxNum)
				}
			}
			ma.RecalcVisibleFiles(RootNum(entries_count))

			ma_tx = ma.BeginTemporalTx()
			defer ma_tx.Close()

			rwtx, err = db.BeginRw(ctx)
			defer rwtx.Rollback()
			require.NoError(t, err)

			stat, err := ma_tx.Prune(ctx, pruneTo, 1000, nil, rwtx)
			require.NoError(t, err)
			cfgPruneFrom := int64(ma.PruneFrom())
			visibleFilesMaxRootNum := int64(entries_count - cfg.SafetyMargin - extras_count)
			require.Equal(t, max(0, min(int64(pruneTo), visibleFilesMaxRootNum)-cfgPruneFrom), int64(stat.PruneCount))

			require.NoError(t, rwtx.Commit())
			ma_tx.Close()
			ma_tx = ma.BeginTemporalTx()
			defer ma_tx.Close()
			rwtx, err = db.BeginRw(ctx)
			require.NoError(t, err)
			defer rwtx.Rollback()
		})
		if pruneTo >= RootNum(entries_count+1) {
			break
		}
	}

}
func TestUnwind(t *testing.T) {
	// unwind
}

func TestBuildFiles_Marked(t *testing.T) {
	// put stuff until build files is called
	// and snapshot is created (with indexes)
	// check beginfilesro works with new visible file

	// then check get
	// then prune and check get
	// then unwind and check get

	dir, db, log := setup(t)
	headerId, ma := setupHeader(t, log, dir, db)
	ctx := context.Background()

	ma_tx := ma.BeginTemporalTx()
	defer ma_tx.Close()
	rwtx, err := db.BeginRw(ctx)
	defer rwtx.Rollback()
	require.NoError(t, err)
	cfg := state.Registry.SnapshotConfig(headerId)
	entries_count := cfg.MinimumSize + cfg.SafetyMargin + /** in db **/ 2
	buffer := &bytes.Buffer{}

	getData := func(i int) (num Num, hash []byte, value []byte) {
		header := &types.Header{
			Number: big.NewInt(int64(i)),
			Extra:  []byte("test header"),
		}
		buffer.Reset()
		err = header.EncodeRLP(buffer)
		require.NoError(t, err)

		return Num(i), header.Hash().Bytes(), buffer.Bytes()
	}

	for i := range int(entries_count) {
		num, hash, value := getData(i)
		err = ma_tx.Put(num, hash, value, rwtx)
		require.NoError(t, err)
	}

	require.NoError(t, rwtx.Commit())
	ma_tx.Close()

	ps := background.NewProgressSet()
	from, to := RootNum(0), RootNum(entries_count)
	file, built, err := ma.BuildFile(ctx, from, to, db, 1, ps)
	require.NoError(t, err)
	require.NotNil(t, file)
	require.True(t, built)

	_, end := file.Range()
	from, to = RootNum(end), RootNum(entries_count)

	file2, built, err := ma.BuildFile(ctx, from, to, db, 1, ps)
	require.NoError(t, err)
	require.Nil(t, file2)
	require.False(t, built)

	ma.IntegrateDirtyFile(file)
	ma.RecalcVisibleFiles(RootNum(entries_count))

	ma_tx = ma.BeginTemporalTx()
	defer ma_tx.Close()

	rwtx, err = db.BeginRw(ctx)
	defer rwtx.Rollback()
	require.NoError(t, err)

	firstRootNumNotInSnap := ma_tx.DebugFiles().VisibleFilesMaxRootNum()
	stat, err := ma_tx.Prune(ctx, firstRootNumNotInSnap, 1000, nil, rwtx)
	require.NoError(t, err)
	require.Equal(t, stat.PruneCount, uint64(firstRootNumNotInSnap)-uint64(ma.PruneFrom()))

	require.NoError(t, rwtx.Commit())
	ma_tx = ma.BeginTemporalTx()
	defer ma_tx.Close()
	rwtx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwtx.Rollback()

	// check unified interface
	for i := range int(entries_count) {
		num, hash, value := getData(i)
		returnv, err := ma_tx.DebugDb().GetDb(num, nil, rwtx)
		require.NoError(t, err)
		if num < Num(firstRootNumNotInSnap) && num >= ma.PruneFrom() {
			require.True(t, returnv == nil)
		} else {
			require.Equal(t, returnv, value)
		}

		// just look in db....
		if num < ma.PruneFrom() || num >= Num(firstRootNumNotInSnap) {
			// these should be in db
			returnv, err = ma_tx.DebugDb().GetDb(num, hash, rwtx)
			require.NoError(t, err)
			require.Equal(t, value, returnv)

			value, err = rwtx.GetOne(kv.HeaderCanonical, num.EncTo8Bytes())
			require.NoError(t, err)
			require.Equal(t, value, hash)
			found := false

			rwtx.ForAmount(kv.Headers, num.EncTo8Bytes(), 1, func(k, v []byte) error {
				require.Equal(t, k[:8], num.EncTo8Bytes())
				found = true
				require.Equal(t, k[8:], hash)
				return nil
			})

			require.True(t, found, "value expected to be found in db")
		} else {
			// these should not be in db (pruned)
			value, err = rwtx.GetOne(kv.HeaderCanonical, num.EncTo8Bytes())
			require.NoError(t, err)
			require.True(t, value == nil)

			rwtx.ForAmount(kv.Headers, num.EncTo8Bytes(), 1, func(k, v []byte) error {
				if !bytes.Equal(k[:8], num.EncTo8Bytes()) {
					return nil
				}
				require.Fail(t, "should not be in db")
				return nil
			})
		}
	}
}

func TestMerging(t *testing.T) {
	// keep stuffing data until things merge
	// begin files ro should give "right" files.
}
