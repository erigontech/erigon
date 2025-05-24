package test

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"os"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-db/snaptype"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	ee "github.com/erigontech/erigon-lib/state/entity_extras"
	"github.com/erigontech/erigon-lib/types"
)

type Num = state.Num
type RootNum = state.RootNum
type ForkableId = ee.ForkableId
type MarkedTxI = state.MarkedTxI
type UnmarkedTxI = state.UnmarkedTxI

func registerEntity(dirs datadir.Dirs, name string) ee.ForkableId {
	stepSize := uint64(10)
	return registerEntityWithSnapshotConfig(dirs, name, ee.NewSnapshotConfig(&ee.SnapshotCreationConfig{
		RootNumPerStep: 10,
		MergeStages:    []uint64{20, 40},
		MinimumSize:    10,
		SafetyMargin:   5,
	}, ee.NewE2SnapSchemaWithStep(dirs, name, []string{name}, stepSize)))

}

func registerEntityWithSnapshotConfig(dirs datadir.Dirs, name string, cfg *ee.SnapshotConfig) ee.ForkableId {
	return ee.RegisterForkable(name, dirs, nil, ee.WithSnapshotConfig(cfg))
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
	require.Equal(t, ee.ForkableId(0), headerId)

	// create marked forkable
	freezer := snaptype.NewHeaderFreezer(kv.HeaderCanonical, kv.Headers, log)

	builder := state.NewSimpleAccessorBuilder(state.NewAccessorArgs(true, true), headerId, log,
		state.WithIndexKeyFactory(&snaptype.HeaderAccessorIndexKeyFactory{}))

	ma, err := state.NewMarkedForkable(headerId, kv.Headers, kv.HeaderCanonical, ee.IdentityRootRelationInstance, log,
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

		ee.Cleanup()
		db.Close()
		os.RemoveAll(dirs.Snap)
		os.RemoveAll(dirs.Chaindata)
		os.RemoveAll(dirs.SnapIdx)
	})
}

// TESTS BEGIN HERE

// test marked forkable
func TestMarkedForkableRegistration(t *testing.T) {
	// just registration goes fine
	t.Cleanup(func() {
		ee.Cleanup()
	})
	dirs := datadir.New(t.TempDir())
	blockId := registerEntity(dirs, "blocks")
	require.Equal(t, ee.ForkableId(0), blockId)
	headerId := registerEntity(dirs, "headers")
	require.Equal(t, ee.ForkableId(1), headerId)
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

	require.Equal(t, ma_tx.Type(), state.Marked)
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
			cfg := headerId.SnapshotConfig()
			entries_count = cfg.MinimumSize + cfg.SafetyMargin + /** in db **/ 5

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

			rwtx, err = db.BeginRw(ctx)
			defer rwtx.Rollback()
			require.NoError(t, err)

			del, err := ma_tx.Prune(ctx, pruneTo, 1000, rwtx)
			require.NoError(t, err)
			cfgPruneFrom := int64(ma.PruneFrom())
			require.Equal(t, int64(del), max(0, min(int64(pruneTo), int64(entries_count))-cfgPruneFrom))

			require.NoError(t, rwtx.Commit())
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
	cfg := headerId.SnapshotConfig()
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
	file, built, err := ma.BuildFile(ctx, from, to, db, ps)
	require.NoError(t, err)
	require.NotNil(t, file)
	require.True(t, built)

	_, end := file.Range()
	from, to = RootNum(end), RootNum(entries_count)

	file2, built, err := ma.BuildFile(ctx, from, to, db, ps)
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
	del, err := ma_tx.Prune(ctx, firstRootNumNotInSnap, 1000, rwtx)
	require.NoError(t, err)
	require.Equal(t, del, uint64(firstRootNumNotInSnap)-uint64(ma.PruneFrom()))

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
