package test

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"os"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	ae "github.com/erigontech/erigon-lib/state/appendable_extras"
	"github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/core/types"
	"github.com/stretchr/testify/require"
)

type Num = state.Num
type RootNum = state.RootNum
type AppendableId = ae.AppendableId
type MarkedTxI = state.MarkedTxI
type UnmarkedTxI = state.UnmarkedTxI

// test marked appendable
func TestMarkedAppendableRegistration(t *testing.T) {
	// just registration goes fine
	t.Cleanup(func() {
		ae.Cleanup()
	})
	dirs := datadir.New(t.TempDir())
	blockId := registerEntity(dirs, "blocks")
	require.Equal(t, ae.AppendableId(0), blockId)
	headerId := registerEntity(dirs, "headers")
	require.Equal(t, ae.AppendableId(1), headerId)
}

func registerEntity(dirs datadir.Dirs, name string) ae.AppendableId {
	return registerEntityWithSnapshotConfig(dirs, name, &ae.SnapshotConfig{
		SnapshotCreationConfig: &ae.SnapshotCreationConfig{
			RootNumPerStep: 10,
			MergeStages:    []uint64{20, 40},
			MinimumSize:    10,
			SafetyMargin:   5,
		},
	})
}

func registerEntityWithSnapshotConfig(dirs datadir.Dirs, name string, cfg *ae.SnapshotConfig) ae.AppendableId {
	return ae.RegisterAppendable(name, dirs, nil, ae.WithSnapshotCreationConfig(cfg))
}

func setup(tb testing.TB) (datadir.Dirs, kv.RwDB, log.Logger) {
	tb.Helper()
	logger := log.New()
	dirs := datadir.New(tb.TempDir())
	db := mdbx.New(kv.ChainDB, logger).InMem(dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	return dirs, db, logger
}

func setupHeader(t *testing.T, log log.Logger, dir datadir.Dirs, db kv.RoDB) (AppendableId, *state.Appendable[state.MarkedTxI]) {
	headerId := registerEntity(dir, "headers")
	require.Equal(t, ae.AppendableId(0), headerId)

	// create marked appendable
	freezer := snaptype.NewHeaderFreezer(kv.HeaderCanonical, kv.Headers, log)

	builder := state.NewSimpleAccessorBuilder(state.NewAccessorArgs(true, true), headerId, log,
		state.WithIndexKeyFactory(&snaptype.HeaderAccessorIndexKeyFactory{}))

	ma, err := state.NewMarkedAppendable(headerId, kv.Headers, kv.HeaderCanonical, ae.IdentityRootRelationInstance, log,
		state.App_WithFreezer(freezer),
		state.App_WithPruneFrom(Num(1)),
		state.App_WithIndexBuilders(builder),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		ma.Close()
		ma.RecalcVisibleFiles(0)

		ae.Cleanup()
		db.Close()
		os.RemoveAll(dir.Snap)
		os.RemoveAll(dir.Chaindata)
	})

	return headerId, ma
}

func TestMarked_PutToDb(t *testing.T) {
	/*
		put, get, getnc on db-only data
	*/
	dir, db, log := setup(t)
	_, ma := setupHeader(t, log, dir, db)

	ma_tx := ma.BeginFilesTx()
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
	// prune
	for pruneTo := RootNum(0); ; pruneTo++ {
		var entries_count uint64
		t.Run(fmt.Sprintf("prune to %d", pruneTo), func(t *testing.T) {
			dir, db, log := setup(t)
			headerId, ma := setupHeader(t, log, dir, db)

			ctx := context.Background()
			cfg := headerId.SnapshotConfig()
			entries_count = cfg.MinimumSize + cfg.SafetyMargin + /** in db **/ 5

			ma_tx := ma.BeginFilesTx()
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
			ma_tx = ma.BeginFilesTx()
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

	ma_tx := ma.BeginFilesTx()
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
	files, err := ma.BuildFiles(ctx, 0, RootNum(entries_count), db, ps)
	require.NoError(t, err)
	require.True(t, len(files) == 1) // 1 snapshot made

	ma.IntegrateDirtyFiles(files)
	ma.RecalcVisibleFiles(RootNum(entries_count))

	ma_tx = ma.BeginFilesTx()
	defer ma_tx.Close()

	rwtx, err = db.BeginRw(ctx)
	defer rwtx.Rollback()
	require.NoError(t, err)

	firstRootNumNotInSnap := ma_tx.DebugFiles().VisibleFilesMaxRootNum()
	del, err := ma_tx.Prune(ctx, firstRootNumNotInSnap, 1000, rwtx)
	require.NoError(t, err)
	require.Equal(t, del, uint64(firstRootNumNotInSnap)-uint64(ma.PruneFrom()))

	require.NoError(t, rwtx.Commit())
	ma_tx = ma.BeginFilesTx()
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
