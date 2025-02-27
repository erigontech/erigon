package state_test

import (
	"bytes"
	"context"
	"math/big"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	ae "github.com/erigontech/erigon-lib/state/entity_extras"
	"github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/core/types"
	"github.com/stretchr/testify/require"
)

type Num = state.Num
type RootNum = state.RootNum
type EntityId = ae.EntityId

// test marked appendable
func TestMarkedAppendableRegistration(t *testing.T) {
	// just registration goes fine
	dirs := datadir.New(t.TempDir())
	blockId := registerEntity(dirs, "blocks")
	require.Equal(t, ae.EntityId(0), blockId)
	headerId := registerEntity(dirs, "headers")
	require.Equal(t, ae.EntityId(1), headerId)
}

func registerEntity(dirs datadir.Dirs, name string) ae.EntityId {
	preverified := snapcfg.Mainnet
	return ae.RegisterEntity(name, dirs, preverified, ae.WithSnapshotCreationConfig(
		&ae.SnapshotConfig{
			SnapshotCreationConfig: &ae.SnapshotCreationConfig{
				EntitiesPerStep: 1000,
				MergeStages:     []uint64{1000, 20000, 600000},
				MinimumSize:     1000000,
				SafetyMargin:    1000,
			},
		},
	))
}

func registerEntityRelaxed(dirs datadir.Dirs, name string) ae.EntityId {
	return ae.RegisterEntity(name, dirs, nil, ae.WithSnapshotCreationConfig(
		&ae.SnapshotConfig{
			SnapshotCreationConfig: &ae.SnapshotCreationConfig{
				EntitiesPerStep: 10,
				MergeStages:     []uint64{20, 40},
				MinimumSize:     10,
				SafetyMargin:    5,
			},
		},
	))
}

func setup(tb testing.TB) (datadir.Dirs, kv.RwDB, log.Logger) {
	tb.Helper()
	logger := log.New()
	dirs := datadir.New(tb.TempDir())
	db := mdbx.New(kv.ChainDB, logger).InMem(dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	tb.Cleanup(db.Close)
	return dirs, db, logger
}

func setupHeader(t *testing.T, log log.Logger, dir datadir.Dirs) (EntityId, *state.Appendable[state.MarkedTxI]) {
	headerId := registerEntityRelaxed(dir, "headers")
	require.Equal(t, ae.EntityId(0), headerId)

	// create marked appendable
	freezer := snaptype.NewHeaderFreezer(kv.HeaderCanonical, kv.Headers, log)

	builder := state.NewSimpleAccessorBuilder(state.NewAccessorArgs(true, true), headerId, log,
		state.WithIndexKeyFactory(&snaptype.HeaderAccessorIndexKeyFactory{}))

	ma, err := state.NewMarkedAppendable(headerId, kv.Headers, kv.HeaderCanonical, ae.IdentityRootRelationInstance, log,
		state.App_WithFreezer[state.MarkedTxI](freezer),
		state.App_WithPruneFrom[state.MarkedTxI](Num(1)),
		state.App_WithIndexBuilders[state.MarkedTxI](builder),
	)
	require.NoError(t, err)

	return headerId, ma
}

func TestMarked_PutToDb(t *testing.T) {
	/*
		put, get, getnc on db-only data
	*/
	dir, db, log := setup(t)
	_, ma := setupHeader(t, log, dir)

	ma_tx := ma.BeginFilesRo()
	defer ma_tx.Close()
	rwtx, err := db.BeginRw(context.Background())
	defer rwtx.Rollback()
	require.NoError(t, err)

	num := Num(1)
	hash := common.HexToHash("0x1234").Bytes()
	value := []byte{1, 2, 3, 4, 5}

	err = ma_tx.Put(num, hash, value, rwtx)
	require.NoError(t, err)
	returnv, snap, err := ma_tx.Get(num, rwtx.(kv.Tx))
	require.NoError(t, err)
	require.Equal(t, value, returnv)
	require.False(t, snap)

	returnv, err = ma_tx.GetNc(num, hash, rwtx.(kv.Tx))
	require.NoError(t, err)
	require.Equal(t, value, returnv)

	returnv, err = ma_tx.GetNc(num, []byte{1}, rwtx.(kv.Tx))
	require.NoError(t, err)
	require.True(t, returnv == nil) // Equal fails

	require.Equal(t, ma_tx.Type(), state.Marked)
}

func TestGetFromDb(t *testing.T) {
	// get
	// getnc
}

func TestPrune(t *testing.T) {
	// prune
}
func TestUnwind(t *testing.T) {
	// unwind
}

func TestBuildFiles(t *testing.T) {
	// put stuff until build files is called
	// and snapshot is created (with indexes)
	// check beginfilesro works with new visible file

	// then check get
	// then prune and check get
	// then unwind and check get

	dir, db, log := setup(t)
	headerId, ma := setupHeader(t, log, dir)
	ctx := context.Background()

	ma_tx := ma.BeginFilesRo()
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

	ma_tx = ma.BeginFilesRo()
	defer ma_tx.Close()

	rwtx, err = db.BeginRw(ctx)
	defer rwtx.Rollback()
	require.NoError(t, err)

	firstRootNumNotInSnap := ma_tx.VisibleFilesMaxRootNum()
	err = ma_tx.Prune(ctx, firstRootNumNotInSnap, 1000, rwtx)
	require.NoError(t, err)

	require.NoError(t, rwtx.Commit())
	ma_tx = ma.BeginFilesRo()
	defer ma_tx.Close()
	rwtx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwtx.Rollback()

	// check unified interface
	for i := range int(entries_count) {
		num, hash, value := getData(i)
		returnv, snap, err := ma_tx.Get(num, rwtx)
		require.NoError(t, err)

		if num < Num(firstRootNumNotInSnap) {
			require.True(t, snap)
			require.Equal(t, value, returnv[1:]) // headers freezer stores first byte as first byte of header hash
		} else {
			require.False(t, snap)
			require.Equal(t, value, returnv)
		}

		// just look in db....
		if num < ma.PruneFrom() || num >= Num(firstRootNumNotInSnap) {
			// these should be in db
			returnv, err = ma_tx.GetNc(num, hash, rwtx)
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
