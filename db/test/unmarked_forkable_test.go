package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/polygon/heimdall"
)

type BorSpanRootRelation struct{}

func (r *BorSpanRootRelation) RootNum2Num(from state.RootNum, tx kv.Tx) (state.Num, error) {
	return Num(CustomSpanIdAt(uint64(from))), nil
}

func setupBorSpans(t *testing.T, log log.Logger, dirs datadir.Dirs, db kv.RoDB) (ForkableId, *state.Forkable[UnmarkedTxI]) {
	t.Helper()
	id := kv.ForkableId(2)
	stepSize := uint64(10)
	name := "borspans"
	snapCfg := state.NewSnapshotConfig(
		&state.SnapshotCreationConfig{
			RootNumPerStep: stepSize,
			MergeStages:    []uint64{200, 400},
			MinimumSize:    10,
			SafetyMargin:   5,
		},
		state.NewE2SnapSchemaWithStep(dirs, name, []string{name}, stepSize),
	)
	registerEntityWithSnapshotConfig(dirs, name, id, snapCfg)

	cfg := &statecfg.ForkableCfg{ValsTbl: kv.BorSpans, ValuesOnCompressedPage: 1}
	indexb := state.NewSimpleAccessorBuilder(
		state.NewAccessorArgs(false, false, cfg.ValuesOnCompressedPage, stepSize),
		id, dirs.Tmp, log)

	uma, err := state.NewUnmarkedForkable(id,
		cfg,
		&BorSpanRootRelation{},
		dirs,
		log,
		state.App_WithIndexBuilders(indexb))
	require.NoError(t, err)

	cleanup(t, uma.ProtoForkable, db, dirs)
	return id, uma
}

// TESTS BEGIN HERE

func TestUnmarkedForkableRegistration(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	require.NotPanics(t, func() { registerEntity(dirs, "borspans", kv.ForkableId(2)) })
}

func TestUnmarked_PutToDb(t *testing.T) {
	dir, db, log := setup(t)
	_, uma := setupBorSpans(t, log, dir, db)

	uma_tx := uma.BeginTemporalTx()
	defer uma_tx.Close()
	rwtx, err := db.BeginRw(context.Background())
	defer rwtx.Rollback()
	require.NoError(t, err)

	num := Num(0)
	value := []byte{1, 2, 3, 4, 5}

	err = uma_tx.Append(num, value, rwtx)
	require.NoError(t, err)
	returnv, err := uma_tx.Get(num, rwtx)
	require.NoError(t, err)
	require.Equal(t, value, returnv)

	returnv, err = uma_tx.Get(Num(1), rwtx)
	require.NoError(t, err)
	//require.True(t, returnv == nil)a
	require.True(t, returnv == nil)
}

func TestUnmarkedPrune(t *testing.T) {
	for pruneTo := RootNum(0); ; pruneTo++ {
		var entries_count uint64
		t.Run(fmt.Sprintf("prune to %d", pruneTo), func(t *testing.T) {
			dir, db, log := setup(t)
			borSpanId, uma := setupBorSpans(t, log, dir, db)

			ctx := context.Background()
			cfg := state.Registry.SnapshotConfig(borSpanId)
			extras_count := uint64(5) // db
			entries_count = cfg.MinimumSize + cfg.SafetyMargin + extras_count

			uma_tx := uma.BeginTemporalTx()
			defer uma_tx.Close()
			rwtx, err := db.BeginRw(ctx)
			defer rwtx.Rollback()
			require.NoError(t, err)

			getData := func(i int) (Num, state.Bytes) {
				return Num(i), state.Bytes(fmt.Sprintf("data%d", i))
			}

			for i := range int(entries_count) {
				num, value := getData(i)
				err = uma_tx.Append(num, value, rwtx)
				require.NoError(t, err)
			}

			require.NoError(t, rwtx.Commit())
			uma_tx.Close()

			built := true
			from := RootNum(0)
			var df *state.FilesItem
			ps := background.NewProgressSet()

			for built {
				df, built, err = uma.BuildFile(ctx, from, RootNum(entries_count), db, 1, ps)
				require.NoError(t, err)
				if df != nil {
					uma.IntegrateDirtyFile(df)
					_, endTxNum := df.Range()
					from = RootNum(endTxNum)
				}
			}
			uma.RecalcVisibleFiles(RootNum(entries_count))

			uma_tx = uma.BeginTemporalTx()
			defer uma_tx.Close()

			rwtx, err = db.BeginRw(ctx)
			defer rwtx.Rollback()
			require.NoError(t, err)

			stat, err := uma_tx.Prune(ctx, pruneTo, 1000, nil, rwtx)
			require.NoError(t, err)

			visibleFilesMaxNum := CustomSpanIdAt(min(uint64(pruneTo), entries_count-cfg.SafetyMargin-extras_count))
			require.Equal(t, stat.PruneCount, uint64(visibleFilesMaxNum))

			require.NoError(t, rwtx.Commit())
			uma_tx.Close()
			uma_tx = uma.BeginTemporalTx()
			defer uma_tx.Close()
			rwtx, err = db.BeginRw(ctx)
			require.NoError(t, err)
			defer rwtx.Rollback()
		})
		if uint64(CustomSpanIdAt(uint64(pruneTo))) >= entries_count+1 {
			break
		}
	}
}

const (
	customSpanLength    = 10 // Number of blocks in a span
	customZerothSpanEnd = 2  // End block of 0th span
)

// SpanIdAt returns the corresponding span id for the given block number.
func CustomSpanIdAt(blockNum uint64) heimdall.SpanId {
	if blockNum > customZerothSpanEnd {
		return heimdall.SpanId(1 + (blockNum-customZerothSpanEnd-1)/customSpanLength)
	}
	return 0
}

func TestBuildFiles_Unmarked(t *testing.T) {
	dir, db, log := setup(t)
	borSpanId, uma := setupBorSpans(t, log, dir, db)
	ctx := context.Background()

	uma_tx := uma.BeginTemporalTx()
	defer uma_tx.Close()
	rwtx, err := db.BeginRw(ctx)
	defer rwtx.Rollback()
	require.NoError(t, err)
	cfg := state.Registry.SnapshotConfig(borSpanId)
	num_files := uint64(5)
	entries_count := num_files*cfg.MinimumSize + cfg.SafetyMargin + /** in db **/ 5

	getData := func(i int) (Num, state.Bytes) {
		return Num(i), state.Bytes(fmt.Sprintf("data%d", i))
	}

	for i := range int(entries_count) {
		num, value := getData(i)
		err = uma_tx.Append(num, value, rwtx)
		require.NoError(t, err)
	}

	require.NoError(t, rwtx.Commit())
	uma_tx.Close()

	ps := background.NewProgressSet()

	built := true
	i := 0
	from, to := RootNum(0), RootNum(entries_count)
	files := make([]*state.FilesItem, 0)
	for built {
		file, built2, err := uma.BuildFile(ctx, from, to, db, 1, ps)
		require.NoError(t, err)
		if i < int(num_files) {
			require.NotNil(t, file)
			require.True(t, built2)
			files = append(files, file)
		} else {
			require.Nil(t, file)
			require.False(t, built2)
			built = built2
			continue
		}
		i++
		_, endTxNum := file.Range()
		from, to = RootNum(endTxNum), RootNum(entries_count)
	}

	require.Len(t, files, int(num_files))
	uma.IntegrateDirtyFiles(files)
	uma.RecalcVisibleFiles(RootNum(entries_count))

	uma_tx = uma.BeginTemporalTx()
	defer uma_tx.Close()

	rwtx, err = db.BeginRw(ctx)
	defer rwtx.Rollback()
	require.NoError(t, err)

	firstRootNumNotInSnap := uma_tx.DebugFiles().VisibleFilesMaxRootNum()
	firstSpanIdNotInSnap := Num(CustomSpanIdAt(uint64(firstRootNumNotInSnap)))
	stat, err := uma_tx.Prune(ctx, firstRootNumNotInSnap, 1000, nil, rwtx)
	require.NoError(t, err)
	require.Equal(t, uint64(CustomSpanIdAt(uint64(firstRootNumNotInSnap)-uint64(uma.PruneFrom()))), stat.PruneCount)

	require.NoError(t, rwtx.Commit())
	uma_tx = uma.BeginTemporalTx()
	defer uma_tx.Close()
	rwtx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwtx.Rollback()

	for i := range int(entries_count) {
		num, value := getData(i)
		returnv, err := uma_tx.DebugDb().GetDb(num, rwtx)
		require.NoError(t, err)
		if num < firstSpanIdNotInSnap {
			require.True(t, returnv == nil)
		} else {
			require.Equal(t, returnv, value)
		}

		returnv, found, _, err := uma_tx.DebugFiles().GetFromFiles(num)
		require.NoError(t, err)
		if num < firstSpanIdNotInSnap {
			require.True(t, found)
			require.Equal(t, returnv, value)
		} else {
			require.False(t, found)
			require.True(t, returnv == nil)
		}
	}
}

func TestBuildFiles_PagedUnmarked(t *testing.T) {
	dir, db, log := setup(t)
	pagedDataId, uma := setupPagedEntity(t, log, dir, db)
	ctx := context.Background()

	uma_tx := uma.BeginTemporalTx()
	defer uma_tx.Close()
	rwtx, err := db.BeginRw(ctx)
	defer rwtx.Rollback()
	require.NoError(t, err)
	cfg := state.Registry.SnapshotConfig(pagedDataId)
	num_files := uint64(5)
	entries_count := num_files*cfg.MinimumSize + cfg.SafetyMargin + /** in db **/ 5

	getData := func(i int) (Num, state.Bytes) {
		return Num(i), state.Bytes(fmt.Sprintf("data%d", i))
	}

	for i := range int(entries_count) {
		num, value := getData(i)
		err = uma_tx.Append(num, value, rwtx)
		require.NoError(t, err)
	}

	require.NoError(t, rwtx.Commit())
	uma_tx.Close()

	ps := background.NewProgressSet()

	built := true
	i := 0
	from, to := RootNum(0), RootNum(entries_count)
	files := make([]*state.FilesItem, 0)
	for built {
		file, built2, err := uma.BuildFile(ctx, from, to, db, 1, ps)
		require.NoError(t, err)
		if i < int(num_files) {
			require.NotNil(t, file)
			require.True(t, built2)
			files = append(files, file)
		} else {
			require.Nil(t, file)
			require.False(t, built2)
			built = built2
			continue
		}
		i++
		_, endTxNum := file.Range()
		from, to = RootNum(endTxNum), RootNum(entries_count)
	}

	require.Len(t, files, int(num_files))
	uma.IntegrateDirtyFiles(files)
	uma.RecalcVisibleFiles(RootNum(entries_count))

	uma_tx = uma.BeginTemporalTx()
	defer uma_tx.Close()

	rwtx, err = db.BeginRw(ctx)
	defer rwtx.Rollback()
	require.NoError(t, err)

	firstRootNumNotInSnap := uma_tx.DebugFiles().VisibleFilesMaxRootNum()
	firstNumNotInSnap := uma_tx.DebugFiles().VisibleFilesMaxNum()
	require.Equal(t, firstRootNumNotInSnap.Uint64(), firstNumNotInSnap.Uint64())
	stat, err := uma_tx.Prune(ctx, firstRootNumNotInSnap, 100000, nil, rwtx)
	require.NoError(t, err)
	require.Equal(t, uint64(firstRootNumNotInSnap)-uint64(uma.PruneFrom()), stat.PruneCount)

	require.NoError(t, rwtx.Commit())
	uma_tx = uma.BeginTemporalTx()
	defer uma_tx.Close()
	rwtx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwtx.Rollback()

	for i := range int(entries_count) {
		num, value := getData(i)
		returnv, err := uma_tx.DebugDb().GetDb(num, rwtx)
		require.NoError(t, err)
		if num < firstNumNotInSnap {
			require.True(t, returnv == nil)
		} else {
			require.Equal(t, returnv, value)
		}

		returnv, found, _, err := uma_tx.DebugFiles().GetFromFiles(num)
		require.NoError(t, err)
		if num < firstNumNotInSnap {
			require.True(t, found)
			require.Equal(t, value, returnv)
		} else {
			require.False(t, found)
			require.True(t, returnv == nil)
		}
	}
}

func setupPagedEntity(t *testing.T, log log.Logger, dirs datadir.Dirs, db kv.RwDB) (ForkableId, *state.Forkable[UnmarkedTxI]) {
	t.Helper()
	id := kv.ForkableId(3)
	stepSize := uint64(50)
	name := "random_paged_data"
	snapCfg := state.NewSnapshotConfig(
		&state.SnapshotCreationConfig{
			RootNumPerStep: stepSize,
			MergeStages:    []uint64{400, 800},
			MinimumSize:    50,
			SafetyMargin:   5,
		},
		state.NewE2SnapSchemaWithStep(dirs, name, []string{name}, stepSize),
	)
	registerEntityWithSnapshotConfig(dirs, name, id, snapCfg)

	cfg := &statecfg.ForkableCfg{ValsTbl: "TestPagedTable", ValuesOnCompressedPage: 10}
	indexb := state.NewSimpleAccessorBuilder(
		state.NewAccessorArgs(false, false, cfg.ValuesOnCompressedPage, stepSize),
		id, dirs.Tmp, log)

	rwtx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwtx.Commit()

	require.NoError(t, rwtx.CreateTable(cfg.ValsTbl))

	uma, err := state.NewUnmarkedForkable(id,
		cfg,
		&state.IdentityRootRelation{},
		dirs,
		log,
		state.App_WithIndexBuilders(indexb))
	require.NoError(t, err)

	cleanup(t, uma.ProtoForkable, db, dirs)
	return id, uma
}
