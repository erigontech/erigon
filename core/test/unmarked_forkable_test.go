package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon-lib/state"
	ee "github.com/erigontech/erigon-lib/state/entity_extras"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/stretchr/testify/require"
)

type BorSpanRootRelation struct{}

func (r *BorSpanRootRelation) RootNum2Num(from state.RootNum, tx kv.Tx) (state.Num, error) {
	return Num(heimdall.SpanIdAt(uint64(from))), nil
}

func setupBorSpans(t *testing.T, log log.Logger, dirs datadir.Dirs, db kv.RoDB) (ForkableId, *state.Forkable[UnmarkedTxI]) {
	stepSize := uint64(10)
	name := "borspans"
	borspanId := registerEntityWithSnapshotConfig(dirs, name, ee.NewSnapshotConfig(
		&ee.SnapshotCreationConfig{
			RootNumPerStep: stepSize,
			MergeStages:    []uint64{200, 400},
			MinimumSize:    10,
			SafetyMargin:   5,
		},
		ee.NewE2SnapSchemaWithStep(dirs, name, []string{name}, stepSize),
	))
	require.Equal(t, ee.ForkableId(0), borspanId)

	indexb := state.NewSimpleAccessorBuilder(state.NewAccessorArgs(true, false), borspanId, log)
	indexb.SetFirstEntityNumFetcher(func(from, to RootNum, seg *seg.Decompressor) Num {
		return Num(heimdall.SpanIdAt(uint64(from)))
	})

	uma, err := state.NewUnmarkedForkable(borspanId,
		kv.BorSpans,
		&BorSpanRootRelation{},
		log,
		state.App_WithIndexBuilders(indexb))
	require.NoError(t, err)

	cleanup(t, uma.ProtoForkable, db, dirs)
	return borspanId, uma
}

// TESTS BEGIN HERE

func TestUnmarkedForkableRegistration(t *testing.T) {
	t.Cleanup(func() {
		ee.Cleanup()
	})
	dirs := datadir.New(t.TempDir())
	blockId := registerEntity(dirs, "borspans")
	require.Equal(t, ee.ForkableId(0), blockId)
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
			cfg := ee.Registry.SnapshotConfig(borSpanId)
			entries_count := cfg.MinimumSize + cfg.SafetyMargin + /** in db **/ 5

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

			rwtx, err = db.BeginRw(ctx)
			defer rwtx.Rollback()
			require.NoError(t, err)

			stat, err := uma_tx.Prune(ctx, pruneTo, 1000, nil, rwtx)
			require.NoError(t, err)

			spanId := heimdall.SpanIdAt(uint64(pruneTo))
			require.Equal(t, stat.PruneCount, uint64(spanId))

			require.NoError(t, rwtx.Commit())
			uma_tx = uma.BeginTemporalTx()
			defer uma_tx.Close()
			rwtx, err = db.BeginRw(ctx)
			require.NoError(t, err)
			defer rwtx.Rollback()
		})
		if pruneTo >= RootNum(entries_count+1) {
			break
		}
	}
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
	cfg := ee.Registry.SnapshotConfig(borSpanId)
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
	firstSpanIdNotInSnap := Num(heimdall.SpanIdAt(uint64(firstRootNumNotInSnap)))
	stat, err := uma_tx.Prune(ctx, firstRootNumNotInSnap, 1000, nil, rwtx)
	require.NoError(t, err)
	require.Equal(t, stat.PruneCount, uint64(firstSpanIdNotInSnap))

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
