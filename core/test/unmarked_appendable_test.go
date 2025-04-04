package test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon-lib/state"
	ae "github.com/erigontech/erigon-lib/state/appendable_extras"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/stretchr/testify/require"
)

type BorSpanRootRelation struct{}

func (r *BorSpanRootRelation) RootNum2Num(from state.RootNum, tx kv.Tx) (state.Num, error) {
	return Num(heimdall.SpanIdAt(uint64(from))), nil
}

func TestUnmarkedAppendableRegistration(t *testing.T) {
	t.Cleanup(func() {
		ae.Cleanup()
	})
	dirs := datadir.New(t.TempDir())
	blockId := registerEntity(dirs, "borspans")
	require.Equal(t, ae.AppendableId(0), blockId)
}

func setupBorSpans(t *testing.T, log log.Logger, dir datadir.Dirs, db kv.RoDB) (AppendableId, *state.Appendable[UnmarkedTxI]) {
	minAggStep := uint64(10)
	name := "borspans"
	borspanId := registerEntityWithSnapshotConfig(dir, name, &ae.SnapshotConfig{
		SnapshotCreationConfig: &ae.SnapshotCreationConfig{
			RootNumPerStep: minAggStep,
			MergeStages:    []uint64{200, 400},
			MinimumSize:    10,
			SafetyMargin:   5,
		},
		Directory: dir.Snap,
		Parser:    ae.NewE2ParserWithStep(minAggStep, dir.Snap, name, []string{name}),
	})
	require.Equal(t, ae.AppendableId(0), borspanId)

	indexb := state.NewSimpleAccessorBuilder(state.NewAccessorArgs(true, false), borspanId, log)
	indexb.SetFirstEntityNumFetcher(func(from, to RootNum, seg *seg.Decompressor) Num {
		return Num(heimdall.SpanIdAt(uint64(from)))
	})

	uma, err := state.NewUnmarkedAppendable(borspanId,
		kv.BorSpans,
		&BorSpanRootRelation{},
		log,
		state.App_WithIndexBuilders(indexb))
	require.NoError(t, err)

	t.Cleanup(func() {
		uma.Close()
		uma.RecalcVisibleFiles(0)

		ae.Cleanup()
		db.Close()
		os.RemoveAll(dir.Snap)
		os.RemoveAll(dir.Chaindata)
	})

	return borspanId, uma
}

func TestUnmarked_PutToDb(t *testing.T) {
	dir, db, log := setup(t)
	_, uma := setupBorSpans(t, log, dir, db)

	uma_tx := uma.BeginFilesTx()
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
			cfg := borSpanId.SnapshotConfig()
			entries_count := cfg.MinimumSize + cfg.SafetyMargin + /** in db **/ 5

			uma_tx := uma.BeginFilesTx()
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

			ndels, err := uma_tx.Prune(ctx, pruneTo, 1000, rwtx)
			require.NoError(t, err)

			spanId := heimdall.SpanIdAt(uint64(pruneTo))
			require.Equal(t, ndels, uint64(spanId))

			require.NoError(t, rwtx.Commit())
			uma_tx = uma.BeginFilesTx()
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

	uma_tx := uma.BeginFilesTx()
	defer uma_tx.Close()
	rwtx, err := db.BeginRw(ctx)
	defer rwtx.Rollback()
	require.NoError(t, err)
	cfg := borSpanId.SnapshotConfig()
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
	files, err := uma.BuildFiles(ctx, 0, RootNum(entries_count), db, ps)
	require.NoError(t, err)
	require.Equal(t, len(files), int(num_files))

	uma.IntegrateDirtyFiles(files)
	uma.RecalcVisibleFiles(RootNum(entries_count))

	uma_tx = uma.BeginFilesTx()
	defer uma_tx.Close()

	rwtx, err = db.BeginRw(ctx)
	defer rwtx.Rollback()
	require.NoError(t, err)

	firstRootNumNotInSnap := uma_tx.DebugFiles().VisibleFilesMaxRootNum()
	firstSpanIdNotInSnap := Num(heimdall.SpanIdAt(uint64(firstRootNumNotInSnap)))
	del, err := uma_tx.Prune(ctx, firstRootNumNotInSnap, 1000, rwtx)
	require.NoError(t, err)
	require.Equal(t, del, uint64(firstSpanIdNotInSnap))

	require.NoError(t, rwtx.Commit())
	uma_tx = uma.BeginFilesTx()
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
