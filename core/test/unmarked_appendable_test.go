package test

import (
	"context"
	"os"
	"testing"

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
	borspanId := registerEntity(dir, "borspans")
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

	uma_tx := uma.BeginFilesRo()
	defer uma_tx.Close()
	rwtx, err := db.BeginRw(context.Background())
	defer rwtx.Rollback()
	require.NoError(t, err)

	num := Num(0)
	value := []byte{1, 2, 3, 4, 5}

	err = uma_tx.Append(num, value, rwtx)
	require.NoError(t, err)
	returnv, found, err := uma_tx.Get(num, rwtx)
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, value, returnv)

	//returnv, err = uma_tx.GetNc(num, rwtx)

}
