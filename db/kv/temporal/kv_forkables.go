package temporal

import (
	"context"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
)

type Num = kv.Num
type RootNum = kv.RootNum

type forkableBaseTx struct {
	f state.ForkableFilesTxI
}

func (b *forkableBaseTx) GetFromFiles(entityNum Num) (v []byte, found bool, fileIdx int, err error) {
	return b.f.GetFromFiles(entityNum)
}

func (b *forkableBaseTx) VisibleFilesMaxRootNum() RootNum {
	return b.f.VisibleFilesMaxRootNum()
}

func (b *forkableBaseTx) VisibleFilesMaxNum() Num {
	return b.f.VisibleFilesMaxNum()
}

func (b *forkableBaseTx) VisibleFiles() kv.VisibleFiles {
	return b.f.VisibleFiles()
}

func (b *forkableBaseTx) GetFromFile(entityNum Num, idx int) (v []byte, found bool, err error) {
	return b.f.GetFromFile(entityNum, idx)
}

// implements both UnmarkedRoTx and UnmarkedRwTx
type unmarkedTx struct {
	forkableBaseTx
	s    state.UnmarkedTxI
	dbtx kv.Tx
}

var (
	_ kv.UnmarkedRwTx = (*unmarkedTx)(nil)
)

func newUnmarkedTx(dbtx kv.Tx, s state.UnmarkedTxI) *unmarkedTx {
	return &unmarkedTx{
		forkableBaseTx: forkableBaseTx{f: s.DebugFiles()},
		s:              s,
		dbtx:           dbtx,
	}
}

func (m *unmarkedTx) Get(num Num) ([]byte, error) {
	return m.s.Get(num, m.dbtx)
}

func (m *unmarkedTx) Debug() kv.ForkableTxCommons {
	return m
}

func (m *unmarkedTx) RoDbDebug() kv.UnmarkedDbTx {
	return m
}

func (m *unmarkedTx) GetDb(num Num) ([]byte, error) {
	return m.s.DebugDb().GetDb(num, m.dbtx)
}

func (m *unmarkedTx) HasRootNumUpto(ctx context.Context, to RootNum) (bool, error) {
	return m.s.DebugDb().HasRootNumUpto(ctx, to, m.dbtx)
}

func (m *unmarkedTx) Type() kv.CanonicityStrategy {
	return m.s.Type()
}

func (m *unmarkedTx) Close() {
	m.s.Close()
}

func (m *unmarkedTx) Append(num Num, v []byte) error {
	return m.s.Append(num, v, m.dbtx.(kv.RwTx))
}

func (m *unmarkedTx) RwDbDebug() kv.UnmarkedDbRwTx {
	return m
}

func (m *unmarkedTx) Prune(ctx context.Context, to RootNum, limit uint64) (uint64, error) {
	stat, err := m.s.DebugDb().Prune(ctx, to, limit, nil, m.dbtx.(kv.RwTx))
	return stat.PruneCount, err
}

func (m *unmarkedTx) Unwind(ctx context.Context, from RootNum) error {
	_, err := m.s.DebugDb().Unwind(ctx, from, m.dbtx.(kv.RwTx))
	return err
}
