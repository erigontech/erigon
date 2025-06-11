package temporal

import (
	"context"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/state"
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

// implements both MarkedRoTx and MarkedRwTx
type markedTx struct {
	forkableBaseTx
	s    state.MarkedTxI
	dbtx kv.Tx
}

func newMarkedRoTx(dbtx kv.Tx, s state.MarkedTxI) *markedTx {
	return &markedTx{
		forkableBaseTx: forkableBaseTx{f: s.DebugFiles()},
		s:              s,
		dbtx:           dbtx,
	}
}

func (m *markedTx) Get(num Num) ([]byte, error) {
	return m.s.Get(num, m.dbtx)
}

func (m *markedTx) Debug() kv.ForkableRoTxCommons {
	return m
}

func (m *markedTx) RoDbDebug() kv.MarkedDbRoTx {
	return m
}

func (m *markedTx) GetDb(num Num, hash []byte) ([]byte, error) {
	return m.s.DebugDb().GetDb(num, hash, m.dbtx)
}

func (m *markedTx) HasRootNumUpto(ctx context.Context, to RootNum) (bool, error) {
	return m.s.DebugDb().HasRootNumUpto(ctx, to, m.dbtx)
}

func (m *markedTx) Type() kv.CanonicityStrategy {
	return m.s.Type()
}

func (m *markedTx) Close() {
	m.s.Close()
}

func (m *markedTx) Put(num Num, hash, v []byte) error {
	return m.s.Put(num, hash, v, m.dbtx.(kv.RwTx))
}

func (m *markedTx) RwDbDebug() kv.MarkedDbRwTx {
	return m
}

func (m *markedTx) Prune(ctx context.Context, to RootNum, limit uint64) (uint64, error) {
	return m.s.DebugDb().Prune(ctx, to, limit, m.dbtx.(kv.RwTx))
}

func (m *markedTx) Unwind(ctx context.Context, from RootNum) error {
	return m.s.DebugDb().Unwind(ctx, from, m.dbtx.(kv.RwTx))
}
