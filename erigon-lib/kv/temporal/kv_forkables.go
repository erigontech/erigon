package temporal

import (
	"context"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/state"
)

type Num = kv.Num
type RootNum = kv.RootNum

// some convenience objects....

type baseRoTx struct {
	f state.ForkableFilesTxI
}

type markedRoTx struct {
	baseRoTx
	s    state.MarkedTxI
	dbtx kv.Tx
}

func newMarkedRoTx(dbtx kv.Tx, s state.MarkedTxI) *markedRoTx {
	return &markedRoTx{
		baseRoTx: baseRoTx{f: s.DebugFiles()},
		s:        s,
		dbtx:     dbtx,
	}
}

func (m *markedRoTx) Get(num Num) ([]byte, error) {
	return m.s.Get(num, m.dbtx)
}

func (m *markedRoTx) Debug() kv.ForkableRoTxCommons {
	return m
}

func (m *markedRoTx) RoDbDebug() kv.MarkedDbRoTx {
	return m
}

func (m *markedRoTx) GetDb(num Num, hash []byte) ([]byte, error) {
	return m.s.DebugDb().GetDb(num, hash, m.dbtx)
}

func (m *markedRoTx) HasRootNumUpto(ctx context.Context, to RootNum) (bool, error) {
	return m.s.DebugDb().HasRootNumUpto(ctx, to, m.dbtx)
}

func (m *markedRoTx) Type() kv.CanonicityStrategy {
	return m.s.Type()
}

func (m *markedRoTx) Close() {
	m.s.Close()
}

func (b *baseRoTx) GetFromFiles(entityNum Num) (v []byte, found bool, fileIdx int, err error) {
	return b.f.GetFromFiles(entityNum)
}

func (b *baseRoTx) VisibleFilesMaxRootNum() RootNum {
	return b.f.VisibleFilesMaxRootNum()
}

func (b *baseRoTx) VisibleFilesMaxNum() Num {
	return b.f.VisibleFilesMaxNum()
}

func (b *baseRoTx) VisibleFiles() state.VisibleFiles {
	return b.f.VisibleFiles()
}

func (b *baseRoTx) GetFromFile(entityNum Num, idx int) (v []byte, found bool, err error) {
	return b.f.GetFromFile(entityNum, idx)
}
