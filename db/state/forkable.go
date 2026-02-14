package state

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
)

const MaxUint64 = ^uint64(0)

type BufferFactory interface {
	New() etl.Buffer
}

var ErrNotFoundInSnapshot = errors.New("entity not found in snapshot")

type Forkable[T ForkableBaseTxI] struct {
	*ProtoForkable

	canonicalTbl string // for marked structures

	// whether this forkable is responsible for updating the canonical table (multiple forkables can share the same canonical table)
	// only one forkable should be responsible for updating the canonical table.
	updateCanonical bool

	valsTbl string

	//	ts4Bytes   bool               // caplin entities are encoded as 4 bytes
	pruneFrom  Num      // should this be rootnum? Num is fine for now.
	beginTxGen func() T // returns a tx over files

	rel RootRelationI
}

type AppOpts func(ForkableConfig)

func App_WithFreezer(freezer Freezer) AppOpts {
	return func(a ForkableConfig) {
		a.SetFreezer(freezer)
	}
}

func App_WithIndexBuilders(builders ...AccessorIndexBuilder) AppOpts {
	return func(a ForkableConfig) {
		a.SetIndexBuilders(builders...)
	}
}

func App_WithPruneFrom(pruneFrom Num) AppOpts {
	return func(a ForkableConfig) {
		a.SetPruneFrom(pruneFrom)
	}
}

func App_WithUpdateCanonical() AppOpts {
	return func(a ForkableConfig) {
		a.UpdateCanonicalTbl()
	}
}

// func App
func NewMarkedForkable(id kv.ForkableId, schema *statecfg.ForkableCfg, canonicalTbl string, relation RootRelationI, dirs datadir.Dirs, logger log.Logger, options ...AppOpts) (*Forkable[MarkedTxI], error) {
	a, err := create[MarkedTxI](id, schema, canonicalTbl, relation, dirs, logger, options...)
	if err != nil {
		return nil, err
	}

	a.beginTxGen = func() MarkedTxI {
		return &MarkedTx{ap: a, ProtoForkableTx: a.ProtoForkable.BeginFilesRo()}
	}

	return a, nil
}

func NewUnmarkedForkable(id kv.ForkableId, schema *statecfg.ForkableCfg, relation RootRelationI, dirs datadir.Dirs, logger log.Logger, options ...AppOpts) (*Forkable[UnmarkedTxI], error) {
	a, err := create[UnmarkedTxI](id, schema, "", relation, dirs, logger, options...)
	if err != nil {
		return nil, err
	}

	// un-marked structure have default freezer and builders
	if a.freezer == nil {
		freezer := &SimpleRelationalFreezer{rel: relation, valsTbl: schema.ValsTbl}
		a.freezer = freezer
	}

	if a.builders == nil {
		// mapping num -> offset
		args := NewAccessorArgs(false, false, schema.ValuesOnCompressedPage, a.StepSize())
		builder := NewSimpleAccessorBuilder(args, id, dirs.Tmp, logger)
		a.builders = []AccessorIndexBuilder{builder}
	}

	a.beginTxGen = func() UnmarkedTxI {
		return &UnmarkedTx{ap: a, ProtoForkableTx: a.ProtoForkable.BeginFilesRo()}
	}

	return a, nil
}

func create[T ForkableBaseTxI](id kv.ForkableId, schema *statecfg.ForkableCfg, canonicalTbl string, relation RootRelationI, dirs datadir.Dirs, logger log.Logger, options ...AppOpts) (*Forkable[T], error) {
	a := &Forkable[T]{
		ProtoForkable: NewProto(id, nil, nil, dirs, logger),
	}
	a.rel = relation
	a.valsTbl = schema.ValsTbl
	a.cfg = schema
	a.canonicalTbl = canonicalTbl
	for _, opt := range options {
		opt(a)
	}
	return a, nil
}

func (a *Forkable[T]) PruneFrom() Num {
	return a.pruneFrom
}

func (a *Forkable[T]) encTs(ts kv.EncToBytesI) []byte {
	return ts.EncToBytes(true)
}

func (a *Forkable[MarkedTxI]) valsTblKey(ts Num, hash []byte) []byte {
	// key for valsTbl
	// relevant only for marked forkable
	// assuming hash is common.Hash which is 32 bytes
	const HashBytes = 32
	k := make([]byte, 8+HashBytes)
	binary.BigEndian.PutUint64(k, uint64(ts))
	copy(k[8:], hash)
	return k
}

func (a *Forkable[MarkedTxI]) valsTblKey2(ts []byte, hash []byte) []byte {
	// key for valsTbl
	// relevant only for marked forkable
	// assuming hash is common.Hash which is 32 bytes
	const HashBytes = 32
	k := make([]byte, 8+HashBytes)
	copy(k, ts)
	copy(k[8:], hash)
	return k
}

func (a *Forkable[T]) BeginTemporalTx() T {
	return a.beginTxGen()
}

func (a *Forkable[T]) SetFreezer(freezer Freezer) {
	a.freezer = freezer
}

func (a *Forkable[T]) SetIndexBuilders(builders ...AccessorIndexBuilder) {
	a.builders = builders
}

func (a *Forkable[T]) SetPruneFrom(pruneFrom Num) {
	a.pruneFrom = pruneFrom
}

func (a *Forkable[T]) UpdateCanonicalTbl() {
	a.updateCanonical = true
}

// align the snapshots of this forkable entity
// with others members of entity set
func (a *Forkable[T]) Aligned(aligned bool) {
	a.unaligned = !aligned
}

func (a *Forkable[T]) Repo() *SnapshotRepo {
	return a.snaps
}

// marked tx
type MarkedTx struct {
	*ProtoForkableTx
	ap *Forkable[MarkedTxI]
}

func (m *MarkedTx) Get(num Num, tx kv.Tx) (Bytes, error) {
	v, found, _, err := m.GetFromFiles(num)
	if err != nil {
		return nil, err
	}

	if found {
		return v, nil
	} else if num < m.VisibleFilesMaxNum() {
		// expected in file, but not added to file (gap)
		return nil, nil
	}

	return m.GetDb(num, nil, tx)
}

func (m *MarkedTx) GetDb(num Num, hash []byte, tx kv.Tx) (Bytes, error) {
	a := m.ap
	if hash == nil {
		// find canonical hash
		canHash, err := tx.GetOne(a.canonicalTbl, a.encTs(num))
		if err != nil {
			return nil, err
		}
		hash = canHash
	}
	return tx.GetOne(a.valsTbl, m.ap.valsTblKey(num, hash))
}

func (m *MarkedTx) Put(num Num, hash []byte, val Bytes, tx kv.RwTx) error {
	// can then val
	a := m.ap
	if m.ap.updateCanonical {
		if err := tx.Append(a.canonicalTbl, a.encTs(num), hash); err != nil {
			return err
		}
	}

	key := m.ap.valsTblKey(num, hash)
	return tx.Put(a.valsTbl, key, val)
}

func (m *MarkedTx) Unwind(ctx context.Context, from RootNum, tx kv.RwTx) (fs ForkablePruneStat, err error) {
	a := m.ap
	efrom, err := a.rel.RootNum2Num(from, tx) // for marked, id==num
	if err != nil {
		return
	}
	fromKey := a.encTs(efrom)
	delCnt, err := DeleteRangeFromTbl(ctx, a.canonicalTbl, fromKey, nil, MaxUint64, nil, a.logger, tx)
	fs.Set(efrom, Num(MaxUint64), delCnt)
	return
}

func (m *MarkedTx) Prune(ctx context.Context, to RootNum, limit uint64, logEvery *time.Ticker, tx kv.RwTx) (fs ForkablePruneStat, err error) {
	pruneTo := min(m.VisibleFilesMaxRootNum(), to)
	a := m.ap
	fromKeyPrefix := a.encTs(a.pruneFrom)
	eto, err := a.rel.RootNum2Num(pruneTo, tx)
	if err != nil {
		return fs, err
	}
	toKeyPrefix := a.encTs(eto)
	del, err := DeleteRangeFromTbl(ctx, a.canonicalTbl, fromKeyPrefix, toKeyPrefix, limit, logEvery, a.logger, tx)
	fs.Set(a.pruneFrom, eto, del)
	if err != nil {
		return
	}
	_, err = DeleteRangeFromTbl(ctx, a.valsTbl, fromKeyPrefix, toKeyPrefix, limit, logEvery, a.logger, tx)
	return
}

func (m *MarkedTx) HasRootNumUpto(ctx context.Context, to RootNum, tx kv.Tx) (bool, error) {
	a := m.ap
	lastNum, _ := kv.LastKey(tx, a.canonicalTbl)
	if len(lastNum) == 0 {
		return false, nil
	}
	iLastNum := binary.BigEndian.Uint64(lastNum)
	eto, err := a.rel.RootNum2Num(to, tx)
	if err != nil {
		return false, fmt.Errorf("err RootNum2Num %v %w", to, err)
	}

	return iLastNum+1 >= eto.Uint64(), nil
}

func (m *MarkedTx) Progress(tx kv.Tx) (Num, error) {
	return progress(m.ap.canonicalTbl, tx, m)
}

func (m *MarkedTx) DebugFiles() ForkableFilesTxI {
	return m
}

func (m *MarkedTx) DebugDb() MarkedDbTxI {
	return m
}

// unmarked tx
type UnmarkedTx struct {
	*ProtoForkableTx
	ap *Forkable[UnmarkedTxI]
}

func (m *UnmarkedTx) Get(num Num, tx kv.Tx) (Bytes, error) {
	v, found, _, err := m.GetFromFiles(num)
	if err != nil {
		return nil, err
	}

	if found {
		return v, nil
	}

	return m.GetDb(num, tx)
}

func (m *UnmarkedTx) GetDb(num Num, tx kv.Tx) (Bytes, error) {
	return tx.GetOne(m.ap.valsTbl, m.ap.encTs(num))
}

func (m *UnmarkedTx) Append(entityNum Num, value Bytes, tx kv.RwTx) error {
	return tx.Append(m.ap.valsTbl, m.ap.encTs(entityNum), value)
}

func (m *UnmarkedTx) Unwind(ctx context.Context, from RootNum, tx kv.RwTx) (fs ForkablePruneStat, err error) {
	ap := m.ap
	fromN, err := ap.rel.RootNum2Num(from, tx)
	if err != nil {
		return fs, err
	}
	delCnt, err := DeleteRangeFromTbl(ctx, ap.valsTbl, ap.encTs(fromN), nil, 0, nil, ap.logger, tx)
	fs.Set(fromN, Num(MaxUint64), delCnt)
	return
}

func (m *UnmarkedTx) Prune(ctx context.Context, to RootNum, limit uint64, logEvery *time.Ticker, tx kv.RwTx) (fs ForkablePruneStat, err error) {
	ap := m.ap
	pruneTo := min(m.VisibleFilesMaxRootNum(), to)
	toNum, err := ap.rel.RootNum2Num(pruneTo, tx)
	if err != nil {
		return fs, err
	}

	eFrom := ap.encTs(ap.pruneFrom)
	eTo := ap.encTs(toNum)
	delCnt, err := DeleteRangeFromTbl(ctx, ap.valsTbl, eFrom, eTo, limit, logEvery, ap.logger, tx)
	fs.Set(ap.pruneFrom, toNum, delCnt)
	return
}

func (m *UnmarkedTx) HasRootNumUpto(ctx context.Context, to RootNum, tx kv.Tx) (bool, error) {
	a := m.ap
	lastNum, _ := kv.LastKey(tx, a.valsTbl)
	if len(lastNum) == 0 {
		return false, nil
	}
	iLastNum := binary.BigEndian.Uint64(lastNum)
	eto, err := a.rel.RootNum2Num(to, tx)
	if err != nil {
		return false, fmt.Errorf("err RootNum2Num %v %w", to, err)
	}

	return iLastNum >= eto.Uint64(), nil
}

func (m *UnmarkedTx) Progress(tx kv.Tx) (Num, error) {
	return progress(m.ap.valsTbl, tx, m)
}

func (m *UnmarkedTx) DebugFiles() ForkableFilesTxI {
	return m
}

func (m *UnmarkedTx) DebugDb() UnmarkedDbTxI {
	return m
}

func (m *UnmarkedTx) BufferedWriter() *UnmarkedBufferedWriter {
	return &UnmarkedBufferedWriter{
		values:  etl.NewCollector(Registry.Name(m.id)+".etl.flush", m.ap.dirs.Tmp, etl.SmallSortableBuffers.Get(), m.a.logger).LogLvl(log.LvlTrace),
		valsTbl: m.ap.valsTbl,
	}
}

type UnmarkedBufferedWriter struct {
	values  *etl.Collector
	valsTbl string
	f       *Forkable[UnmarkedTxI]
}

func (w *UnmarkedBufferedWriter) Put(n Num, v Bytes) error {
	key := w.f.encTs(n)
	return w.values.Collect(key, v)
}

func (w *UnmarkedBufferedWriter) Flush(ctx context.Context, tx kv.RwTx) error {
	if w.values == nil {
		return nil
	}
	return w.values.Load(tx, w.valsTbl, etl.IdentityLoadFunc, etl.TransformArgs{Quit: ctx.Done()})
}

func (w *UnmarkedBufferedWriter) Close() {
	if w == nil {
		return
	}
	if w.values != nil {
		w.values.Close()
		w.values = nil
	}
}

////////////////////////

func progress(tbl string, tx kv.Tx, f ForkableFilesTxI) (Num, error) {
	c, err := tx.Cursor(tbl)
	if err != nil {
		return 0, err
	}

	defer c.Close()
	k, _, err := c.Last()
	if err != nil {
		return 0, err
	}

	var num uint64
	if k != nil {
		num = binary.BigEndian.Uint64(k)
	}
	filesProgress := f.VisibleFilesMaxNum()
	if filesProgress > 0 {
		filesProgress--
	}
	return max(Num(num), filesProgress), nil
}

var (
	_ MarkedTxI   = (*MarkedTx)(nil)
	_ UnmarkedTxI = (*UnmarkedTx)(nil)
	//_ AppendingTxI = (*AppendingTx)(nil)
)

type ForkablePruneStat struct {
	MinNum, MaxNum Num
	PruneCount     uint64
}

var EmptyForkablePruneStat = ForkablePruneStat{MinNum: Num(uint64(math.MaxUint64)), MaxNum: Num(0)}

func (ps *ForkablePruneStat) Set(minNum, maxNum Num, pruneCount uint64) {
	ps.MinNum = minNum
	ps.MaxNum = maxNum
	ps.PruneCount = pruneCount
}

func (ps *ForkablePruneStat) PrunedNothing() bool {
	return ps.PruneCount == 0
}

func (ps *ForkablePruneStat) Accumulate(other *ForkablePruneStat) {
	if other == nil {
		return
	}
	ps.PruneCount += other.PruneCount
	ps.MinNum = min(ps.MinNum, other.MinNum)
	ps.MaxNum = max(ps.MaxNum, other.MaxNum)
}
