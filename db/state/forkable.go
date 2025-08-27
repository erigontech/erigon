package state

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
)

const MaxUint64 = ^uint64(0)

type BufferFactory interface {
	New() etl.Buffer
}

var _ StartRoTx[ForkableBaseTxI] = (*Forkable[ForkableBaseTxI])(nil)
var ErrNotFoundInSnapshot = errors.New("entity not found in snapshot")

type Forkable[T ForkableBaseTxI] struct {
	*ProtoForkable

	canonicalTbl string // for marked structures

	// whether this forkable is responsible for updating the canonical table (multiple forkables can share the same canonical table)
	// only one forkable should be responsible for updating the canonical table.
	updateCanonical bool

	valsTbl string

	//	ts4Bytes   bool               // caplin entities are encoded as 4 bytes
	pruneFrom  Num                // should this be rootnum? Num is fine for now.
	beginTxGen func(files bool) T // returns a tx, with "files ro tx" or not

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
func NewMarkedForkable(id ForkableId, valsTbl string, canonicalTbl string, relation RootRelationI, logger log.Logger, options ...AppOpts) (*Forkable[MarkedTxI], error) {
	a, err := create[MarkedTxI](id, kv.Marked, valsTbl, canonicalTbl, relation, logger, options...)
	if err != nil {
		return nil, err
	}

	a.beginTxGen = func(files bool) MarkedTxI {
		m := &MarkedTx{ap: a}
		if files {
			m.ProtoForkableTx = a.ProtoForkable.BeginFilesRo()
		} else {
			m.ProtoForkableTx = a.ProtoForkable.BeginNoFilesRo()
		}
		return m
	}

	return a, nil
}

func NewUnmarkedForkable(id ForkableId, valsTbl string, relation RootRelationI, logger log.Logger, options ...AppOpts) (*Forkable[UnmarkedTxI], error) {
	a, err := create[UnmarkedTxI](id, kv.Unmarked, valsTbl, "", relation, logger, options...)
	if err != nil {
		return nil, err
	}

	// un-marked structure have default freezer and builders
	if a.freezer == nil {
		freezer := &SimpleRelationalFreezer{rel: relation, valsTbl: valsTbl}
		a.freezer = freezer
	}

	if a.builders == nil {
		// mapping num -> offset (ordinal map)
		builder := NewSimpleAccessorBuilder(NewAccessorArgs(true, false), id, logger)
		a.builders = []AccessorIndexBuilder{builder}
	}

	a.beginTxGen = func(files bool) UnmarkedTxI {
		m := &UnmarkedTx{ap: a}
		if files {
			m.ProtoForkableTx = a.ProtoForkable.BeginFilesRo()
		} else {
			m.ProtoForkableTx = a.ProtoForkable.BeginNoFilesRo()
		}
		return m
	}

	return a, nil
}

func NewBufferedForkable(id ForkableId, valsTbl string, relation RootRelationI, factory BufferFactory, logger log.Logger, options ...AppOpts) (*Forkable[BufferedTxI], error) {
	a, err := create[BufferedTxI](id, kv.Buffered, valsTbl, "", relation, logger, options...)
	if err != nil {
		return nil, err
	}

	if factory == nil {
		panic("no factory")
	}

	a.beginTxGen = func(files bool) BufferedTxI {
		m := &BufferedTx{ap: a}
		if files {
			m.ProtoForkableTx = a.ProtoForkable.BeginFilesRo()
		} else {
			m.ProtoForkableTx = a.ProtoForkable.BeginNoFilesRo()
		}
		return m
	}

	// TODO: default builders and index builders
	return a, nil
}

func create[T ForkableBaseTxI](id ForkableId, strategy kv.CanonicityStrategy, valsTbl string, canonicalTbl string, relation RootRelationI, logger log.Logger, options ...AppOpts) (*Forkable[T], error) {
	a := &Forkable[T]{
		ProtoForkable: NewProto(id, nil, nil, logger),
	}
	a.rel = relation
	a.valsTbl = valsTbl
	a.canonicalTbl = canonicalTbl
	for _, opt := range options {
		opt(a)
	}
	a.strategy = strategy
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
	return a.beginTxGen(true)
}

func (a *Forkable[T]) BeginNoFilesTx() T {
	return a.beginTxGen(false)
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

func (m *UnmarkedTx) DebugFiles() ForkableFilesTxI {
	return m
}

func (m *UnmarkedTx) DebugDb() UnmarkedDbTxI {
	return m
}

type BufferedTx struct {
	*ProtoForkableTx
	ap      *Forkable[BufferedTxI]
	values  *etl.Collector
	factory BufferFactory
}

// doesn't look into buffer, but just db + files
func (m *BufferedTx) Get(num Num, tx kv.Tx) (Bytes, error) {
	v, found, _, err := m.GetFromFiles(num)
	if err != nil {
		return nil, err
	}

	if found {
		return v, nil
	}

	return m.GetDb(num, tx)
}

// Get doesn't reflect the values currently in Buffer
func (m *BufferedTx) GetDb(entityNum Num, tx kv.Tx) (data Bytes, err error) {
	return tx.GetOne(m.ap.valsTbl, m.ap.encTs(entityNum))
}

func (m *BufferedTx) Put(entityNum Num, value Bytes) error {
	if m.values == nil {
		m.values = etl.NewCollector(Registry.Name(m.id)+".forkable.flush",
			Registry.Dirs(m.id).Tmp, m.factory.New(), m.a.logger).LogLvl(log.LvlTrace)
	}

	key := m.ap.encTs(entityNum)
	return m.values.Collect(key, value)
}

func (m *BufferedTx) Flush(ctx context.Context, tx kv.RwTx) error {
	if m.values == nil {
		return nil
	}
	// load uses Append since identityLoadFunc is used.
	// might want to configure other TransformArgs here?
	return m.values.Load(tx, m.ap.valsTbl, etl.IdentityLoadFunc, etl.TransformArgs{Quit: ctx.Done()})
}

func (m *BufferedTx) Prune(ctx context.Context, to RootNum, limit uint64, logEvery *time.Ticker, tx kv.RwTx) (fs ForkablePruneStat, err error) {
	ap := m.ap
	pruneTo := min(m.VisibleFilesMaxRootNum(), to)
	toNum, err := ap.rel.RootNum2Num(pruneTo, tx)
	if err != nil {
		return
	}

	eFrom := ap.encTs(ap.pruneFrom)
	eTo := ap.encTs(toNum)
	delCnt, err := DeleteRangeFromTbl(ctx, ap.valsTbl, eFrom, eTo, limit, logEvery, ap.logger, tx)
	fs.Set(ap.pruneFrom, toNum, delCnt)
	return
}

func (m *BufferedTx) Unwind(ctx context.Context, from RootNum, tx kv.RwTx) (fs ForkablePruneStat, err error) {
	// no op
	return
}

func (m *BufferedTx) HasRootNumUpto(ctx context.Context, to RootNum, tx kv.Tx) (bool, error) {
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

func (m *BufferedTx) Close() {
	if m.values != nil {
		m.values.Close()
	}

	m.ProtoForkableTx.Close()
}

func (m *BufferedTx) DebugFiles() ForkableFilesTxI {
	return m
}

func (m *BufferedTx) DebugDb() BufferedDbTxI {
	return m
}

var (
	_ MarkedTxI   = (*MarkedTx)(nil)
	_ UnmarkedTxI = (*UnmarkedTx)(nil)
	//_ AppendingTxI = (*AppendingTx)(nil)
	_ BufferedTxI = (*BufferedTx)(nil)
)

// type AppendingTx struct {
// 	*ProtoForkableTx
// 	ap *Forkable[AppendingTxI]
// }

// func NewAppendingForkable(id ForkableId, valsTbl string, relation RootRelationI, logger log.Logger, options ...AppOpts[AppendingTxI]) (*Forkable[AppendingTxI], error) {
// 	a, err := create(id, Appending, valsTbl, "", relation, logger, options...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	a.beginFilesRoGen = func() AppendingTxI {
// 		return &AppendingTx{
// 			ProtoForkableTx: a.ProtoForkable.BeginFilesRo(),
// 			ap:                a,
// 		}
// 	}
// 	return a, nil
// }

// // Get operates on snapshots only, it doesn't do resolution of
// // Num -> Id needed for finding canonical values in db.
// func (m *AppendingTx) Get(entityNum Num, tx kv.Tx) (value Bytes, foundInSnapshot bool, err error) {
// 	// snapshots only
// 	data, found, err := m.LookupFile(entityNum, tx)
// 	if err != nil {
// 		return nil, false, err
// 	}
// 	if !found {
// 		return nil, false, ErrNotFoundInSnapshot
// 	}
// 	return data, true, nil
// }

// func (m *AppendingTx) GetNc(entityId Id, tx kv.Tx) (Bytes, error) {
// 	return tx.GetOne(m.ap.valsTbl, m.ap.encTs(Num(entityId)))
// }

// func (m *AppendingTx) Append(entityId Id, value Bytes, tx kv.RwTx) error {
// 	return tx.Append(m.ap.valsTbl, m.ap.encTs(Num(entityId)), value)
// }

// func (m *AppendingTx) IncrementSequence(amount uint64, tx kv.RwTx) (baseId uint64, err error) {
// 	return tx.IncrementSequence(m.ap.valsTbl, amount)
// }

// func (m *AppendingTx) ReadSequence(tx kv.Tx) (uint64, error) {
// 	return tx.ReadSequence(m.ap.valsTbl)
// }

// func (m *AppendingTx) ResetSequence(value uint64, tx kv.RwTx) error {
// 	return tx.ResetSequence(m.ap.valsTbl, value)
// }

// func (m *AppendingTx) Unwind(ctx context.Context, from RootNum, tx kv.RwTx) error {
// 	ap := m.ap
// 	fromId, err := ap.rel.RootNum2Num(from, tx)
// 	if err != nil {
// 		return err
// 	}
// 	_, err = ae.DeleteRangeFromTbl(ap.valsTbl, ap.encTs(fromId), nil, 0, tx)
// 	return err
// }

// func (m *AppendingTx) Prune(ctx context.Context, to RootNum, limit uint64, tx kv.RwTx) (pruneCount uint64, err error) {
// 	ap := m.ap
// 	toId, err := ap.rel.RootNum2Num(to, tx)
// 	if err != nil {
// 		return 0, err
// 	}
// 	log.Info("pruning", "forkable", ap.a.Name(), "from", ap.pruneFrom, "to", toId)

// 	eFrom := ap.encTs(ap.pruneFrom)
// 	eTo := ap.encTs(toId)
// 	return ae.DeleteRangeFromTbl(ap.valsTbl, eFrom, eTo, limit, tx)
// }

type ForkablePruneStat struct {
	MinNum, MaxNum Num
	PruneCount     uint64
}

var EmptyForkablePruneStat = ForkablePruneStat{MinNum: Num(math.MaxUint64), MaxNum: Num(0)}

func (ps *ForkablePruneStat) Set(minNum, maxNum Num, pruneCount uint64) {
	ps.MinNum = minNum
	ps.MaxNum = maxNum
	ps.PruneCount = pruneCount
}

func (ps *ForkablePruneStat) PruneNothing() bool {
	return ps.PruneCount != 0
}

func (ps *ForkablePruneStat) String() string {
	if ps.PruneNothing() {
		return ""
	}

	return fmt.Sprintf("pruned %d entries from [%d-%d]", ps.PruneCount, ps.MinNum, ps.MaxNum)
}

func (ps *ForkablePruneStat) Accumulate(other *ForkablePruneStat) {
	if other == nil {
		return
	}
	ps.PruneCount += other.PruneCount
	ps.MinNum = min(ps.MinNum, other.MinNum)
	ps.MaxNum = max(ps.MaxNum, other.MaxNum)
}
