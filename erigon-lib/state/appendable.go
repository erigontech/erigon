package state

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	ae "github.com/erigontech/erigon-lib/state/appendable_extras"
)

const MaxUint64 = ^uint64(0)

type RootRelationI interface {
	RootNum2Num(from RootNum, tx kv.Tx) (Num, error)
}

type BufferFactory interface {
	New() etl.Buffer
}

var _ StartRoTx[AppendableDbLessTxI] = (*Appendable[AppendableDbLessTxI])(nil)
var ErrNotFoundInSnapshot = errors.New("entity not found in snapshot")

type Appendable[T AppendableDbLessTxI] struct {
	*ProtoAppendable

	canonicalTbl string // for marked structures
	valsTbl      string

	ts4Bytes   bool               // caplin entities are encoded as 4 bytes
	pruneFrom  Num                // should this be rootnum? Num is fine for now.
	beginTxGen func(files bool) T // returns a tx, with "files ro tx" or not

	rel RootRelationI
}

type AppOpts func(AppendableConfig)

func App_WithFreezer(freezer Freezer) AppOpts {
	return func(a AppendableConfig) {
		a.SetFreezer(freezer)
	}
}

func App_WithIndexBuilders(builders ...AccessorIndexBuilder) AppOpts {
	return func(a AppendableConfig) {
		a.SetIndexBuilders(builders...)
	}
}

func App_WithTs4Bytes(ts4Bytes bool) AppOpts {
	return func(a AppendableConfig) {
		a.SetTs4Bytes(ts4Bytes)
	}
}

func App_WithPruneFrom(pruneFrom Num) AppOpts {
	return func(a AppendableConfig) {
		a.SetPruneFrom(pruneFrom)
	}
}

// func App
func NewMarkedAppendable(id AppendableId, valsTbl string, canonicalTbl string, relation RootRelationI, logger log.Logger, options ...AppOpts) (*Appendable[MarkedTxI], error) {
	a, err := create[MarkedTxI](id, Marked, valsTbl, canonicalTbl, relation, logger, options...)
	if err != nil {
		return nil, err
	}

	a.beginTxGen = func(files bool) MarkedTxI {
		m := &MarkedTx{ap: a}
		if files {
			m.ProtoAppendableTx = a.ProtoAppendable.BeginFilesRo()
		} else {
			m.ProtoAppendableTx = a.ProtoAppendable.BeginNoFilesRo()
		}
		return m
	}

	return a, nil
}

func NewUnmarkedAppendable(id AppendableId, valsTbl string, relation RootRelationI, logger log.Logger, options ...AppOpts) (*Appendable[UnmarkedTxI], error) {
	a, err := create[UnmarkedTxI](id, Unmarked, valsTbl, "", relation, logger, options...)
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
			m.ProtoAppendableTx = a.ProtoAppendable.BeginFilesRo()
		} else {
			m.ProtoAppendableTx = a.ProtoAppendable.BeginNoFilesRo()
		}
		return m
	}

	return a, nil
}

func NewBufferedAppendable(id AppendableId, valsTbl string, relation RootRelationI, factory BufferFactory, logger log.Logger, options ...AppOpts) (*Appendable[BufferedTxI], error) {
	a, err := create[BufferedTxI](id, Buffered, valsTbl, "", relation, logger, options...)
	if err != nil {
		return nil, err
	}

	if factory == nil {
		panic("no factory")
	}

	a.beginTxGen = func(files bool) BufferedTxI {
		m := &BufferedTx{ap: a}
		if files {
			m.ProtoAppendableTx = a.ProtoAppendable.BeginFilesRo()
		} else {
			m.ProtoAppendableTx = a.ProtoAppendable.BeginNoFilesRo()
		}
		return m
	}

	// TODO: default builders and index builders
	return a, nil
}

func create[T AppendableDbLessTxI](id AppendableId, strategy CanonicityStrategy, valsTbl string, canonicalTbl string, relation RootRelationI, logger log.Logger, options ...AppOpts) (*Appendable[T], error) {
	a := &Appendable[T]{
		ProtoAppendable: NewProto(id, nil, nil, logger),
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

func (a *Appendable[T]) PruneFrom() Num {
	return a.pruneFrom
}

func (a *Appendable[T]) encTs(ts ae.EncToBytesI) []byte {
	return ts.EncToBytes(!a.ts4Bytes)
}

func (a *Appendable[T]) BeginFilesTx() T {
	return a.beginTxGen(true)
}

func (a *Appendable[T]) BeginDbTx() T {
	return a.beginTxGen(false)
}

func (a *Appendable[T]) SetFreezer(freezer Freezer) {
	a.freezer = freezer
}

func (a *Appendable[T]) SetIndexBuilders(builders ...AccessorIndexBuilder) {
	a.builders = builders
}

func (a *Appendable[T]) SetPruneFrom(pruneFrom Num) {
	a.pruneFrom = pruneFrom
}

func (a *Appendable[T]) SetTs4Bytes(ts4Bytes bool) {
	a.ts4Bytes = ts4Bytes
}

// marked tx
type MarkedTx struct {
	*ProtoAppendableTx
	ap *Appendable[MarkedTxI]
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
	return tx.GetOne(a.valsTbl, m.combK(num, hash))
}

func (m *MarkedTx) Put(num Num, hash []byte, val Bytes, tx kv.RwTx) error {
	// can then val
	a := m.ap
	if err := tx.Append(a.canonicalTbl, a.encTs(num), hash); err != nil {
		return err
	}

	key := m.combK(num, hash)
	return tx.Put(a.valsTbl, key, val)
}

func (m *MarkedTx) Unwind(ctx context.Context, from RootNum, tx kv.RwTx) error {
	a := m.ap
	efrom, err := a.rel.RootNum2Num(from, tx) // for marked, id==num
	if err != nil {
		return err
	}
	fromKey := a.encTs(efrom)
	_, err = ae.DeleteRangeFromTbl(a.canonicalTbl, fromKey, nil, MaxUint64, tx)
	return err
}

func (m *MarkedTx) Prune(ctx context.Context, to RootNum, limit uint64, tx kv.RwTx) (pruneCount uint64, err error) {
	a := m.ap
	fromKeyPrefix := a.encTs(a.pruneFrom)
	eto, err := a.rel.RootNum2Num(to, tx)
	if err != nil {
		return 0, err
	}
	toKeyPrefix := a.encTs(eto)
	if del, err := ae.DeleteRangeFromTbl(a.canonicalTbl, fromKeyPrefix, toKeyPrefix, limit, tx); err != nil {
		return del, err
	}

	return ae.DeleteRangeFromTbl(a.valsTbl, fromKeyPrefix, toKeyPrefix, limit, tx)
}

func (m *MarkedTx) combK(ts Num, hash []byte) []byte {
	// relevant only for marked appendable
	// assuming hash is common.Hash which is 32 bytes
	const HashBytes = 32
	k := make([]byte, 8+HashBytes)
	binary.BigEndian.PutUint64(k, uint64(ts))
	copy(k[8:], hash)
	return k
}

// unmarked tx
type UnmarkedTx struct {
	*ProtoAppendableTx
	ap *Appendable[UnmarkedTxI]
}

func (m *UnmarkedTx) GetDb(num Num, tx kv.Tx) (Bytes, error) {
	return tx.GetOne(m.ap.valsTbl, m.ap.encTs(num))
}

func (m *UnmarkedTx) Append(entityNum Num, value Bytes, tx kv.RwTx) error {
	return tx.Append(m.ap.valsTbl, m.ap.encTs(entityNum), value)
}

func (m *UnmarkedTx) Unwind(ctx context.Context, from RootNum, tx kv.RwTx) error {
	ap := m.ap
	fromN, err := ap.rel.RootNum2Num(from, tx)
	if err != nil {
		return err
	}
	_, err = ae.DeleteRangeFromTbl(ap.valsTbl, ap.encTs(fromN), nil, 0, tx)
	return err
}

func (m *UnmarkedTx) Prune(ctx context.Context, to RootNum, limit uint64, tx kv.RwTx) (pruneCount uint64, err error) {
	ap := m.ap
	toNum, err := ap.rel.RootNum2Num(to, tx)
	if err != nil {
		return 0, err
	}
	log.Info("pruning", "appendable", ap.a.Name(), "from", ap.pruneFrom, "to", toNum)

	eFrom := ap.encTs(ap.pruneFrom)
	eTo := ap.encTs(toNum)
	return ae.DeleteRangeFromTbl(ap.valsTbl, eFrom, eTo, limit, tx)
}

type BufferedTx struct {
	*ProtoAppendableTx
	ap      *Appendable[BufferedTxI]
	values  *etl.Collector
	factory BufferFactory
}

// Get doesn't reflect the values currently in Buffer
func (m *BufferedTx) GetDb(entityNum Num, tx kv.Tx) (data Bytes, err error) {
	return tx.GetOne(m.ap.valsTbl, m.ap.encTs(entityNum))
}

func (m *BufferedTx) Put(entityNum Num, value Bytes) error {
	if m.values == nil {
		m.values = etl.NewCollector(m.id.Name()+".appendable.flush",
			m.id.Dirs().Tmp, m.factory.New(), m.a.logger).LogLvl(log.LvlTrace)
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

func (m *BufferedTx) Prune(ctx context.Context, to RootNum, limit uint64, tx kv.RwTx) (pruneCount uint64, err error) {
	ap := m.ap
	toNum, err := ap.rel.RootNum2Num(to, tx)
	if err != nil {
		return 0, err
	}
	log.Info("pruning", "appendable", ap.a.Name(), "from", ap.pruneFrom, "to", toNum)

	eFrom := ap.encTs(ap.pruneFrom)
	eTo := ap.encTs(toNum)
	return ae.DeleteRangeFromTbl(ap.valsTbl, eFrom, eTo, limit, tx)
}

func (m *BufferedTx) Unwind(ctx context.Context, from RootNum, tx kv.RwTx) error {
	// no op
	return nil
}

func (m *BufferedTx) Close() {
	if m.values != nil {
		m.values.Close()
	}

	m.ProtoAppendableTx.Close()
}

var (
	_ MarkedTxI   = (*MarkedTx)(nil)
	_ UnmarkedTxI = (*UnmarkedTx)(nil)
	//_ AppendingTxI = (*AppendingTx)(nil)
	_ BufferedTxI = (*BufferedTx)(nil)
)

// type AppendingTx struct {
// 	*ProtoAppendableTx
// 	ap *Appendable[AppendingTxI]
// }

// func NewAppendingAppendable(id AppendableId, valsTbl string, relation RootRelationI, logger log.Logger, options ...AppOpts[AppendingTxI]) (*Appendable[AppendingTxI], error) {
// 	a, err := create(id, Appending, valsTbl, "", relation, logger, options...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	a.beginFilesRoGen = func() AppendingTxI {
// 		return &AppendingTx{
// 			ProtoAppendableTx: a.ProtoAppendable.BeginFilesRo(),
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
// 	log.Info("pruning", "appendable", ap.a.Name(), "from", ap.pruneFrom, "to", toId)

// 	eFrom := ap.encTs(ap.pruneFrom)
// 	eTo := ap.encTs(toId)
// 	return ae.DeleteRangeFromTbl(ap.valsTbl, eFrom, eTo, limit, tx)
// }
