package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	ae "github.com/erigontech/erigon-lib/state/entity_extras"
)

const MaxUint64 = ^uint64(0)

type RootRelationI interface {
	RootNum2Num(from RootNum, tx kv.Tx) (Num, error)
}

type markedStructure struct {
	canonicalTbl string
}

type TxInfo interface{ Type() CanonicityStrategy }

type Appendable[T TxInfo] struct {
	*ProtoEntity

	ms      *markedStructure
	valsTbl string

	ts4Bytes        bool
	pruneFrom       Num // should this be rootnum? Num is fine for now.
	beginFilesRoGen func() T

	rel RootRelationI
}

type AppOpts[T TxInfo] func(a *Appendable[T])

func App_WithFreezer[T TxInfo](freezer Freezer) AppOpts[T] {
	return func(a *Appendable[T]) {
		a.freezer = freezer
	}
}

func App_WithIndexBuilders[T TxInfo](builders ...AccessorIndexBuilder) AppOpts[T] {
	return func(a *Appendable[T]) {
		a.builders = builders
	}
}

func App_WithTs4Bytes[T TxInfo](ts4Bytes bool) AppOpts[T] {
	return func(a *Appendable[T]) {
		a.ts4Bytes = ts4Bytes
	}
}

func App_WithPruneFrom[T TxInfo](pruneFrom Num) AppOpts[T] {
	return func(a *Appendable[T]) {
		a.pruneFrom = pruneFrom
	}
}

// func App
func NewMarkedAppendable(id EntityId, valsTbl string, canonicalTbl string, relation RootRelationI, logger log.Logger, options ...AppOpts[MarkedTxI]) (*Appendable[MarkedTxI], error) {
	a, err := create(id, Marked, valsTbl, canonicalTbl, relation, logger, options...)
	if err != nil {
		return nil, err
	}

	a.beginFilesRoGen = func() MarkedTxI {
		return &MarkedTx{
			ProtoEntityTx: a.ProtoEntity.BeginFilesRo(),
			ap:            (*Appendable[MarkedTxI])(a),
		}
	}

	return a, nil
}

func NewUnmarkedAppendable(id EntityId, valsTbl string, relation RootRelationI, logger log.Logger, options ...AppOpts[UnmarkedTxI]) (*Appendable[UnmarkedTxI], error) {
	a, err := create(id, Unmarked, valsTbl, "", relation, logger, options...)
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
		builder := NewSimpleAccessorBuilder(NewAccessorArgs(true, false), id)
		a.builders = []AccessorIndexBuilder{builder}
	}

	a.beginFilesRoGen = func() UnmarkedTxI {
		return &UnmarkedTx{
			ProtoEntityTx: a.ProtoEntity.BeginFilesRo(),
			ap:            (*Appendable[UnmarkedTxI])(a),
		}
	}

	return a, nil
}

func NewAppendingAppendable(id EntityId, valsTbl string, relation RootRelationI, logger log.Logger, options ...AppOpts[AppendingTxI]) (*Appendable[AppendingTxI], error) {
	a, err := create(id, Appending, valsTbl, "", relation, logger, options...)
	if err != nil {
		return nil, err
	}
	a.beginFilesRoGen = func() AppendingTxI {
		return &AppendingTx{
			ProtoEntityTx: a.ProtoEntity.BeginFilesRo(),
			ap:            (*Appendable[AppendingTxI])(a),
		}
	}
	return a, nil
}

func NewBufferedAppendable(id EntityId, valsTbl string, relation RootRelationI, factory BufferFactory, logger log.Logger, options ...AppOpts[BufferedTxI]) (*Appendable[BufferedTxI], error) {
	a, err := create(id, Buffered, valsTbl, "", relation, logger, options...)
	if err != nil {
		return nil, err
	}

	if factory == nil {
		panic("no factory")
	}

	a.beginFilesRoGen = func() BufferedTxI {
		return &BufferedTx{
			ProtoEntityTx: a.ProtoEntity.BeginFilesRo(),
			ap:            (*Appendable[BufferedTxI])(a),
			factory:       factory,
		}
	}

	// TODO: default builders and index builders
	return a, nil
}

func create[T EntityTxI](id EntityId, strategy CanonicityStrategy, valsTbl string, canonicalTbl string, relation RootRelationI, logger log.Logger, options ...AppOpts[T]) (*Appendable[T], error) {
	a := &Appendable[T]{
		ProtoEntity: NewProto(id, nil, nil, logger),
	}
	a.rel = relation
	a.valsTbl = valsTbl
	if canonicalTbl != "" {
		a.ms = &markedStructure{canonicalTbl: canonicalTbl}
	}

	for _, opt := range options {
		opt(a)
	}
	a.strategy = strategy
	return a, nil
}

func (a *Appendable[T]) encTs(ts Num) []byte {
	return ts.EncToBytes(!a.ts4Bytes)
}

func (a *Appendable[T]) combK(ts Num, hash []byte) []byte {
	// TODO: move this to marked_tx
	// relevant only for marked appendable
	// assuming hash is common.Hash which is 32 bytes
	const HashBytes = 32
	k := make([]byte, 8+HashBytes)
	binary.BigEndian.PutUint64(k, uint64(ts))
	copy(k[8:], hash)
	return k
}

func (a *Appendable[T]) BeginFilesRo() T {
	return a.beginFilesRoGen()
}

// marked tx

type MarkedTx struct {
	*ProtoEntityTx
	ap *Appendable[MarkedTxI]
}

var _ MarkedTxI = (*MarkedTx)(nil)

func (m *MarkedTx) Get(entityNum Num, tx kv.Tx) (Bytes, error) {
	ap := m.ap
	lastNum := ap.VisibleFilesMaxNum()
	if entityNum <= lastNum {
		var word []byte
		// snapshot
		index := sort.Search(len(ap._visible), func(i int) bool {
			return ap._visible[i].src.FirstEntityNum() >= uint64(entityNum)
		})

		if index == -1 {
			return nil, fmt.Errorf("entity get error: snapshot expected but now found: (%s, %d)", ap.a.Name(), entityNum)
		}
		visible := ap._visible[index]
		g := visible.getter
		offset := visible.reader.OrdinalLookup(uint64(entityNum))
		g.Reset(offset)
		if g.HasNext() {
			word, _ = g.Next(word[:0])
			return word, nil
		}

		return nil, fmt.Errorf("entity get error: %s expected %d in snapshot %s but not found", m.id.Name(), entityNum, visible.src.decompressor.FileName())
	}

	return m.getDb(entityNum, nil, tx)
}

func (m *MarkedTx) getDb(entityNum Num, hash []byte, tx kv.Tx) (Bytes, error) {
	a := m.ap
	if hash == nil {
		// find canonical hash
		canHash, err := tx.GetOne(a.ms.canonicalTbl, a.encTs(entityNum))
		if err != nil {
			return nil, err
		}
		hash = canHash
	}
	return tx.GetOne(a.valsTbl, a.combK(entityNum, hash))
}

func (m *MarkedTx) GetNc(num Num, hash []byte, tx kv.Tx) (Bytes, error) {
	return m.getDb(num, hash, tx)
}

func (m *MarkedTx) Put(num Num, hash []byte, val Bytes, tx kv.RwTx) error {
	// can then val
	a := m.ap
	if err := tx.Append(a.ms.canonicalTbl, a.encTs(num), hash); err != nil {
		return err
	}

	key := a.combK(num, hash)
	return tx.Put(a.valsTbl, key, val)
}

func (m *MarkedTx) Unwind(ctx context.Context, from RootNum, tx kv.RwTx) error {
	a := m.ap
	efrom, err := a.rel.RootNum2Num(from, tx)
	if err != nil {
		return err
	}
	fromKey := a.encTs(efrom)
	return ae.DeleteRangeFromTbl(a.ms.canonicalTbl, fromKey, nil, MaxUint64, tx)
}

func (m *MarkedTx) Prune(ctx context.Context, to RootNum, limit uint64, tx kv.RwTx) error {
	a := m.ap
	fromKeyPrefix := a.encTs(a.pruneFrom)
	eto, err := a.rel.RootNum2Num(to, tx)
	if err != nil {
		return err
	}
	toKeyPrefix := a.encTs(eto - 1)
	if err := ae.DeleteRangeFromTbl(a.ms.canonicalTbl, fromKeyPrefix, toKeyPrefix, limit, tx); err != nil {
		return err
	}

	if err := ae.DeleteRangeFromTbl(a.valsTbl, fromKeyPrefix, toKeyPrefix, limit, tx); err != nil {
		return err
	}

	return nil
}

/// unmarked tx

type UnmarkedTx struct {
	*ProtoEntityTx
	ap *Appendable[UnmarkedTxI]
}

var _ UnmarkedTxI = (*UnmarkedTx)(nil)

func (m *UnmarkedTx) Get(entityNum Num, tx kv.Tx) (Bytes, error) {
	ap := m.ap
	lastNum := ap.VisibleFilesMaxNum()
	if entityNum <= lastNum {
		var word []byte
		index := sort.Search(len(ap._visible), func(i int) bool {
			return ap._visible[i].src.FirstEntityNum() >= uint64(entityNum)
		})

		if index == -1 {
			return nil, fmt.Errorf("entity get error: snapshot expected but now found: (%s, %d)", ap.a.Name(), entityNum)
		}

		visible := ap._visible[index]
		g := visible.getter

		offset := visible.reader.OrdinalLookup(uint64(entityNum)) // TODO: allowed values
		g.Reset(offset)
		if g.HasNext() {
			word, _ = g.Next(word[:0])
			return word, nil
		}

		return nil, fmt.Errorf("entity get error: %s expected %d in snapshot %s but not found", ap.a.Name(), entityNum, visible.src.decompressor.FileName())
	}

	// else the db
	return tx.GetOne(ap.valsTbl, ap.encTs(entityNum))
}

func (m *UnmarkedTx) Append(entityNum Num, value Bytes, tx kv.RwTx) error {
	return tx.Append(m.ap.valsTbl, m.ap.encTs(entityNum), value)
}

func (m *UnmarkedTx) NewWriter() *ValueBufferedWriter {
	// TODO: caplin uses some pool for sortable buffer
	// probably can have a global pool here for this...
	return &ValueBufferedWriter{
		values: etl.NewCollector(m.id.Name()+".appendable.flush",
			m.id.Dirs().Tmp, etl.NewSortableBuffer(WALCollectorRAM), m.a.logger).LogLvl(log.LvlTrace),
		valsTable: m.ap.valsTbl,
		encFn:     m.ap.encTs,
	}
}

func (m *UnmarkedTx) Unwind(ctx context.Context, from RootNum, tx kv.RwTx) error {
	ap := m.ap
	fromId, err := ap.rel.RootNum2Num(from, tx)
	if err != nil {
		return err
	}
	return ae.DeleteRangeFromTbl(ap.valsTbl, ap.encTs(fromId), nil, 0, tx)
}

func (m *UnmarkedTx) Prune(ctx context.Context, to RootNum, limit uint64, tx kv.RwTx) error {
	ap := m.ap
	toId, err := ap.rel.RootNum2Num(to, tx)
	if err != nil {
		return err
	}
	log.Info("pruning", "appendable", ap.a.Name(), "from", ap.pruneFrom, "to", toId)

	eFrom := ap.encTs(ap.pruneFrom)
	eTo := ap.encTs(toId - 1)
	return ae.DeleteRangeFromTbl(ap.valsTbl, eFrom, eTo, limit, tx)
}

type AppendingTx struct {
	*ProtoEntityTx
	ap *Appendable[AppendingTxI]
}

var _ AppendingTxI = (*AppendingTx)(nil)

// Get operates on snapshots only, it doesn't do resolution of
// Num -> Id needed for finding canonical values in db.
func (m *AppendingTx) Get(entityNum Num, tx kv.Tx) (Bytes, error) {
	// snapshots only
	ap := m.ap
	lastNum := ap.VisibleFilesMaxNum()
	if entityNum > lastNum {
		return nil, nil
	}
	var word []byte
	index := sort.Search(len(ap._visible), func(i int) bool {
		return ap._visible[i].src.FirstEntityNum() >= uint64(entityNum)
	})

	if index == -1 {
		return nil, fmt.Errorf("entity get error: snapshot expected but now found: (%s, %d)", ap.a.Name(), entityNum)
	}

	visible := ap._visible[index]
	g := visible.getter

	offset := visible.reader.OrdinalLookup(uint64(entityNum)) // TODO: allowed values
	g.Reset(offset)
	if g.HasNext() {
		word, _ = g.Next(word[:0])
		return word, nil
	}
	return nil, fmt.Errorf("entity get error: %s expected %d in snapshot %s but not found", ap.a.Name(), entityNum, visible.src.decompressor.FileName())

}

func (m *AppendingTx) GetNc(entityId Id, tx kv.Tx) (Bytes, error) {
	return tx.GetOne(m.ap.valsTbl, m.ap.encTs(Num(entityId)))
}

func (m *AppendingTx) Append(entityId Id, value Bytes, tx kv.RwTx) error {
	return tx.Append(m.ap.valsTbl, m.ap.encTs(Num(entityId)), value)
}

func (m *AppendingTx) IncrementSequence(amount uint64, tx kv.RwTx) (uint64, error) {
	return tx.IncrementSequence(m.ap.valsTbl, amount)
}

func (m *AppendingTx) ReadSequence(tx kv.Tx) (uint64, error) {
	return tx.ReadSequence(m.ap.valsTbl)
}

func (m *AppendingTx) ResetSequence(value uint64, tx kv.RwTx) error {
	return tx.ResetSequence(m.ap.valsTbl, value)
}

func (m *AppendingTx) Unwind(ctx context.Context, from RootNum, tx kv.RwTx) error {
	ap := m.ap
	fromId, err := ap.rel.RootNum2Num(from, tx)
	if err != nil {
		return err
	}
	return ae.DeleteRangeFromTbl(ap.valsTbl, ap.encTs(fromId), nil, 0, tx)
}

func (m *AppendingTx) Prune(ctx context.Context, to RootNum, limit uint64, tx kv.RwTx) error {
	ap := m.ap
	toId, err := ap.rel.RootNum2Num(to, tx)
	if err != nil {
		return err
	}
	log.Info("pruning", "appendable", ap.a.Name(), "from", ap.pruneFrom, "to", toId)

	eFrom := ap.encTs(ap.pruneFrom)
	eTo := ap.encTs(toId - 1)
	return ae.DeleteRangeFromTbl(ap.valsTbl, eFrom, eTo, limit, tx)
}

type BufferedTx struct {
	*ProtoEntityTx
	ap      *Appendable[BufferedTxI]
	writer  *ValueBufferedWriter
	factory BufferFactory
}

var _ BufferedTxI = (*BufferedTx)(nil)

// type BufferedTxI interface {
// 	EntityTxI
// 	Put(Num, Bytes) error
// 	Flush(context.Context, kv.RwTx) error
// }

// Get doesn't reflect the values currently in Buffer
func (m *BufferedTx) Get(entityNum Num, tx kv.Tx) (Bytes, error) {
	return tx.GetOne(m.ap.valsTbl, m.ap.encTs(entityNum))
}

func (m *BufferedTx) Put(entityNum Num, value Bytes) error {
	if m.writer == nil {
		m.writer = &ValueBufferedWriter{
			values: etl.NewCollector(m.id.Name()+".appendable.flush",
				m.id.Dirs().Tmp, m.factory.New(), m.a.logger).LogLvl(log.LvlTrace),
			valsTable: m.ap.valsTbl,
			encFn:     m.ap.encTs,
		}
	}

	return m.writer.Add(entityNum, value)
}

func (m *BufferedTx) Flush(ctx context.Context, tx kv.RwTx) error {
	if m.writer == nil {
		return nil
	}
	return m.writer.Flush(ctx, tx)
}

func (m *BufferedTx) Prune(ctx context.Context, to RootNum, limit uint64, tx kv.RwTx) error {
	ap := m.ap
	toId, err := ap.rel.RootNum2Num(to, tx)
	if err != nil {
		return err
	}
	log.Info("pruning", "appendable", ap.a.Name(), "from", ap.pruneFrom, "to", toId)

	eFrom := ap.encTs(ap.pruneFrom)
	eTo := ap.encTs(toId - 1)
	return ae.DeleteRangeFromTbl(ap.valsTbl, eFrom, eTo, limit, tx)
}

func (m *BufferedTx) Unwind(ctx context.Context, from RootNum, tx kv.RwTx) error {
	// no op
	return nil
}

func (m *BufferedTx) Close() {
	if m.writer != nil {
		m.writer.Close()
	}

	m.ProtoEntityTx.Close()
}

//func ()

// ValueBufferedWriter buffered write into etl collector, and then flush to db
// can't perform unwinds on this writer
type ValueBufferedWriter struct {
	values    *etl.Collector
	valsTable string
	encFn     func(ts Num) []byte
}

func (w *ValueBufferedWriter) Close() {
	if w == nil {
		return
	}
	if w.values != nil {
		w.values.Close()
	}
}

func (w *ValueBufferedWriter) Add(num Num, value Bytes) error {
	key := w.encFn(num)
	if err := w.values.Collect(key, value); err != nil {
		return err
	}

	return nil
}

func (w *ValueBufferedWriter) Flush(ctx context.Context, tx kv.RwTx) error {
	if w.values == nil {
		return nil
	}
	// load uses Append since identityLoadFunc is used.
	// might want to configure other TransformArgs here?
	//
	return w.values.Load(tx, w.valsTable, etl.IdentityLoadFunc, etl.TransformArgs{Quit: ctx.Done()})
}

type BufferFactory interface {
	New() etl.Buffer
}
