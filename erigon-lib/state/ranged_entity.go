package state

import (
	"context"
	"fmt"
	"sort"

	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	ae "github.com/erigontech/erigon-lib/state/entity_extras"
)

type RootRelationI interface {
	RootNum2Num(from RootNum, tx kv.Tx) (Num, error)
}

// this have 1:many or many:1 or 1:1 relation with root num
type RangedEntity struct {
	*ProtoEntity

	rel       RootRelationI
	valsTbl   string
	pruneFrom Num
}

type RAOpts func(a *RangedEntity)

// making this a method on RelationalAppendableOption to allow namespacing; since MarkedAppendableOptions
// have same functions.
func RA_WithFreezer(freezer Freezer) RAOpts {
	return func(a *RangedEntity) {
		a.freezer = freezer
	}
}

func RA_WithIndexBuilders(builders ...AccessorIndexBuilder) RAOpts {
	return func(a *RangedEntity) {
		a.builders = builders
	}
}

func NewRangedEntity(id EntityId, relation RootRelationI, valsTbl string, logger log.Logger, options ...RAOpts) (*RangedEntity, error) {
	a := &RangedEntity{
		ProtoEntity: NewProto(id, nil, nil, logger),
		rel:         relation,
	}
	a.sameKeyAsRoot = false

	for _, opt := range options {
		opt(a)
	}

	if a.freezer == nil {
		// default freezer
		freezer := &SimpleRelationalFreezer{rel: relation, valsTbl: valsTbl}
		a.freezer = freezer
	}

	if a.builders == nil {
		// mapping num -> offset (ordinal map)
		builder := NewSimpleAccessorBuilder(NewAccessorArgs(true, false), id)
		a.builders = []AccessorIndexBuilder{builder}
	}

	return a, nil
}

func (a *RangedEntity) encTs(ts uint64) []byte {
	return ae.EncToBytes(ts, true)
}

type RangedEntityTx struct {
	*ProtoEntityTx
	a  *RangedEntity
	id EntityId
}

func (a *RangedEntity) BeginFilesRo() RangedTxI {
	return &RangedEntityTx{
		ProtoEntityTx: a.ProtoEntity.BeginFilesRo(),
		a:             a,
		id:            a.a,
	}
}

func (a *RangedEntityTx) Get(entityNum Num, tx kv.Tx) (Bytes, error) {
	ap := a.a
	lastNum := ap.VisibleFilesMaxNum()
	var word []byte
	if entityNum <= lastNum {
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
	return tx.GetOne(ap.valsTbl, entityNum.EncTo8Bytes())
}

func (a *RangedEntityTx) Append(entityNum Num, value Bytes, tx kv.RwTx) error {
	return tx.Append(a.a.valsTbl, a.a.encTs(uint64(entityNum)), value)
}

// IncrementSequence some entity ids are not generated or provided by consensus (eg. txns)
// These are generated as db ids. In such case, when we get a new entity, we need to find which
// entity id is next (to use with the Append operation). This is the purpose of this function.
// this returns the "base id" from which `amount` is reserved for the collections of txns
// func (a *RelationalAppendableTx) IncrementSequence(amount uint64, tx kv.RwTx) (uint64, error) {
// 	baseId, err := tx.IncrementSequence(a.a.valsTbl, amount)
// 	if err != nil {
// 		return 0, err
// 	}

// 	return baseId, nil
// }

// func (a *RelationalAppendableTx) ReadSequence(tx kv.Tx) (uint64, error) {
// 	return tx.ReadSequence(a.a.valsTbl)
// }

// func (a *RelationalAppendableTx) ResetSequence(tx kv.RwTx, newValue Num) error {
// 	return tx.ResetSequence(a.a.valsTbl, uint64(newValue))
// }

func (a *RangedEntityTx) Prune(ctx context.Context, to RootNum, limit uint64, tx kv.RwTx) error {
	ap := a.a
	fromId := uint64(ap.pruneFrom)
	toId, err := ap.rel.RootNum2Num(to, tx)
	if err != nil {
		return err
	}
	log.Info("pruning", "appendable", ap.a.Name(), "from", fromId, "to", toId)

	eFrom := ap.encTs(uint64(fromId))
	eTo := ap.encTs(uint64(toId) - 1)
	return ae.DeleteRangeFromTbl(ap.valsTbl, eFrom, eTo, limit, tx)
}

func (a *RangedEntityTx) Unwind(ctx context.Context, from RootNum, rwTx kv.RwTx) error {
	ap := a.a
	fromId, err := ap.rel.RootNum2Num(from, rwTx)
	if err != nil {
		return err
	}
	return ae.DeleteRangeFromTbl(ap.valsTbl, ap.encTs(uint64(fromId)), nil, 0, rwTx)
}

func (a *RangedEntityTx) NewWriter() *RangedEntityWriter {
	// TODO: caplin uses some pool for sortable buffer
	// probably can have a global pool here for this...
	return &RangedEntityWriter{
		values: etl.NewCollector(a.id.Name()+".rappendable.flush",
			a.id.Dirs().Tmp, etl.NewSortableBuffer(WALCollectorRAM), a.a.logger).LogLvl(log.LvlTrace),
		valsTable: a.a.valsTbl,
		encFn:     a.a.encTs,
	}
}

func (a *RangedEntityTx) Close() {
	if a.files == nil {
		return
	}

	a.ProtoEntityTx.Close()
}

// RangedEntityWriter buffered write into etl collector, and then flush to db
// can't perform unwinds on this writer
type RangedEntityWriter struct {
	values    *etl.Collector
	valsTable string
	encFn     func(ts uint64) []byte
}

func NewRelationalAppendableWriter(r *RangedEntity, valsTable string, values *etl.Collector) *RangedEntityWriter {
	return &RangedEntityWriter{
		values:    values,
		valsTable: valsTable,
		encFn:     r.encTs,
	}
}

func (w *RangedEntityWriter) Close() {
	if w == nil {
		return
	}
	if w.values != nil {
		w.values.Close()
	}
}

func (w *RangedEntityWriter) Add(id Id, value Bytes) error {
	key := w.encFn(uint64(id))
	if err := w.values.Collect(key, value); err != nil {
		return err
	}

	return nil
}

func (w *RangedEntityWriter) Flush(ctx context.Context, tx kv.RwTx) error {
	if w.values == nil {
		return nil
	}
	// load uses Append since identityLoadFunc is used.
	// might want to configure other TransformArgs here?
	//
	return w.values.Load(tx, w.valsTable, etl.IdentityLoadFunc, etl.TransformArgs{Quit: ctx.Done()})
}
