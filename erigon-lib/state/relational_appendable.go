package state

import (
	"context"
	"fmt"
	"sort"

	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	ae "github.com/erigontech/erigon-lib/state/appendables_extras"
)

type RootRelationI interface {
	RootNum2Num(from RootNum, tx kv.Tx) (Num, error)
}

// this have 1:many or many:1 or 1:1 relation with root num
type RelationalAppendable struct {
	*ProtoAppendable

	rel       RootRelationI
	valsTbl   string
	pruneFrom Num
}

type RAOpts func(a *RelationalAppendable)

// making this a method on RelationalAppendableOption to allow namespacing; since MarkedAppendableOptions
// have same functions.
func (r *RAOpts) WithFreezer(freezer Freezer) RAOpts {
	return func(a *RelationalAppendable) {
		a.freezer = freezer
	}
}

func (r *RAOpts) WithIndexBuilders(builders ...AccessorIndexBuilder) RAOpts {
	return func(a *RelationalAppendable) {
		a.builders = builders
	}
}

func NewRelationalAppendable(id AppendableId, relation RootRelationI, valsTbl string, logger log.Logger, options ...RAOpts) (*RelationalAppendable, error) {
	a := &RelationalAppendable{
		ProtoAppendable: NewProto(id, nil, nil, logger),
		rel:             relation,
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
		salt, err := snaptype.GetIndexSalt(a.a.Dirs().Snap)
		if err != nil {
			return nil, err
		}
		builder := NewSimpleAccessorBuilder(NewAccessorArgs(true, false, false, salt), id)
		a.builders = []AccessorIndexBuilder{builder}
	}

	return a, nil
}

func (a *RelationalAppendable) encTs(ts uint64) []byte {
	return ae.EncToBytes(ts, true)
}

type RelationalAppendableTx struct {
	*ProtoAppendableTx
	a  *RelationalAppendable
	id AppendableId
}

func (a *RelationalAppendable) BeginFilesRo() *RelationalAppendableTx {
	return &RelationalAppendableTx{
		ProtoAppendableTx: a.ProtoAppendable.BeginFilesRo(),
		a:                 a,
		id:                a.a,
	}
}

func (a *RelationalAppendableTx) GetWithFlags(entityNum Num, tx kv.Tx) (Bytes, error) {
	ap := a.a
	lastNum := ap.VisibleFilesMaxNum()
	if entityNum <= lastNum {
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
	}

	// else the db
	return tx.GetOne(ap.valsTbl, entityNum.EncTo8Bytes())
}

func (a *RelationalAppendableTx) Append(entityNum Num, value Bytes, tx kv.RwTx) error {
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

func (a *RelationalAppendableTx) Prune(ctx context.Context, to RootNum, limit uint64, tx kv.RwTx) error {
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

func (a *RelationalAppendableTx) Unwind(ctx context.Context, from RootNum, rwTx kv.RwTx) error {
	ap := a.a
	fromId, err := ap.rel.RootNum2Num(from, rwTx)
	if err != nil {
		return err
	}
	return ae.DeleteRangeFromTbl(ap.valsTbl, ap.encTs(uint64(fromId)), nil, 0, rwTx)
}

func (a *RelationalAppendableTx) NewWriter() *RelationalAppendableWriter {
	// TODO: caplin uses some pool for sortable buffer
	// probably can have a global pool here for this...
	return &RelationalAppendableWriter{
		values: etl.NewCollector(a.id.Name()+".rappendable.flush",
			a.id.Dirs().Tmp, etl.NewSortableBuffer(WALCollectorRAM), a.a.logger).LogLvl(log.LvlTrace),
		valsTable: a.a.valsTbl,
		encFn:     a.a.encTs,
	}
}

func (a *RelationalAppendableTx) Close() {
	if a.files == nil {
		return
	}

	a.ProtoAppendableTx.Close()
}

// buffered writer

// buffered write into etl collector, and then flush to db
// can't perform unwinds on this writer
type RelationalAppendableWriter struct {
	values    *etl.Collector
	valsTable string
	encFn     func(ts uint64) []byte
}

func NewRelationalAppendableWriter(r *RelationalAppendable, valsTable string, values *etl.Collector) *RelationalAppendableWriter {
	return &RelationalAppendableWriter{
		values:    values,
		valsTable: valsTable,
		encFn:     r.encTs,
	}
}

func (w *RelationalAppendableWriter) Close() {
	if w == nil {
		return
	}
	if w.values != nil {
		w.values.Close()
	}
}

func (w *RelationalAppendableWriter) Add(id Id, value Bytes) error {
	key := w.encFn(uint64(id))
	if err := w.values.Collect(key, value); err != nil {
		return err
	}

	return nil
}

func (w *RelationalAppendableWriter) Flush(ctx context.Context, tx kv.RwTx) error {
	if w.values == nil {
		return nil
	}
	// load uses Append since identityLoadFunc is used.
	// might want to configure other TransformArgs here?
	//
	return w.values.Load(tx, w.valsTable, etl.IdentityLoadFunc, etl.TransformArgs{Quit: ctx.Done()})
}
