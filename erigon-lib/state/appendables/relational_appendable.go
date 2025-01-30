package appendables

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
)

// concrete impls in relations.go
type RelationI interface {
	BaseNum2Id(fromBaseNum Num, tx kv.Tx) (Id, error)
	Num2Id(num Num, tx kv.Tx) (Id, error)
}

// this have 1:many or many:1 or 1:1 relation with base appendable
type RelationalAppendable struct {
	*ProtoAppendable

	relation RelationI
	valsTbl  string
	noUnwind bool // if true, don't delete on unwind; tsId keeps increasing
}

type RAOpts func(a *RelationalAppendable)

// making this a method on RelationalAppendableOption to allow namespacing; since MarkedAppendableOptions
// have same functions.
func (r *RAOpts) WithFreezer(freezer Freezer) RAOpts {
	return func(a *RelationalAppendable) {
		a.SetFreezer(freezer)
	}
}

func (r *RAOpts) WithIndexBuilders(builders ...AccessorIndexBuilder) RAOpts {
	return func(a *RelationalAppendable) {
		a.SetIndexBuilders(builders...)
	}
}

func (r *RAOpts) WithStepSize(stepSize uint64) RAOpts {
	return func(a *RelationalAppendable) {
		a.stepSize = stepSize
	}
}

func (r *RAOpts) WithNoUnwind() RAOpts {
	return func(a *RelationalAppendable) {
		a.noUnwind = true
	}
}

func (r *RAOpts) WithDataDir(dirs datadir.Dirs) RAOpts {
	return func(a *RelationalAppendable) {
		a.dirs = dirs
	}
}

func NewRelationalAppendable(relation RelationI, enum ApEnum, options ...RAOpts) (*RelationalAppendable, error) {
	a := &RelationalAppendable{
		ProtoAppendable: NewProtoAppendable(enum, 500),
		relation:        relation,
	}

	for _, opt := range options {
		opt(a)
	}

	if a.freezer == nil {
		// default freezer
		freezer := &PlainFreezer{fetcher: &ValueKeyFetcherFromRelation{relation: relation}}
		a.SetFreezer(freezer)
	}

	if a.indexBuilders == nil {
		// mapping num -> offset (ordinal map)
		salt, err := snaptype.GetIndexSalt(a.dirs.Snap) // this is bad; ApEnum should know it;s own Dirs, current specified via RAOpts
		if err != nil {
			return nil, err
		}
		builder := NewSimpleAccessorBuilder(NewAccessorArgs(true, false, false, salt), enum)
		a.SetIndexBuilders([]AccessorIndexBuilder{builder}...)

	}

	return a, nil
}

func (a *RelationalAppendable) encTs(ts uint64) []byte {
	return Encode64ToBytes(ts, true)
}

type RelationalAppendableRoTx struct {
	*ProtoAppendableRoTx
	a *RelationalAppendable
}

func (a *RelationalAppendable) BeginFilesRo() *RelationalAppendableRoTx {
	return &RelationalAppendableRoTx{
		ProtoAppendableRoTx: a.ProtoAppendable.BeginFilesRo(),
		a:                   a,
	}
}

func (a *RelationalAppendableRoTx) Get(num Num, tx kv.Tx) (VVType, error) {
	ap := a.a
	lastNum := ap.VisibleSegmentsMaxNum()
	if num <= lastNum {
		if ap.baseNumSameAsNum {
			// TODO: can do binary search or loop over visible segments and find which segment contains num
			// and then get from there
			var v *VisibleSegment

			// Note: Get assumes that the first index allows ordinal lookup on num. Is this valid assumption?
			// for borevents this is not a valid assumption
			if ap.indexBuilders[0].AllowsOrdinalLookupByNum() {
				return v.Get(num)
			} else {
				return nil, fmt.Errorf("ordinal lookup by num not supported for %s", a.enum)
			}
		} else {
			// TODO: loop over all visible segments and find which segment contains num
		}
	}

	// then db
	id, err := ap.relation.Num2Id(num, tx)
	if err != nil {
		return nil, err
	}
	return tx.GetOne(ap.valsTbl, ap.encTs(uint64(id)))
}

// only db
func (a *RelationalAppendableRoTx) GetNc(id Id, tx kv.Tx) (VVType, error) {
	return tx.GetOne(a.a.valsTbl, a.a.encTs(uint64(id)))
}

func (a *RelationalAppendableRoTx) NewWriter() *RelationalAppendableWriter {
	return &RelationalAppendableWriter{}
}

func (a *RelationalAppendableRoTx) Close() {
	if a.files == nil {
		return
	}

	a.ProtoAppendableRoTx.Close()
}

// RelationalAppendableRwTx

// type RelationalAppendableRwTx struct {
// 	*RelationalAppendableRoTx
// }

// func (a *RelationalAppendable) BeginFilesRw() *RelationalAppendableRwTx {
// 	return &RelationalAppendableRwTx{
// 		RelationalAppendableRoTx: a.BeginFilesRo(),
// 	}
// }

func (a *RelationalAppendableRoTx) Put(id Id, value VVType, tx kv.RwTx) error {
	return tx.Append(a.a.valsTbl, a.a.encTs(uint64(id)), value)
}

func (a *RelationalAppendableRoTx) Prune(ctx context.Context, baseKeyTo Num, limit uint64, rwTx kv.RwTx) error {
	fromKey := a.a.encTs(uint64(1)) // config driven
	toKey := a.a.encTs(uint64(baseKeyTo) - 1)
	return DeleteRangeFromTbl(a.a.valsTbl, fromKey, toKey, limit, rwTx)
}

func (a *RelationalAppendableRoTx) Unwind(ctx context.Context, baseKeyFrom Num, limit uint64, rwTx kv.RwTx) error {
	if a.a.noUnwind {
		return nil
	}
	fromKey := a.a.encTs(uint64(baseKeyFrom))
	return DeleteRangeFromTbl(a.a.valsTbl, fromKey, nil, MaxUint64, rwTx)
}

///// appendable writers

// NewWriter returns a writer for write ops on the appendbale
// buffered=true => writes written to db via etl pipeline => read after write not available
// buffered=false => writes written to db via direct write => read after write is available
// func (r *RelationalAppendable) NewWriter(buffered bool) *AppendableWriterer {
// 	if

// }

///////

type ValueKeyFetcherFromRelation struct {
	relation RelationI
}

func (f *ValueKeyFetcherFromRelation) GetKeys(baseNumFrom, baseNumTo Num, tx kv.Tx) stream.Uno[VKType] {
	from, _ := f.relation.BaseNum2Id(baseNumFrom, tx)
	to, _ := f.relation.BaseNum2Id(baseNumTo, tx)

	// can do better here for sparsed valsTbl like borcheckpoints, many:1; rather than
	// iterating over all the keys, might be more efficient to iterate over db.NExt()
	return NewSequentialStream(uint64(from), uint64(to))
}

///

// / buffered write into etl collector, and then flush to db
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

func (w *RelationalAppendableWriter) Add(id Id, value VVType) error {
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
