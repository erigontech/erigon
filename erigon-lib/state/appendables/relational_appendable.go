package appendables

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
)

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

func NewRelationalAppendable(relation RelationI, enum ApEnum, stepSize uint64) *RelationalAppendable {
	a := &RelationalAppendable{
		ProtoAppendable: NewProtoAppendable(enum, stepSize),
		relation:        relation,
	}

	//freezer := &PlainFreezer{fetcher:
	freezer := &PlainFreezer{fetcher: &ValueKeyFetcherFromRelation{relation: relation}}
	a.SetFreezer(freezer)

	// default index builders can also be used...these map num -> offset

	return a
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
	id, err := ap.relation.BaseNum2Id(num, tx)
	if err != nil {
		return nil, err
	}
	return tx.GetOne(ap.valsTbl, ap.encTs(uint64(id)))
}

// only db
func (a *RelationalAppendableRoTx) GetNc(id Id, tx kv.Tx) (VVType, error) {
	return tx.GetOne(a.a.valsTbl, a.a.encTs(uint64(id)))
}

func (a *RelationalAppendableRoTx) Close() {
	if a.files == nil {
		return
	}

	a.ProtoAppendableRoTx.Close()
}

// RelationalAppendableRwTx

type RelationalAppendableRwTx struct {
	*RelationalAppendableRoTx
}

func (a *RelationalAppendable) BeginFilesRw() *RelationalAppendableRwTx {
	return &RelationalAppendableRwTx{
		RelationalAppendableRoTx: a.BeginFilesRo(),
	}
}

func (a *RelationalAppendableRwTx) Put(id Id, value VVType, tx kv.RwTx) error {
	return tx.Append(a.a.valsTbl, a.a.encTs(uint64(id)), value)
}

func (a *RelationalAppendableRwTx) Prune(ctx context.Context, baseKeyTo Num, limit uint64, rwTx kv.RwTx) error {
	fromKey := a.a.encTs(uint64(1)) // config driven
	toKey := a.a.encTs(uint64(baseKeyTo))
	return DeleteRangeFromTbl(a.a.valsTbl, fromKey, toKey, limit, rwTx)
}

func (a *RelationalAppendableRwTx) Unwind(ctx context.Context, baseKeyFrom Num, limit uint64, rwTx kv.RwTx) error {
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

//// relations

// 1:1; baseNum = Num
type PointRelation struct{}

func (r *PointRelation) BaseNum2Id(fromBaseNum Num, tx kv.Tx) (Id, error) {
	return Id(fromBaseNum), nil
}

func (r *PointRelation) Num2Id(num Num, tx kv.Tx) (Id, error) {
	return Id(num), nil
}

// many:1; EntityEnds tbl: start baseNum -> num
// also id == num here (only canonical data)
type ManyToOneRelation struct {
	entityEndsTbl string
}

func (r *ManyToOneRelation) BaseNum2Id(fromBaseNum Num, tx kv.Tx) (Id, error) {
	c, err := tx.Cursor(r.entityEndsTbl)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	_, v, err := c.Seek(Encode64ToBytes(uint64(fromBaseNum), true))
	if err != nil {
		return 0, err
	}

	return Id(Decode64FromBytes(v, true)), nil
}

func (r *ManyToOneRelation) Num2Id(num Num, tx kv.Tx) (Id, error) {
	return Id(num), nil
}

// 1:many; with MaxNumTbl
// e.g. txs, borevents
type OneToManyRelation struct {
	maxNumTbl         string
	strictlyAppending bool // i.e. no delete on unwind
}

func (r *OneToManyRelation) BaseNum2Id(fromBaseNum Num, tx kv.Tx) (Id, error) {
	prevMaxNum, err := tx.GetOne(r.maxNumTbl, Encode64ToBytes(uint64(fromBaseNum)-1, true))
	if err != nil {
		return 0, err
	}

	return Id(Decode64FromBytes(prevMaxNum, true) + 1), nil
}

func (r *OneToManyRelation) Num2Id(num Num, tx kv.Tx) (Id, error) {
	if !r.strictlyAppending {
		// id == num
		return Id(num), nil
	}

	// TODO: else, it's case like txs and we need to binary search over the maxNumTbl
	return 0, nil
}

// 1: many; pure function
// e.g: spans
// no non-canonical data (id == num)
type OneToManyRelationPure struct {
	fn func(baseNum Num) Id
}

func (r *OneToManyRelationPure) BaseNum2Id(fromBaseNum Num, tx kv.Tx) (Id, error) {
	return r.fn(fromBaseNum), nil
}

func (r *OneToManyRelationPure) Num2Id(num Num, tx kv.Tx) (Id, error) {
	return Id(num), nil
}
