package appendables

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
)

type RelationI interface {
	BaseNum2Id(fromBaseTsNum TsNum, tx kv.Tx) (TsId, error)
	Num2Id(num TsNum, tx kv.Tx) (TsId, error)
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

	// default index builders can also be used...these map tsNum -> offset

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

func (a *RelationalAppendableRoTx) Get(tsNum TsNum, tx kv.Tx) (VVType, error) {
	ap := a.a
	lastTsNum := ap.VisibleSegmentsMaxTsNum()
	if tsNum <= lastTsNum {
		if ap.baseKeySameAsTsNum {
			// TODO: can do binary search or loop over visible segments and find which segment contains tsNum
			// and then get from there
			var v *VisibleSegment

			// Note: Get assumes that the first index allows ordinal lookup on tsNum. Is this valid assumption?
			// for borevents this is not a valid assumption
			if ap.indexBuilders[0].AllowsOrdinalLookupByTsNum() {
				return v.Get(tsNum)
			} else {
				return nil, fmt.Errorf("ordinal lookup by tsNum not supported for %s", a.enum)
			}
		} else {
			// TODO: loop over all visible segments and find which segment contains tsNum
		}
	}

	// then db
	tsId, err := ap.relation.BaseNum2Id(tsNum, tx)
	if err != nil {
		return nil, err
	}
	return tx.GetOne(ap.valsTbl, ap.encTs(uint64(tsId)))
}

func (a *RelationalAppendableRoTx) Put(tsId TsId, value VVType, tx kv.RwTx) error {
	return tx.Append(a.a.valsTbl, a.a.encTs(uint64(tsId)), value)
}

func (a *RelationalAppendableRoTx) Prune(ctx context.Context, baseKeyTo TsNum, limit uint64, rwTx kv.RwTx) error {
	fromKey := a.a.encTs(uint64(1)) // config driven
	toKey := a.a.encTs(uint64(baseKeyTo))
	return DeleteRangeFromTbl(a.a.valsTbl, fromKey, toKey, limit, rwTx)
}

func (a *RelationalAppendableRoTx) Unwind(ctx context.Context, baseKeyFrom TsNum, limit uint64, rwTx kv.RwTx) error {
	if a.a.noUnwind {
		return nil
	}
	fromKey := a.a.encTs(uint64(baseKeyFrom))
	return DeleteRangeFromTbl(a.a.valsTbl, fromKey, nil, MaxUint64, rwTx)
}

func (a *RelationalAppendableRoTx) Close() {
	if a.files == nil {
		return
	}

	a.ProtoAppendableRoTx.Close()
}

///////

type ValueKeyFetcherFromRelation struct {
	relation RelationI
}

func (f *ValueKeyFetcherFromRelation) GetKeys(baseTsNumFrom, baseTsNumTo TsNum, tx kv.Tx) stream.Uno[VKType] {
	from, _ := f.relation.BaseNum2Id(baseTsNumFrom, tx)
	to, _ := f.relation.BaseNum2Id(baseTsNumTo, tx)

	// can do better here for sparsed valsTbl like borcheckpoints, many:1; rather than
	// iterating over all the keys, might be more efficient to iterate over db.NExt()
	return NewSequentialStream(uint64(from), uint64(to))
}

//// relations

// 1:1; baseTsNum = tsNum
type PointRelation struct{}

func (r *PointRelation) BaseNum2Id(fromBaseTsNum TsNum, tx kv.Tx) (TsId, error) {
	return TsId(fromBaseTsNum), nil
}

func (r *PointRelation) Num2Id(tsNum TsNum, tx kv.Tx) (TsId, error) {
	return TsId(tsNum), nil
}

// many:1; EntityEnds tbl: start baseTsNum -> tsNum
// also tsId == tsNum here (only canonical data)
type ManyToOneRelation struct {
	entityEndsTbl string
}

func (r *ManyToOneRelation) BaseNum2Id(fromBaseTsNum TsNum, tx kv.Tx) (TsId, error) {
	c, err := tx.Cursor(r.entityEndsTbl)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	_, v, err := c.Seek(Encode64ToBytes(uint64(fromBaseTsNum), true))
	if err != nil {
		return 0, err
	}

	return TsId(Decode64FromBytes(v, true)), nil
}

func (r *ManyToOneRelation) Num2Id(tsNum TsNum, tx kv.Tx) (TsId, error) {
	return TsId(tsNum), nil
}

// 1:many; with MaxTsNumTbl
// e.g. txs, borevents
type OneToManyRelation struct {
	maxTsNumTbl       string
	strictlyAppending bool // i.e. no delete on unwind
}

func (r *OneToManyRelation) BaseNum2Id(fromBaseTsNum TsNum, tx kv.Tx) (TsId, error) {
	prevMaxTsNum, err := tx.GetOne(r.maxTsNumTbl, Encode64ToBytes(uint64(fromBaseTsNum)-1, true))
	if err != nil {
		return 0, err
	}

	return TsId(Decode64FromBytes(prevMaxTsNum, true) + 1), nil
}

func (r *OneToManyRelation) Num2Id(tsNum TsNum, tx kv.Tx) (TsId, error) {
	if !r.strictlyAppending {
		// tsId == tsNum
		return TsId(tsNum), nil
	}

	// TODO: else, it's case like txs and we need to binary search over the maxTsNumTbl
	return 0, nil
}

// 1: many; pure function
// e.g: spans
// no non-canonical data (tsId == tsNum)
type OneToManyRelationPure struct {
	fn func(baseTsNum TsNum) TsId
}

func (r *OneToManyRelationPure) BaseNum2Id(fromBaseTsNum TsNum, tx kv.Tx) (TsId, error) {
	return r.fn(fromBaseTsNum), nil
}

func (r *OneToManyRelationPure) Num2Id(tsNum TsNum, tx kv.Tx) (TsId, error) {
	return TsId(tsNum), nil
}
