package state

import (
	"github.com/erigontech/erigon-lib/kv"
	ae "github.com/erigontech/erigon-lib/state/appendables_extras"
)

//// relations

var _ RootRelationI = (*PointRelation)(nil)
var _ RootRelationI = (*ManyToOneRelation)(nil)
var _ RootRelationI = (*OneToManyRelation)(nil)
var _ RootRelationI = (*OneToManyPureRelation)(nil)

// 1:1; RootNum = Num
type PointRelation struct{}

func (r *PointRelation) RootNum2Id(inp RootNum, tx kv.Tx) (Id, error) {
	return Id(inp), nil
}

func (r *PointRelation) Num2Id(num Num, tx kv.Tx) (Id, error) {
	return Id(num), nil
}

//////////////////////////////////////////////

// many:1; EntityEnds tbl: start RootNum -> num
// also id == num here (only canonical data)
type ManyToOneRelation struct {
	entityEndsTbl string
}

func (r *ManyToOneRelation) RootNum2Id(inp RootNum, tx kv.Tx) (Id, error) {
	c, err := tx.Cursor(r.entityEndsTbl)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	_, v, err := c.Seek(inp.EncTo8Bytes())
	if err != nil {
		return 0, err
	}

	return Id(ae.Decode64FromBytes(v, true)), nil
}

func (r *ManyToOneRelation) Num2Id(num Num, tx kv.Tx) (Id, error) {
	return Id(num), nil
}

//////////////////////////////////////////////

// 1:many; with MaxNumTbl
// e.g. txs, borevents
type OneToManyRelation struct {
	maxNumTbl         string
	strictlyAppending bool // i.e. no delete on unwind
}

// returns 1st id present in the given inp RootNum
func (r *OneToManyRelation) RootNum2Id(inp RootNum, tx kv.Tx) (Id, error) {
	prevMaxNum, err := tx.GetOne(r.maxNumTbl, ae.EncToBytes(uint64(inp)-1, true))
	if err != nil {
		return 0, err
	}
	
	// wrong: for txs this gives num; but i'm using id here
	return Id(ae.Decode64FromBytes(prevMaxNum, true) + 1), nil
}

func (r *OneToManyRelation) Num2Id(num Num, tx kv.Tx) (Id, error) {
	if !r.strictlyAppending {
		// id == num
		return Id(num), nil
	}

	// TODO: else, it's case like txs and we need to binary search over the maxNumTbl
	cursor, err := tx.Cursor(r.maxNumTbl)
	if err != nil {
		return 0, err
	}
	defer cursor.Close()

	//curso

	return 0, nil
}

// 1: many; pure function
// e.g: spans
// no non-canonical data (id == num)
type OneToManyPureRelation struct {
	fn func(inp RootNum) Id
}

func (r *OneToManyPureRelation) RootNum2Id(inp RootNum, tx kv.Tx) (Id, error) {
	return r.fn(inp), nil
}

func (r *OneToManyPureRelation) Num2Id(num Num, tx kv.Tx) (Id, error) {
	return Id(num), nil
}
