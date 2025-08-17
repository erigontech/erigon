package state

import (
	"github.com/erigontech/erigon/db/kv"
)

//// relations

var _ RootRelationI = (*PointRelation)(nil)
var _ RootRelationI = (*ManyToOneRelation)(nil)
var _ RootRelationI = (*OneToManyRelation)(nil)

// 1:1; RootNum = Num
type PointRelation struct{}

func (r *PointRelation) RootNum2Num(inp RootNum, tx kv.Tx) (Num, error) {
	return Num(inp), nil
}

//////////////////////////////////////////////

// many:1; EntityEnds tbl: start RootNum -> id
type ManyToOneRelation struct {
	entityEndsTbl string
}

func (r *ManyToOneRelation) RootNum2Num(inp RootNum, tx kv.Tx) (Num, error) {
	c, err := tx.Cursor(r.entityEndsTbl)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	_, v, err := c.Seek(inp.EncTo8Bytes())
	if err != nil {
		return 0, err
	}

	return Num(kv.Decode64FromBytes(v, true)), nil
}

//////////////////////////////////////////////

// 1:many; with MaxNumTbl
// e.g. borevents
// also id == num here (only canonical data)
type OneToManyRelation struct {
	maxNumTbl string
}

// returns 1st num present in the given inp RootNum
func (r *OneToManyRelation) RootNum2Num(inp RootNum, tx kv.Tx) (Num, error) {
	prevMaxNum, err := tx.GetOne(r.maxNumTbl, kv.EncToBytes(uint64(inp)-1, true))
	if err != nil {
		return 0, err
	}

	return Num(kv.Decode64FromBytes(prevMaxNum, true) + 1), nil
}

// // 1: many; pure function
// // e.g: spans
// // no non-canonical data (id == num)
// type OneToManyPureRelation struct {
// 	fn func(inp RootNum) Num
// }

// func (r *OneToManyPureRelation) RootNum2Num(inp RootNum, tx kv.Tx) (Num, error) {
// 	return r.fn(inp), nil
// }
