package state

import (
	"github.com/erigontech/erigon-lib/kv"
	ae "github.com/erigontech/erigon-lib/state/appendables_extras"
	"github.com/erigontech/erigon/core/types"
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
// e.g. borevents
// also id == num here (only canonical data)
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

	return Id(ae.Decode64FromBytes(prevMaxNum, true) + 1), nil
}

func (r *OneToManyRelation) Num2Id(num Num, tx kv.Tx) (Id, error) {
	if !r.strictlyAppending {
		// id == num
		return Id(num), nil
	}

	panic("must be implemented for strictly appending")
}

// 1:many; with MaxNumTbl
// also id != num here (only canonical data)
// e.g. txs
type BlockToTxnRelation struct {
	r       *OneToManyRelation
	block   *MarkedAppendable
	decoder func(inp []byte, body *types.BodyForStorage) error
}

func (r *BlockToTxnRelation) RootNum2Id(inp RootNum, tx kv.Tx) (Id, error) {
	blockBytes, err := r.block.GetDb(Num(inp), nil, tx)
	if err != nil {
		return 0, err
	}
	b := &types.BodyForStorage{}
	if err := r.decoder(blockBytes, b); err != nil {
		return 0, err
	}

	// not sure to return system tx or not
	return Id(b.BaseTxnID.First()), nil
}

func (r *BlockToTxnRelation) Num2Id(num Num, tx kv.Tx) (Id, error) {
	
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
