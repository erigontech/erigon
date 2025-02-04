package appendables

import "github.com/erigontech/erigon-lib/kv"

//// relations

// 1:1; baseNum = Num
type PointRelation struct{}

func (r *PointRelation) BaseNum2Id(inp BaseNum, tx kv.Tx) (Id, error) {
	return Id(inp), nil
}

func (r *PointRelation) Num2Id(num Num, tx kv.Tx) (Id, error) {
	return Id(num), nil
}

//////////////////////////////////////////////

// many:1; EntityEnds tbl: start baseNum -> num
// also id == num here (only canonical data)
type ManyToOneRelation struct {
	entityEndsTbl string
}

func (r *ManyToOneRelation) BaseNum2Id(inp BaseNum, tx kv.Tx) (Id, error) {
	c, err := tx.Cursor(r.entityEndsTbl)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	_, v, err := c.Seek(Encode64ToBytes(uint64(inp), true))
	if err != nil {
		return 0, err
	}

	return Id(Decode64FromBytes(v, true)), nil
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

func (r *OneToManyRelation) BaseNum2Id(inp BaseNum, tx kv.Tx) (Id, error) {
	prevMaxNum, err := tx.GetOne(r.maxNumTbl, Encode64ToBytes(uint64(inp)-1, true))
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
type OneToManyPureRelation struct {
	fn func(inp BaseNum) Id
}

func (r *OneToManyPureRelation) BaseNum2Id(inp BaseNum, tx kv.Tx) (Id, error) {
	return r.fn(inp), nil
}

func (r *OneToManyPureRelation) Num2Id(num Num, tx kv.Tx) (Id, error) {
	return Id(num), nil
}
