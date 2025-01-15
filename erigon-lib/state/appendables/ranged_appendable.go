package appendables

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/kv"
)

// holds EntityEnd table, mapping baseTsnum -> tsNum
// this entity has 1:many relationship with base appendable
// e.g. checkpoints, milestones, spans
// keeps only canonical data
type RangedAppendable struct {
	*ProtoAppendable
	entityEndTbl string
	valsTbl      string
}

func (a *RangedAppendable) Get(tsNum TsNum, tx kv.Tx) (VVType, error) {
	// first look into snapshots..
	lastTsNum := a.VisibleSegmentsMaxTsNum()
	if tsNum <= lastTsNum {
		if a.baseKeySameAsTsNum {
			// TODO: can do binary search or loop over visible segments and find which segment contains tsNum
			// and then get from there
			var v *VisibleSegment

			// Note: Get assumes that the first index allows ordinal lookup on tsNum. Is this valid assumption?
			// for borevents this is not a valid assumption
			if a.indexBuilders[0].AllowsOrdinalLookupByTsNum() {
				return v.Get(tsNum)
			} else {
				return nil, fmt.Errorf("ordinal lookup by tsNum not supported for %s", a.enum)
			}
		} else {
			// TODO: loop over all visible segments and find which segment contains tsNum
		}
	}

	// then db
	return tx.GetOne(a.valsTbl, a.encTs(uint64(tsNum)))
}

func (a *RangedAppendable) Put(tsNum TsNum, value VVType, tx kv.RwTx) error {
	return tx.Append(a.valsTbl, a.encTs(uint64(tsNum)), value)
}

// updating entityEndTbl
func (a *RangedAppendable) PutEntityEnd(tsNum TsNum, startBaseTsNum TsNum, tx kv.RwTx) error {
	return tx.Put(a.entityEndTbl, a.encTs(uint64(tsNum)), a.encTs(uint64(startBaseTsNum)))
}

func (a *RangedAppendable) Prune(ctx context.Context, baseKeyTo TsNum, limit uint64, tx kv.RwTx) error {
	c, err := tx.RwCursor(a.entityEndTbl)
	if err != nil {
		return err
	}

	defer c.Close()
	ibaseKeyTo := uint64(baseKeyTo)
	for _startBaseTsNum, _, err := c.Seek(a.encTs(1)); _startBaseTsNum != nil; _startBaseTsNum, _, err = c.Next() {
		if err != nil {
			return err
		}

		startBaseTsNum := a.decTs(_startBaseTsNum)
		if startBaseTsNum > ibaseKeyTo {
			break
		}

		limit--
		if limit <= 0 {
			break
		}

		if err = c.DeleteCurrent(); err != nil {
			return err
		}
	}

	return nil
}

func (a *RangedAppendable) Unwind(ctx context.Context, baseKeyFrom TsNum, tx kv.RwTx) error {
	c, err := tx.RwCursor(a.entityEndTbl)
	if err != nil {
		return err
	}

	defer c.Close()

	_baseKeyFrom := a.encTs(uint64(baseKeyFrom))
	_, _tsNumFrom, err := c.Seek(_baseKeyFrom)
	if err != nil {
		return err
	}

	c.Close()

	if err := DeleteRangeFromTbl(a.valsTbl, _tsNumFrom, nil, MaxUint64, tx); err != nil {
		return err
	}

	if err := DeleteRangeFromTbl(a.entityEndTbl, _baseKeyFrom, nil, MaxUint64, tx); err != nil {
		return err
	}

	return nil
}

func (a *RangedAppendable) encTs(ts uint64) []byte {
	return Encode64ToBytes(ts, true)
}

func (a *RangedAppendable) decTs(b []byte) uint64 {
	return Decode64FromBytes(b, true)
}
