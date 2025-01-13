package appendables

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/kv"
)

// stores only canonical values (either canonicity known beforehand; or unwind removes non-canonical values)
// examples: bor events/spans/milestones/checkpoints; caplin beaconblocks; receipts (only for canonical blocks)
type CanonicalAppendable struct {
	*ProtoAppendable
	valsTbl string
	mp      TsNumMapper
}

func NewCanonicalAppendable() Appendable {
	return &CanonicalAppendable{ProtoAppendable: &ProtoAppendable{}}
}

func (a *CanonicalAppendable) SetTsNumMapper(mapper TsNumMapper) { a.mp = mapper }

func (a *CanonicalAppendable) Get(tsNum TsNum, tx kv.Tx) (VVType, error) {
	// first look into snapshots..
	lastTsNum := a.VisibleSegmentsMaxTsNum()
	itsNum := uint64(tsNum)
	if tsNum <= lastTsNum {
		if a.baseKeySameAsTsNum {
			// TODO: can do binary search or loop over visible segments and find which segment contains tsNum
			// and then get from there
			var v *VisibleSegment

			// Note: Get assumes that the first index allows ordinal lookup on tsNum. Is this valid assumption?
			// for borevents this is not a valid assumption
			if a.indexBuilders[0].AllowsOrdinalLookupByTsNum() {
				return v.Get(itsNum)
			} else {
				return nil, fmt.Errorf("ordinal lookup by tsNum not supported for %s", a.enum)
			}
		} else {
			// TODO: loop over all visible segments and find which segment contains tsNum
		}
	}

	// then db
	return tx.GetOne(a.valsTbl, a.encTs(itsNum))
}

func (a *CanonicalAppendable) Put(tsNum TsNum, value VVType, tx kv.RwTx) error {
	return tx.Append(a.valsTbl, a.encTs(uint64(tsNum)), value)
}

func (a *CanonicalAppendable) Prune(ctx context.Context, baseKeyTo TsNum, limit uint64, rwTx kv.RwTx) error {
	// probably fromKey value needs to be in configuration...it is 1 because we want to keep genesis block
	// but this might start from 0 as well.
	stre := a.mp.Get(1, baseKeyTo, rwTx)
	defer stre.Close()
	fromKey, err := stre.Next()
	if err != nil {
		return err
	}
	stre.Close()

	stre = a.mp.Get(baseKeyTo, TsNum(MaxUint64), rwTx)
	defer stre.Close()
	toKey, err := stre.Next()
	if err != nil {
		return err
	}
	stre.Close()

	return DeleteRangeFromTbl(a.valsTbl, fromKey, toKey, limit, rwTx)
}

func (a *CanonicalAppendable) Unwind(ctx context.Context, baseKeyFrom TsNum, rwTx kv.RwTx) error {
	stre := a.mp.Get(baseKeyFrom, TsNum(MaxUint64), rwTx)
	defer stre.Close()
	vkey, err := stre.Next()
	if err != nil {
		return err
	}
	stre.Close()

	return DeleteRangeFromTbl(a.valsTbl, vkey, nil, MaxUint64, rwTx)
}

func (a *CanonicalAppendable) encTs(ts uint64) []byte {
	return Encode64ToBytes(ts, true)
}
