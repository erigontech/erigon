package appendables

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
)

// valsTbl has effective mapping baseTsNum -> entity value
// 1 base entity corresponds to 1 entity value
// example caplin states
// valsTbl stores only canonical data
type PointAppendable struct {
	*ProtoAppendable
	valsTbl string
}

func NewPointAppendable(valsTbl string, enum ApEnum, stepSize uint64) Appendable {
	p := &PointAppendable{
		ProtoAppendable: &ProtoAppendable{},
		valsTbl:         valsTbl,
	}

	p.enum = enum
	p.stepSize = stepSize
	p.baseKeySameAsTsNum = true
	// etc.

	// default freezer
	freezer := &PlainFreezer{fetcher: &sequentialFetcher{}, valsTbl: p.valsTbl}
	p.SetFreezer(freezer)

	// default index builders can also be used...these map tsNum -> offset

	return p
}

// func NewCanonicalAppendableWithFreezer() etc.

func (a *PointAppendable) Get(tsNum TsNum, tx kv.Tx) (VVType, error) {
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

func (a *PointAppendable) Put(tsNum TsNum, value VVType, tx kv.RwTx) error {
	return tx.Append(a.valsTbl, a.encTs(uint64(tsNum)), value)
}

func (a *PointAppendable) Prune(ctx context.Context, baseKeyTo TsNum, limit uint64, rwTx kv.RwTx) error {
	// probably fromKey value needs to be in configuration...it is 1 because we want to keep genesis block
	// but this might start from 0 as well.
	fromKey := a.encTs(uint64(1))
	toKey := a.encTs(uint64(baseKeyTo))
	return DeleteRangeFromTbl(a.valsTbl, fromKey, toKey, limit, rwTx)
}

func (a *PointAppendable) Unwind(ctx context.Context, baseKeyFrom TsNum, rwTx kv.RwTx) error {
	fromKey := a.encTs(uint64(baseKeyFrom))
	return DeleteRangeFromTbl(a.valsTbl, fromKey, nil, MaxUint64, rwTx)
}

func (a *PointAppendable) encTs(ts uint64) []byte {
	return Encode64ToBytes(ts, true)
}

/////

type sequentialFetcher struct{}

func (s *sequentialFetcher) GetKeys(baseTsNumFrom, baseTsNumTo TsNum, tx kv.Tx) stream.Uno[VKType] {
	return NewSequentialStream(uint64(baseTsNumFrom), uint64(baseTsNumTo))
}
