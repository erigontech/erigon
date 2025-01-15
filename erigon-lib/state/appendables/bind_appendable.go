package appendables

import (
	"fmt"

	"github.com/erigontech/erigon-lib/kv"
)

// 1:many btwn base and this appendable.
// e.g. txs, events, receipts
// optionally allows for txId to just continue incrementing
// valsTbl + MaxTsNum table
type BindAppendable struct {
	*ProtoAppendable

	valsTbl     string
	maxTsNumTbl string

	// some appendable like txs, shouldn't delete data on unwind
	// this is because these txs might be reused in a later canonical
	// block, and re-downloading txs happens through network, which is expensive.
	doesUnwind bool
}

func (a *BindAppendable) Get(tsNum TsNum, tx kv.Tx) (VVType, error) {
	// first look into snapshots..
	lastTsNum := a.VisibleSegmentsMaxTsNum()
	itsNum := uint64(tsNum)
	if tsNum <= lastTsNum {
		if a.baseKeySameAsTsNum {
			// TODO: can do binary search or loop over visible segments and find which segment contains tsNum
			// and then get from there
			var v *VisibleSegment

			if a.indexBuilders[0].AllowsOrdinalLookupByTsNum() {
				return v.Get(tsNum)
			} else {
				return nil, fmt.Errorf("ordinal lookup by tsNum not supported for %s", a.enum)
			}
		} else {
			// TODO: loop over all visible segments and find which segment contains tsNum
		}
	}

	// db now...
	if a.doesUnwind {
		return tx.GetOne(a.valsTbl, a.encTs(itsNum))
	}

	// else tsNum!=tsId, so we need to use maxTxNumTbl
	// TODO: needs iterator
	return nil, nil
}

// only looks in db
func (a *BindAppendable) GetNc(tsId TsId, tx kv.Tx) (VVType, error) {
	return tx.GetOne(a.valsTbl, a.encTs(uint64(tsId)))
}

func (a *BindAppendable) Put(tsId TsId, value VVType, tx kv.RwTx) error {
	// can then val
	return tx.Append(a.valsTbl, a.encTs(uint64(tsId)), value)
}

func (a *BindAppendable) PutMaxTsNum(baseTsNum, maxTsNum TsNum, tx kv.RwTx) error {
	return tx.Put(a.maxTsNumTbl, a.encTs(uint64(baseTsNum)), a.encTs(uint64(maxTsNum)))
}

func (a *BindAppendable) encTs(ts uint64) []byte {
	return Encode64ToBytes(ts, true)
}

// index interface
//
