package appendables

import (
	"context"
	"encoding/binary"

	"github.com/erigontech/erigon-lib/kv"
)

type CombinedKey func(tsNum uint64, forkId []byte) []byte

// marked appendable has two tables
// 1. canonicalMarkerTbl: stores tsId -> forkId
// 2. valsTbl: maps tsId + forkId -> value
// currently assuming that only base appendable can be marked.
type MarkedAppendable struct {
	*BaseAppendable
	canonicalTbl string
	valsTbl      string
	comb         CombinedKey
}

func (a *MarkedAppendable) Get(tsNum uint64, tx kv.Tx) (VVType, error) {
	// first look into snapshots..
	lastTsNum := a.VisibleSegmentsMaxTsNum()
	if tsNum <= lastTsNum {
		if a.baseKeySameAsTsNum {
			// can do binary search or loop over visible segments and find which segment contains tsNum
			// and then get from there
			var v *VisibleSegment
			return v.Get(tsNum)
		} else {
			// TODO: loop over all visible segments and find which segment contains tsNum
		}
	}

	// then db
	forkId, err := tx.GetOne(a.canonicalTbl, a.encTsNum(tsNum))
	if err != nil {
		return nil, err
	}

	key := a.comb(tsNum, forkId)
	return tx.GetOne(a.valsTbl, key)
}

func (a *MarkedAppendable) encTsNum(tsNum uint64) []byte {
	// verify: assuming canonical marked table key is always encoded like this
	var encTsNum [8]byte
	binary.BigEndian.PutUint64(encTsNum[:], tsNum)
	return encTsNum[:]
}

func (a *MarkedAppendable) GetNc(tsId uint64, forkId []byte, tx kv.Tx) (VVType, error) {
	key := a.comb(tsId, forkId)
	return tx.GetOne(a.valsTbl, key)
}

func (a *MarkedAppendable) Put(tsId uint64, forkId []byte, value VVType, tx kv.RwTx) error {
	// can then val

	if err := tx.Append(a.canonicalTbl, a.encTsNum(tsId), forkId); err != nil {
		return err
	}

	key := a.comb(tsId, forkId)
	return tx.Put(a.valsTbl, key, value)
}

func (a *MarkedAppendable) Prune(ctx context.Context, baseKeyTo, limit uint64, rwTx kv.RwTx) error {
	// TODO
	return nil
}

func (a *MarkedAppendable) Unwind(ctx context.Context, baseKeyFrom uint64, rwTx kv.RwTx) error {
	// TODO
	return nil
}
