package appendables

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon-lib/kv"
)

// marked appendable has two tables
// 1. canonicalMarkerTbl: stores tsId -> forkId
// 2. valsTbl: maps `bigendian(tsId) + forkId -> value`
// currently assuming that only base appendable can be marked.
type MarkedAppendable struct {
	*ProtoAppendable
	canonicalTbl string
	valsTbl      string

	//tsBytes     int
	ts8Bytes    bool // slots are encoded in 4 bytes; block number in 8 bytes
	forkIdBytes int
	//comb         CombinedKey
}

func NewMarkedAppendable(enum ApEnum, stepSize uint64, canonicalTbl, valsTbl string) Appendable {
	return &MarkedAppendable{
		ProtoAppendable: NewProtoAppendable(enum, stepSize),
		canonicalTbl:    canonicalTbl,
		valsTbl:         valsTbl,
		ts8Bytes:        true,
		forkIdBytes:     32, // assuming common.Hash
	}
}

func (a *MarkedAppendable) Get(tsNum TsNum, tx kv.Tx) (VVType, error) {
	// first look into snapshots..
	lastTsNum := a.VisibleSegmentsMaxTsNum()
	itsNum := uint64(tsNum)
	if tsNum <= lastTsNum {
		if a.baseKeySameAsTsNum {
			// can do binary search or loop over visible segments and find which segment contains tsNum
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
	forkId, err := tx.GetOne(a.canonicalTbl, a.encTs(itsNum))
	if err != nil {
		return nil, err
	}
	// if forkId == nil....

	key := a.combK(itsNum, forkId)
	return tx.GetOne(a.valsTbl, key)
}

func (a *MarkedAppendable) encTs(ts uint64) []byte {
	return Encode64ToBytes(ts, a.ts8Bytes)
}

func (a *MarkedAppendable) GetNc(tsId uint64, forkId []byte, tx kv.Tx) (VVType, error) {
	key := a.combK(tsId, forkId)
	return tx.GetOne(a.valsTbl, key)
}

func (a *MarkedAppendable) Put(tsId uint64, forkId []byte, value VVType, tx kv.RwTx) error {
	// can then val
	if err := tx.Append(a.canonicalTbl, a.encTs(tsId), forkId); err != nil {
		return err
	}

	key := a.combK(tsId, forkId)
	return tx.Put(a.valsTbl, key, value)
}

func (a *MarkedAppendable) Prune(ctx context.Context, baseKeyTo TsNum, limit uint64, rwTx kv.RwTx) error {
	// from 1 to baseKeyTo (exclusive)

	// probably fromKey value needs to be in configuration...starts from 1 because we want to keep genesis block
	// but this might start from 0 as well.
	fromKey := uint64(1)
	fromKeyPrefix := a.encTs(fromKey)
	toKeyPrefix := a.encTs(uint64(baseKeyTo))
	if err := DeleteRangeFromTbl(a.canonicalTbl, fromKeyPrefix, toKeyPrefix, limit, rwTx); err != nil {
		return err
	}

	if err := DeleteRangeFromTbl(a.valsTbl, fromKeyPrefix, toKeyPrefix, limit, rwTx); err != nil {
		return err
	}

	return nil
}

func (a *MarkedAppendable) Unwind(ctx context.Context, baseKeyFrom TsNum, rwTx kv.RwTx) error {
	fromKey := a.encTs(uint64(baseKeyFrom))
	if err := DeleteRangeFromTbl(a.canonicalTbl, fromKey, nil, MaxUint64, rwTx); err != nil {
		return err
	}

	if err := DeleteRangeFromTbl(a.valsTbl, fromKey, nil, MaxUint64, rwTx); err != nil {
		return err
	}

	return nil
}

// TODO: slots are encoded 4 bytes in canonicalTbl (CanonicalBlockRoots);
// but 8 bytes in prefix of key in valsTbl (BeaconBlocks) -- I think we can be consistent here
// i.e. use a.ts8Bytes instead of a.forkIdBytes....but this needs some kind of migration
// of existing BeaconBlocks table.
const tsLength = 8

func (a *MarkedAppendable) combK(ts uint64, forkId []byte) []byte {
	k := make([]byte, tsLength+a.forkIdBytes)
	binary.BigEndian.PutUint64(k, ts)
	copy(k[tsLength:], forkId)
	return k
}
