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
// common for base appendables to be marked, as it provides quick way to unwind.
// headers are marked; and also bodies. caplin blockbodies too.
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

func (a *MarkedAppendable) Get(num Num, tx kv.Tx) (VVType, error) {
	// first look into snapshots..
	lastNum := a.VisibleSegmentsMaxNum()
	if num <= lastNum {
		if a.baseNumSameAsNum {
			// can do binary search or loop over visible segments and find which segment contains num
			// and then get from there
			var v *VisibleSegment

			// Note: Get assumes that the first index allows ordinal lookup on num. Is this valid assumption?
			// for borevents this is not a valid assumption
			if a.indexBuilders[0].AllowsOrdinalLookupByNum() {
				return v.Get(num)
			} else {
				return nil, fmt.Errorf("ordinal lookup by num not supported for %s", a.enum)
			}
		} else {
			// TODO: loop over all visible segments and find which segment contains num
		}
	}

	// then db
	iNum := uint64(num)
	forkId, err := tx.GetOne(a.canonicalTbl, a.encTs(iNum))
	if err != nil {
		return nil, err
	}
	// if forkId == nil....

	key := a.combK(iNum, forkId)
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

func (a *MarkedAppendable) Prune(ctx context.Context, baseKeyTo Num, limit uint64, rwTx kv.RwTx) error {
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

func (a *MarkedAppendable) Unwind(ctx context.Context, baseKeyFrom Num, rwTx kv.RwTx) error {
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

// rotx
type MarkedAppendableRoTx struct {
	*ProtoAppendableRoTx
	m *MarkedAppendable
}
