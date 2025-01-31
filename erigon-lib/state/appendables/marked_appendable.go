package appendables

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
)

// marked appendable has two tables
// 1. canonicalMarkerTbl: stores tsId -> hash
// 2. valsTbl: maps `bigendian(tsId) + hash -> value`
// common for base appendables to be marked, as it provides quick way to unwind.
// headers are marked; and also bodies. caplin blockbodies too.
type MarkedAppendable struct {
	*ProtoAppendable
	canonicalTbl string
	valsTbl      string

	//tsBytes     int
	ts8Bytes  bool // slots are encoded in 4 bytes; block number in 8 bytes
	hashBytes int
	//comb         CombinedKey
}

type MAOpts func(*MarkedAppendable)

func (r *MAOpts) WithFreezer(freezer Freezer) MAOpts {
	return func(a *MarkedAppendable) {
		a.SetFreezer(freezer)
	}
}

func (r *MAOpts) WithIndexBuilders(builders ...AccessorIndexBuilder) MAOpts {
	return func(a *MarkedAppendable) {
		a.SetIndexBuilders(builders...)
	}
}

func (r *MAOpts) WithStepSize(stepSize uint64) MAOpts {
	return func(a *MarkedAppendable) {
		a.stepSize = stepSize
	}
}

func (r *MAOpts) WithTs8Bytes(ts8Bytes bool) MAOpts {
	return func(a *MarkedAppendable) {
		a.ts8Bytes = ts8Bytes
	}
}

func (r *MAOpts) WithDataDir(dirs datadir.Dirs) MAOpts {
	return func(a *MarkedAppendable) {
		a.dirs = dirs
	}
}

func NewMarkedAppendable(enum ApEnum, canonicalTbl, valsTbl string, opts ...MAOpts) (Appendable, error) {
	m := &MarkedAppendable{
		ProtoAppendable: NewProtoAppendable(enum, 500),
		canonicalTbl:    canonicalTbl,
		valsTbl:         valsTbl,
		ts8Bytes:        true,
		hashBytes:       32, // assuming common.Hash
	}

	for _, opt := range opts {
		opt(m)
	}

	if m.freezer == nil {
		// marked appendable examples: headers, bodies, beaconblocks
		// all of them have custom freezer impl, so not bothering with default freezer here.
		panic("freezer is nil")
	}

	if m.indexBuilders == nil {
		// default
		// mapping num -> offset (ordinal map)
		salt, err := snaptype.GetIndexSalt(m.dirs.Snap) // this is bad; ApEnum should know it;s own Dirs
		if err != nil {
			return nil, err
		}
		builder := NewSimpleAccessorBuilder(NewAccessorArgs(true, false, false, salt), enum)
		m.SetIndexBuilders([]AccessorIndexBuilder{builder}...)
	}

	return m, nil
}

func (a *MarkedAppendable) encTs(ts Num) []byte {
	return Encode64ToBytes(uint64(ts), a.ts8Bytes)
}

// TODO: slots are encoded 4 bytes in canonicalTbl (CanonicalBlockRoots);
// but 8 bytes in prefix of key in valsTbl (BeaconBlocks) -- I think we can be consistent here
// i.e. use a.ts8Bytes instead of a.hashBytes....but this needs some kind of migration
// of existing BeaconBlocks table.
const tsLength = 8

func (a *MarkedAppendable) combK(ts Num, hash []byte) []byte {
	k := make([]byte, tsLength+a.hashBytes)
	binary.BigEndian.PutUint64(k, uint64(ts))
	copy(k[tsLength:], hash)
	return k
}

// rotx
type MarkedAppendableTx struct {
	*ProtoAppendableTx
	a *MarkedAppendable
}

func (m *MarkedAppendable) BeginFilesRo() *MarkedAppendableTx {
	return &MarkedAppendableTx{
		ProtoAppendableTx: m.ProtoAppendable.BeginFilesRo(),
		a:                 m,
	}
}

func (r *MarkedAppendableTx) Get(num Num, tx kv.Tx) (VVType, error) {
	// first look into snapshots..
	a := r.a
	lastNum := Num(a.VisibleSegmentsMaxNum()) // num == baseNum
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
	canHash, err := tx.GetOne(a.canonicalTbl, a.encTs(num))
	if err != nil {
		return nil, err
	}
	// if canHash == nil....

	key := a.combK(num, canHash)
	return tx.GetOne(a.valsTbl, key)
}

func (r *MarkedAppendableTx) GetNc(num Num, hash []byte, tx kv.Tx) (VVType, error) {
	a := r.a
	key := a.combK(num, hash)
	return tx.GetOne(a.valsTbl, key)
}

// type MarkedAppendableRwTx struct {
// 	*MarkedAppendableRoTx
// }

// func (m *MarkedAppendable) BeginFilesRw() *MarkedAppendableRwTx {
// 	return &MarkedAppendableRwTx{
// 		MarkedAppendableRoTx: m.BeginFilesRo(),
// 	}
// }

func (r *MarkedAppendableTx) Put(num Num, hash []byte, value VVType, tx kv.RwTx) error {
	// can then val
	a := r.a
	if err := tx.Append(a.canonicalTbl, a.encTs(num), hash); err != nil {
		return err
	}

	key := a.combK(num, hash)
	return tx.Put(a.valsTbl, key, value)
}

func (r *MarkedAppendableTx) Prune(ctx context.Context, to BaseNum, limit uint64, rwTx kv.RwTx) error {
	// from 1 to baseKeyTo (exclusive)

	// probably fromKey value needs to be in configuration...starts from 1 because we want to keep genesis block
	// but this might start from 0 as well.
	a := r.a
	fromKey := Num(1)
	fromKeyPrefix := a.encTs(fromKey)
	toKeyPrefix := a.encTs(Num(to - 1))
	if err := DeleteRangeFromTbl(a.canonicalTbl, fromKeyPrefix, toKeyPrefix, limit, rwTx); err != nil {
		return err
	}

	if err := DeleteRangeFromTbl(a.valsTbl, fromKeyPrefix, toKeyPrefix, limit, rwTx); err != nil {
		return err
	}

	return nil
}

func (r *MarkedAppendableTx) Unwind(ctx context.Context, from BaseNum, rwTx kv.RwTx) error {
	a := r.a
	fromKey := a.encTs(Num(from))
	if err := DeleteRangeFromTbl(a.canonicalTbl, fromKey, nil, MaxUint64, rwTx); err != nil {
		return err
	}

	if err := DeleteRangeFromTbl(a.valsTbl, fromKey, nil, MaxUint64, rwTx); err != nil {
		return err
	}

	return nil
}
