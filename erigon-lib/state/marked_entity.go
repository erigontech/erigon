package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	ae "github.com/erigontech/erigon-lib/state/entity_extras"
)

const MaxUint64 = ^uint64(0)

/*
marked entity has two tables

 1. canonicalMarkerTbl: stores num -> canonical hash

 2. valsTbl: maps `bigendian(num) + hash -> value`

    valsTbl stores canonical and non-canonical entires. Whereas canonicalMarkerTbl
    maps num -> canonical hash.

    marked entity Num is the same as the entity-set RootNum i.e.
    values of Num == values of RootNum
    headers, bodies, caplin blockbodies are marked entity.
*/
type MarkedEntity struct {
	*ProtoEntity

	canonicalTbl string
	valsTbl      string

	ts4Bytes bool // slots are encoded in 4 bytes; everything else in 8 bytes

	// pruning happens from this entity number
	// e.g. might not want to prune genesis block, in which case pruneFrom = 1
	pruneFrom Num
}

type MAOpts func(*MarkedEntity)

func MA_WithFreezer(freezer Freezer) MAOpts {
	return func(a *MarkedEntity) {
		a.freezer = freezer
	}
}

func MA_WithIndexBuilders(builders ...AccessorIndexBuilder) MAOpts {
	return func(a *MarkedEntity) {
		a.builders = builders
	}
}

func MA_WithTs4Bytes(ts4Bytes bool) MAOpts {
	return func(a *MarkedEntity) {
		a.ts4Bytes = ts4Bytes
	}
}

func MA_WithPruneFrom(pruneFrom Num) MAOpts {
	return func(a *MarkedEntity) {
		a.pruneFrom = pruneFrom
	}
}

func NewMarkedEntity(id EntityId, canonicalTbl, valsTbl string, logger log.Logger, options ...MAOpts) (*MarkedEntity, error) {
	m := &MarkedEntity{
		ProtoEntity:  NewProto(id, nil, nil, logger),
		canonicalTbl: canonicalTbl,
		valsTbl:      valsTbl,
	}

	for _, opt := range options {
		opt(m)
	}

	if m.freezer == nil {
		panic("no freezer")
	}

	if m.builders == nil {
		// mapping num -> offset (ordinal map)
		builder := NewSimpleAccessorBuilder(NewAccessorArgs(true, false), id)
		m.builders = []AccessorIndexBuilder{builder}
	}

	return m, nil
}

func (a *MarkedEntity) encTs(ts Num) []byte {
	return ts.EncToBytes(!a.ts4Bytes)
}

func (a *MarkedEntity) combK(ts Num, hash []byte) []byte {
	// assuming hash is common.Hash which is 32 butes
	const HashBytes = 32
	k := make([]byte, 8+HashBytes)
	binary.BigEndian.PutUint64(k, uint64(ts))
	copy(k[8:], hash)
	return k
}

func (a *MarkedEntity) GetDb(num Num, hash []byte, tx kv.Tx) (Bytes, error) {
	if hash == nil {
		// find canonical hash
		canHash, err := tx.GetOne(a.canonicalTbl, a.encTs(num))
		if err != nil {
			return nil, err
		}
		hash = canHash
	}
	return tx.GetOne(a.valsTbl, a.combK(num, hash))
}

// rotx
type MarkerTx struct {
	*ProtoEntityTx
	a  *MarkedEntity
	id EntityId
}

func (m *MarkedEntity) BeginFilesRo() MarkedTxI {
	return &MarkerTx{
		ProtoEntityTx: m.ProtoEntity.BeginFilesRo(),
		a:             m,
		id:            m.a,
	}
}

func (r *MarkerTx) Get(entityNum Num, tx kv.Tx) (Bytes, error) {
	ap := r.a
	lastNum := ap.VisibleFilesMaxNum()
	var word []byte
	if entityNum <= lastNum {
		index := sort.Search(len(ap._visible), func(i int) bool {
			return ap._visible[i].src.FirstEntityNum() >= uint64(entityNum)
		})

		if index == -1 {
			return nil, fmt.Errorf("entity get error: snapshot expected but now found: (%s, %d)", ap.a.Name(), entityNum)
		}

		visible := ap._visible[index]
		g := visible.getter

		offset := visible.reader.OrdinalLookup(uint64(entityNum)) // TODO: allowed values
		g.Reset(offset)
		if g.HasNext() {
			word, _ = g.Next(word[:0])
			return word, nil
		}

		return nil, fmt.Errorf("entity get error: %s expected %d in snapshot %s but not found", r.id.Name(), entityNum, visible.src.decompressor.FileName())
	}

	return ap.GetDb(entityNum, nil, tx)
}

func (r *MarkerTx) GetNc(num Num, hash []byte, tx kv.Tx) (Bytes, error) {
	a := r.a
	key := a.combK(num, hash)
	return tx.GetOne(a.valsTbl, key)
}

func (r *MarkerTx) Put(num Num, hash []byte, value Bytes, tx kv.RwTx) error {
	// can then val
	a := r.a
	if err := tx.Append(a.canonicalTbl, a.encTs(num), hash); err != nil {
		return err
	}

	key := a.combK(num, hash)
	return tx.Put(a.valsTbl, key, value)
}

func (r *MarkerTx) Prune(ctx context.Context, to RootNum, limit uint64, tx kv.RwTx) error {
	a := r.a
	fromKey := a.pruneFrom
	fromKeyPrefix := a.encTs(fromKey)
	toKeyPrefix := a.encTs(Num(to) - 1)
	if err := ae.DeleteRangeFromTbl(a.canonicalTbl, fromKeyPrefix, toKeyPrefix, limit, tx); err != nil {
		return err
	}

	if err := ae.DeleteRangeFromTbl(a.valsTbl, fromKeyPrefix, toKeyPrefix, limit, tx); err != nil {
		return err
	}

	return nil
}

func (r *MarkerTx) Unwind(ctx context.Context, from RootNum, tx kv.RwTx) error {
	a := r.a
	fromKey := a.encTs(Num(from))
	return ae.DeleteRangeFromTbl(a.canonicalTbl, fromKey, nil, MaxUint64, tx)
}
