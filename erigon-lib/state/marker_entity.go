package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	ae "github.com/erigontech/erigon-lib/state/entity_extras"
)

const MaxUint64 = ^uint64(0)

/*
marked appendable has two tables

 1. canonicalMarkerTbl: stores num -> canonical hash

 2. valsTbl: maps `bigendian(num) + hash -> value`

    valsTbl stores canonical and non-canonical entires. Whereas canonicalMarkerTbl
    maps num -> canonical hash.

    marked entity Num is the same as the entity-set RootNum i.e.
    values of Num == values of RootNum
    headers, bodies, caplin blockbodies are marked entity.
*/
type Marker struct {
	*ProtoEntity

	canonicalTbl string
	valsTbl      string

	ts8Bytes bool // slots are encoded in 4 bytes; everything else in 8 bytes

	// pruning happens from this entity number
	// e.g. might not want to prune genesis block, in which case pruneFrom = 1
	pruneFrom Num
}

type MAOpts func(*Marker)

func (r *MAOpts) WithFreezer(freezer Freezer) MAOpts {
	return func(a *Marker) {
		a.freezer = freezer
	}
}

func (r *MAOpts) WithIndexBuilders(builders ...AccessorIndexBuilder) MAOpts {
	return func(a *Marker) {
		a.builders = builders
	}
}

func (r *MAOpts) WithTs8Bytes(ts8Bytes bool) MAOpts {
	return func(a *Marker) {
		a.ts8Bytes = ts8Bytes
	}
}

func (r *MAOpts) WithPruneFrom(pruneFrom Num) MAOpts {
	return func(a *Marker) {
		a.pruneFrom = pruneFrom
	}
}

func NewMarkedAppendable(id EntityId, canonicalTbl, valsTbl string, logger log.Logger, options ...MAOpts) (*Marker, error) {
	m := &Marker{
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
		salt, err := snaptype.GetIndexSalt(m.a.Dirs().Snap)
		if err != nil {
			return nil, err
		}
		builder := NewSimpleAccessorBuilder(NewAccessorArgs(true, false, false, salt), id)
		m.builders = []AccessorIndexBuilder{builder}
	}

	return m, nil
}

func (a *Marker) encTs(ts Num) []byte {
	return ts.EncToBytes(a.ts8Bytes)
}

func (a *Marker) combK(ts Num, hash []byte) []byte {
	// assuming hash is common.Hash which is 32 butes
	const HashBytes = 32
	k := make([]byte, 8+HashBytes)
	binary.BigEndian.PutUint64(k, uint64(ts))
	copy(k[8:], hash)
	return k
}

func (a *Marker) GetDb(num Num, hash []byte, tx kv.Tx) (Bytes, error) {
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
	a  *Marker
	id EntityId
}

func (m *Marker) BeginFilesRo() *MarkerTx {
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
