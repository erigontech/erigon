package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
)

// default freezer implementation for relational forkables (which have RootRelationI)
// implements Freezer interface
type SimpleRelationalFreezer struct {
	rel     RootRelationI
	valsTbl string
}

func (sf *SimpleRelationalFreezer) Freeze(ctx context.Context, from, to RootNum, coll Collector, db kv.RoDB) error {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_entityNumFrom, err := sf.rel.RootNum2Num(from, tx)
	if err != nil {
		return err
	}
	entityNumFrom := hexutil.EncodeTs(uint64(_entityNumFrom))

	_entityNumTo, err := sf.rel.RootNum2Num(to, tx)
	if err != nil {
		return err
	}
	entityNumTo := hexutil.EncodeTs(uint64(_entityNumTo))

	cursor, err := tx.Cursor(sf.valsTbl)
	if err != nil {
		return err
	}

	defer cursor.Close()

	// bytes.Compare assume big endianness
	var metadata NumMetadata
	for k, v, err := cursor.Seek(entityNumFrom); k != nil && bytes.Compare(k, entityNumTo) < 0; k, v, err = cursor.Next() {
		if err != nil {
			return err
		}
		knum := Num(binary.BigEndian.Uint64(k))
		if metadata.count == 0 {
			metadata.first = knum
		}
		metadata.last = knum
		metadata.count++
		if err := coll.Add(k, v); err != nil {
			return err
		}
	}
	mbytes, err := metadata.Marshal()
	if err != nil {
		return err
	}
	coll.SetMetadata(mbytes)

	return nil
}

// a simple freezer for marked forkables

type SimpleMarkedFreezer struct {
	mfork *Forkable[MarkedTxI]
}

func (sf *SimpleMarkedFreezer) Freeze(ctx context.Context, from, to RootNum, coll Collector, db kv.RoDB) error {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	mfork := sf.mfork
	_entityNumFrom, err := mfork.rel.RootNum2Num(from, tx)
	if err != nil {
		return err
	}
	entityNumFrom := mfork.encTs(_entityNumFrom)

	_entityNumTo, err := mfork.rel.RootNum2Num(to, tx)
	if err != nil {
		return err
	}
	entityNumTo := mfork.encTs(_entityNumTo)

	cursor, err := tx.Cursor(mfork.canonicalTbl)
	if err != nil {
		return err
	}

	defer cursor.Close()

	vcursor, err := tx.Cursor(mfork.valsTbl)
	if err != nil {
		return err
	}
	defer vcursor.Close()

	combK := mfork.valsTblKey2

	// bytes.Compare assume big endianness
	var metadata NumMetadata
	for k, v, err := cursor.Seek(entityNumFrom); k != nil && bytes.Compare(k, entityNumTo) < 0; k, v, err = cursor.Next() {
		if err != nil {
			return err
		}
		knum := Num(binary.BigEndian.Uint64(k))
		if metadata.count == 0 {
			metadata.first = knum
		}
		metadata.last = knum
		metadata.count++
		valsKey := combK(k, v)
		_, value, err := vcursor.SeekExact(valsKey)
		if err != nil {
			return err
		}

		if err := coll.Add(k, value); err != nil {
			return err
		}
	}
	mbytes, err := metadata.Marshal()
	if err != nil {
		return err
	}
	coll.SetMetadata(mbytes)

	return nil

}

// / metadata for seg files
type NumMetadata struct {
	first Num
	last  Num
	count uint64 // num count in file
}

func (nm *NumMetadata) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, nm.first); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, nm.last); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, nm.count); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (nm *NumMetadata) Unmarshal(data []byte) error {
	if len(data) < 16 {
		return errors.New("data too short")
	}
	nm.first = Num(binary.BigEndian.Uint64(data[:8]))
	nm.last = Num(binary.BigEndian.Uint64(data[8:16]))
	nm.count = binary.BigEndian.Uint64(data[16:])
	return nil
}
