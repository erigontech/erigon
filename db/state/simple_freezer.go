package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
)

// default freezer implementation for relational forkables (which have RootRelationI)
// implements Freezer interface
type SimpleRelationalFreezer struct {
	rel     RootRelationI
	valsTbl string
}

func (sf *SimpleRelationalFreezer) Freeze(ctx context.Context, from, to RootNum, coll Collector, db kv.RoDB) (metadata NumMetadata, err error) {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback()
	_entityNumFrom, err := sf.rel.RootNum2Num(from, tx)
	if err != nil {
		return
	}
	entityNumFrom := hexutil.EncodeTs(uint64(_entityNumFrom))

	_entityNumTo, err := sf.rel.RootNum2Num(to, tx)
	if err != nil {
		return
	}
	entityNumTo := hexutil.EncodeTs(uint64(_entityNumTo))

	cursor, err := tx.Cursor(sf.valsTbl)
	if err != nil {
		return
	}

	defer cursor.Close()

	// bytes.Compare assume big endianness
	for k, v, err := cursor.Seek(entityNumFrom); k != nil && bytes.Compare(k, entityNumTo) < 0; k, v, err = cursor.Next() {
		if err != nil {
			return metadata, err
		}
		knum := Num(binary.BigEndian.Uint64(k))
		if metadata.Count == 0 {
			metadata.First = knum
		}
		metadata.Last = knum
		metadata.Count++
		if err := coll.Add(k, v); err != nil {
			return metadata, err
		}
	}

	return
}

// a simple freezer for marked forkables

type SimpleMarkedFreezer struct {
	mfork *Forkable[MarkedTxI]
}

func (sf *SimpleMarkedFreezer) Freeze(ctx context.Context, from, to RootNum, coll Collector, db kv.RoDB) (metadata NumMetadata, err error) {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback()
	mfork := sf.mfork
	_entityNumFrom, err := mfork.rel.RootNum2Num(from, tx)
	if err != nil {
		return
	}
	entityNumFrom := mfork.encTs(_entityNumFrom)

	_entityNumTo, err := mfork.rel.RootNum2Num(to, tx)
	if err != nil {
		return
	}
	entityNumTo := mfork.encTs(_entityNumTo)

	cursor, err := tx.Cursor(mfork.canonicalTbl)
	if err != nil {
		return
	}

	defer cursor.Close()

	vcursor, err := tx.Cursor(mfork.valsTbl)
	if err != nil {
		return
	}
	defer vcursor.Close()

	combK := mfork.valsTblKey2

	// bytes.Compare assume big endianness
	for k, v, err := cursor.Seek(entityNumFrom); k != nil && bytes.Compare(k, entityNumTo) < 0; k, v, err = cursor.Next() {
		if err != nil {
			return metadata, err
		}
		knum := Num(binary.BigEndian.Uint64(k))
		if metadata.Count == 0 {
			metadata.First = knum
		}
		metadata.Last = knum
		metadata.Count++
		valsKey := combK(k, v)
		_, value, err := vcursor.SeekExact(valsKey)
		if err != nil {
			return metadata, err
		}

		if err := coll.Add(k, value); err != nil {
			return metadata, err
		}
	}
	return
}

// / metadata for seg files
type NumMetadata struct {
	First Num
	Last  Num
	Count uint64 // num count in file
}

func (nm *NumMetadata) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, nm.First); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, nm.Last); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, nm.Count); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (nm *NumMetadata) Unmarshal(data []byte) error {
	if len(data) < 16 {
		return errors.New("num metadata too short")
	}
	nm.First = Num(binary.BigEndian.Uint64(data[:8]))
	nm.Last = Num(binary.BigEndian.Uint64(data[8:16]))
	nm.Count = binary.BigEndian.Uint64(data[16:])
	return nil
}
