package state

import (
	"bytes"
	"context"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/kv"
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
	for k, v, err := cursor.Seek(entityNumFrom); k != nil && bytes.Compare(k, entityNumTo) < 0; k, _, err = cursor.Next() {
		if err != nil {
			return err
		}
		if err := coll(v); err != nil {
			return err
		}
	}

	return nil
}
