package state

import (
	"bytes"
	"context"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/kv"
)

// default freezer implementation for relational appendables (which have RootRelationI)
// implements Freezer interface
type SimpleRelationalFreezer struct {
	rel     RootRelationI[Num]
	valsTbl string
	coll    Collector
}

func (sf *SimpleRelationalFreezer) Freeze(ctx context.Context, from, to RootNum, db kv.RoDB) error {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_entityIdFrom, err := sf.rel.RootNum2Num(from, tx)
	if err != nil {
		return err
	}
	entityIdFrom := hexutil.EncodeTs(uint64(_entityIdFrom))

	_entityIdTo, err := sf.rel.RootNum2Num(to, tx)
	if err != nil {
		return err
	}
	entityIdTo := hexutil.EncodeTs(uint64(_entityIdTo))

	cursor, err := tx.Cursor(sf.valsTbl)
	if err != nil {
		return err
	}

	defer cursor.Close()

	// bytes.Compare assume big endianness
	for k, v, err := cursor.Seek(entityIdFrom); k != nil && bytes.Compare(k, entityIdTo) < 0; k, _, err = cursor.Next() {
		if err != nil {
			return err
		}
		if err := sf.coll(v); err != nil {
			return err
		}
	}

	return nil
}

func (sf *SimpleRelationalFreezer) SetCollector(coll Collector) {
	sf.coll = coll
}
