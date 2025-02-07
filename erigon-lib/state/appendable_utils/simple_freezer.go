package appendableutils

import (
	"bytes"
	"context"

	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/state"
)

// default freezer implementation for relational appendables (which have RootRelationI)
// implements state.Freezer interface
type SimpleRelationalFreezer struct {
	rel     state.RootRelationI
	valsTbl string
	coll    state.Collector
}

func (sf *SimpleRelationalFreezer) Freeze(ctx context.Context, from, to state.RootNum, tx kv.Tx) error {
	_entityIdFrom, err := sf.rel.RootNum2Id(from, tx)
	if err != nil {
		return err
	}
	entityIdFrom := hexutility.EncodeTs(uint64(_entityIdFrom))

	_entityIdTo, err := sf.rel.RootNum2Id(to, tx)
	if err != nil {
		return err
	}
	entityIdTo := hexutility.EncodeTs(uint64(_entityIdTo))

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

func (sf *SimpleRelationalFreezer) SetCollector(coll state.Collector) {
	sf.coll = coll
}
