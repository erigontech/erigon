// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package heimdall

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/polygon/polygoncommon"

	"github.com/erigontech/erigon-lib/kv"
)

type RangeIndex interface {
	Lookup(ctx context.Context, blockNum uint64) (uint64, bool, error)
}

type RangeIndexFunc func(ctx context.Context, blockNum uint64) (uint64, bool, error)

func (f RangeIndexFunc) Lookup(ctx context.Context, blockNum uint64) (uint64, bool, error) {
	return f(ctx, blockNum)
}

type RangeIndexer interface {
	RangeIndex
	Put(ctx context.Context, r ClosedRange, id uint64) error
}

type TransactionalRangeIndexer interface {
	RangeIndexer
	WithTx(tx kv.Tx) RangeIndexer
}

type dbRangeIndex struct {
	db    *polygoncommon.Database
	table string
}

type txRangeIndex struct {
	*dbRangeIndex
	tx kv.Tx
}

func NewRangeIndex(db *polygoncommon.Database, table string) RangeIndex {
	return &dbRangeIndex{db, table}
}

func NewTxRangeIndex(db kv.RoDB, table string, tx kv.Tx) RangeIndex {
	return txRangeIndex{&dbRangeIndex{polygoncommon.AsDatabase(db.(kv.RwDB)), table}, tx}
}

func (i *dbRangeIndex) WithTx(tx kv.Tx) RangeIndexer {
	return txRangeIndex{i, tx}
}

func rangeIndexKey(blockNum uint64) [8]byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], blockNum)
	return key
}

func rangeIndexValue(id uint64) [8]byte {
	var value [8]byte
	binary.BigEndian.PutUint64(value[:], id)
	return value
}

func rangeIndexValueParse(value []byte) uint64 {
	return binary.BigEndian.Uint64(value)
}

// Put a mapping from a range to an id.
func (i *dbRangeIndex) Put(ctx context.Context, r ClosedRange, id uint64) error {
	tx, err := i.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := i.WithTx(tx).Put(ctx, r, id); err != nil {
		return err
	}

	return tx.Commit()
}

func (i txRangeIndex) Put(ctx context.Context, r ClosedRange, id uint64) error {
	key := rangeIndexKey(r.End)
	value := rangeIndexValue(id)
	tx, ok := i.tx.(kv.RwTx)

	if !ok {
		return fmt.Errorf("tx not writable")
	}

	return tx.Put(i.table, key[:], value[:])
}

// Lookup an id of a range by a blockNum within that range.
func (i *dbRangeIndex) Lookup(ctx context.Context, blockNum uint64) (uint64, bool, error) {
	var id uint64
	var ok bool

	err := i.db.View(ctx, func(tx kv.Tx) error {
		var err error
		id, ok, err = i.WithTx(tx).Lookup(ctx, blockNum)
		return err
	})
	return id, ok, err
}

func (i txRangeIndex) Lookup(ctx context.Context, blockNum uint64) (uint64, bool, error) {
	cursor, err := i.tx.Cursor(i.table)
	if err != nil {
		return 0, false, err
	}
	defer cursor.Close()

	key := rangeIndexKey(blockNum)
	_, value, err := cursor.Seek(key[:])
	if err != nil {
		return 0, false, err
	}
	// not found
	if value == nil {
		return 0, false, nil
	}

	id := rangeIndexValueParse(value)
	return id, true, err
}
