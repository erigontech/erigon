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
	"errors"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

type RangeIndex interface {
	Lookup(ctx context.Context, blockNum uint64) (uint64, bool, error)
	Last(ctx context.Context) (uint64, bool, error)
}

type TransactionalRangeIndexer interface {
	RangeIndexer
	WithTx(tx kv.Tx) RangeIndexer
}

type RangeIndexFunc func(ctx context.Context, blockNum uint64) (uint64, bool, error)

func (f RangeIndexFunc) Lookup(ctx context.Context, blockNum uint64) (uint64, bool, error) {
	return f(ctx, blockNum)
}

type RangeIndexer interface {
	RangeIndex
	Put(ctx context.Context, r ClosedRange, id uint64) error
	GetIDsBetween(ctx context.Context, blockFrom, blockTo uint64) ([]uint64, bool, error)
}

type dbRangeIndex struct {
	db    *polygoncommon.Database
	table string
}

type txRangeIndex struct {
	*dbRangeIndex
	tx kv.Tx
}

func NewRangeIndex(db *polygoncommon.Database, table string) *dbRangeIndex {
	return &dbRangeIndex{db, table}
}

func NewTxRangeIndex(db kv.RoDB, table string, tx kv.Tx) *txRangeIndex {
	return &txRangeIndex{&dbRangeIndex{polygoncommon.AsDatabase(db.(kv.RwDB)), table}, tx}
}

func (i *dbRangeIndex) WithTx(tx kv.Tx) RangeIndexer {
	return &txRangeIndex{i, tx}
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

func rangeIndexKeyParse(key []byte) uint64 {
	return binary.BigEndian.Uint64(key)
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

func (i *txRangeIndex) Put(ctx context.Context, r ClosedRange, id uint64) error {
	key := rangeIndexKey(r.End)
	value := rangeIndexValue(id)
	tx, ok := i.tx.(kv.RwTx)

	if !ok {
		return errors.New("tx not writable")
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

func (i *dbRangeIndex) Last(ctx context.Context) (uint64, bool, error) {
	var lastKey uint64
	var ok bool

	err := i.db.View(ctx, func(tx kv.Tx) error {
		var err error
		lastKey, ok, err = i.WithTx(tx).Last(ctx)
		return err
	})
	return lastKey, ok, err
}

func (i *txRangeIndex) Lookup(ctx context.Context, blockNum uint64) (uint64, bool, error) {
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

// last key in the index
func (i *txRangeIndex) Last(ctx context.Context) (uint64, bool, error) {
	cursor, err := i.tx.Cursor(i.table)
	if err != nil {
		return 0, false, err
	}
	defer cursor.Close()
	key, value, err := cursor.Last()
	if err != nil {
		return 0, false, err
	}

	if value == nil || key == nil {
		return 0, false, nil
	}

	lastKey := rangeIndexKeyParse(key)
	return lastKey, true, nil
}

// Lookup ids for the given range [blockFrom, blockTo). Return boolean which checks if the result is reliable to use, because
// heimdall data can be not published yet for [blockFrom, blockTo), in that case boolean OK will be false
func (i *dbRangeIndex) GetIDsBetween(ctx context.Context, blockFrom, blockTo uint64) ([]uint64, bool, error) {
	var ids []uint64
	var ok bool

	err := i.db.View(ctx, func(tx kv.Tx) error {
		var err error
		ids, ok, err = i.WithTx(tx).GetIDsBetween(ctx, blockFrom, blockTo)
		return err
	})
	return ids, ok, err
}

func (i *txRangeIndex) GetIDsBetween(ctx context.Context, blockFrom, blockTo uint64) ([]uint64, bool, error) {
	cursor, err := i.tx.Cursor(i.table)
	if err != nil {
		return nil, false, err
	}
	defer cursor.Close()

	key := rangeIndexKey(blockFrom)
	k, value, err := cursor.Seek(key[:])
	if err != nil {
		return nil, false, err
	}

	var ids []uint64

	for k != nil {
		intervalBlock := rangeIndexKeyParse(k)
		if intervalBlock >= blockTo {
			return ids, true, nil
		}

		ids = append(ids, rangeIndexValueParse(value))

		k, value, err = cursor.Next()
		if err != nil {
			return nil, false, err
		}
	}

	return ids, false, nil
}
