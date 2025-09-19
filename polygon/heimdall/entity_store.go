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
	"encoding/json"
	"fmt"
	"sync"

	"github.com/erigontech/erigon-lib/common/generics"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

var databaseTablesCfg = kv.TableCfg{
	kv.BorCheckpoints:             {},
	kv.BorCheckpointEnds:          {},
	kv.BorMilestones:              {},
	kv.BorMilestoneEnds:           {},
	kv.BorSpans:                   {},
	kv.BorSpansIndex:              {},
	kv.BorProducerSelections:      {},
	kv.BorProducerSelectionsIndex: {},
}

//go:generate mockgen -typed=true -source=./entity_store.go -destination=./entity_store_mock.go -package=heimdall
type EntityStore[TEntity Entity] interface {
	Prepare(ctx context.Context) error
	Close()

	LastEntityId(ctx context.Context) (uint64, bool, error)
	LastFrozenEntityId() (uint64, bool, error)
	LastEntity(ctx context.Context) (TEntity, bool, error)
	Entity(ctx context.Context, id uint64) (TEntity, bool, error)
	PutEntity(ctx context.Context, id uint64, entity TEntity) error

	EntityIdFromBlockNum(ctx context.Context, blockNum uint64) (uint64, bool, error)
	RangeFromBlockNum(ctx context.Context, startBlockNum uint64) ([]TEntity, error)
	DeleteToBlockNum(ctx context.Context, unwindPoint uint64, limit int) (int, error)
	DeleteFromBlockNum(ctx context.Context, unwindPoint uint64) (int, error)

	RangeIndex() RangeIndex

	SnapType() snaptype.Type
}

type mdbxEntityStore[TEntity Entity] struct {
	db                *polygoncommon.Database
	table             string
	snapType          snaptype.Type
	makeEntity        func() TEntity
	blockNumToIdIndex RangeIndex
	prepareOnce       sync.Once
}

func newMdbxEntityStore[TEntity Entity](
	db *polygoncommon.Database,
	table string,
	snapType snaptype.Type,
	makeEntity func() TEntity,
	rangeIndex RangeIndex,
) *mdbxEntityStore[TEntity] {
	return &mdbxEntityStore[TEntity]{
		db:                db,
		table:             table,
		snapType:          snapType,
		makeEntity:        makeEntity,
		blockNumToIdIndex: rangeIndex,
	}
}

func (s *mdbxEntityStore[TEntity]) Prepare(ctx context.Context) error {
	var err error
	s.prepareOnce.Do(func() {
		err = s.db.OpenOnce(ctx)
		if err != nil {
			return
		}
	})
	return err
}

func (s *mdbxEntityStore[TEntity]) WithTx(tx kv.Tx) EntityStore[TEntity] {
	return txEntityStore[TEntity]{s, tx}
}

func (s *mdbxEntityStore[TEntity]) RangeIndex() RangeIndex {
	return s.blockNumToIdIndex
}

func (s *mdbxEntityStore[TEntity]) Close() {
}

func (s *mdbxEntityStore[TEntity]) SnapType() snaptype.Type {
	return s.snapType
}

func (s *mdbxEntityStore[TEntity]) LastEntityId(ctx context.Context) (uint64, bool, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()

	return txEntityStore[TEntity]{s, tx}.LastEntityId(ctx)
}

func (s *mdbxEntityStore[TEntity]) LastFrozenEntityId() (uint64, bool, error) {
	return 0, false, nil
}

func (s *mdbxEntityStore[TEntity]) LastEntity(ctx context.Context) (TEntity, bool, error) {
	id, ok, err := s.LastEntityId(ctx)
	if err != nil {
		return generics.Zero[TEntity](), false, err
	}
	// not found
	if !ok {
		return generics.Zero[TEntity](), false, nil
	}
	return s.Entity(ctx, id)
}

func entityStoreKey(id uint64) [8]byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], id)
	return key
}

func entityStoreKeyParse(key []byte) uint64 {
	return binary.BigEndian.Uint64(key)
}

func (s *mdbxEntityStore[TEntity]) entityUnmarshalJSON(jsonBytes []byte) (TEntity, error) {
	entity := s.makeEntity()
	if err := json.Unmarshal(jsonBytes, entity); err != nil {
		return generics.Zero[TEntity](), err
	}
	return entity, nil
}

func (s *mdbxEntityStore[TEntity]) Entity(ctx context.Context, id uint64) (TEntity, bool, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return generics.Zero[TEntity](), false, err
	}
	defer tx.Rollback()

	key := entityStoreKey(id)
	jsonBytes, err := tx.GetOne(s.table, key[:])
	if err != nil {
		return generics.Zero[TEntity](), false, err
	}
	// not found
	if jsonBytes == nil {
		return generics.Zero[TEntity](), false, nil
	}

	val, err := s.entityUnmarshalJSON(jsonBytes)
	return val, true, err
}

func (s *mdbxEntityStore[TEntity]) PutEntity(ctx context.Context, id uint64, entity TEntity) error {
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err = (txEntityStore[TEntity]{s, tx}).PutEntity(ctx, id, entity); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *mdbxEntityStore[TEntity]) RangeFromId(ctx context.Context, startId uint64) ([]TEntity, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	return txEntityStore[TEntity]{s, tx}.RangeFromId(ctx, startId)
}

func (s *mdbxEntityStore[TEntity]) RangeFromBlockNum(ctx context.Context, startBlockNum uint64) ([]TEntity, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	return txEntityStore[TEntity]{s, tx}.RangeFromBlockNum(ctx, startBlockNum)
}

func (s *mdbxEntityStore[TEntity]) EntityIdFromBlockNum(ctx context.Context, blockNum uint64) (uint64, bool, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()

	return txEntityStore[TEntity]{s, tx}.EntityIdFromBlockNum(ctx, blockNum)
}

func (s *mdbxEntityStore[TEntity]) DeleteToBlockNum(ctx context.Context, unwindPoint uint64, limit int) (int, error) {
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	deleted, err := (txEntityStore[TEntity]{s, tx}).DeleteToBlockNum(ctx, unwindPoint, limit)

	if err != nil {
		return deleted, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return deleted, nil
}

func (s *mdbxEntityStore[TEntity]) DeleteFromBlockNum(ctx context.Context, unwindPoint uint64) (int, error) {
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	deleted, err := (txEntityStore[TEntity]{s, tx}).DeleteFromBlockNum(ctx, unwindPoint)

	if err != nil {
		return deleted, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return deleted, nil
}

type txEntityStore[TEntity Entity] struct {
	*mdbxEntityStore[TEntity]
	tx kv.Tx
}

func (s txEntityStore[TEntity]) Prepare(ctx context.Context) error {
	return nil
}

func (s txEntityStore[TEntity]) Close() {

}

func (s txEntityStore[TEntity]) LastEntityId(ctx context.Context) (uint64, bool, error) {
	cursor, err := s.tx.Cursor(s.table)
	if err != nil {
		return 0, false, err
	}
	defer cursor.Close()

	lastKey, _, err := cursor.Last()
	if err != nil {
		return 0, false, err
	}
	// not found
	if lastKey == nil {
		return 0, false, nil
	}

	return entityStoreKeyParse(lastKey), true, nil
}

func (s txEntityStore[TEntity]) Entity(ctx context.Context, id uint64) (TEntity, bool, error) {
	key := entityStoreKey(id)
	jsonBytes, err := s.tx.GetOne(s.table, key[:])
	if err != nil {
		return generics.Zero[TEntity](), false, err
	}
	// not found
	if jsonBytes == nil {
		return generics.Zero[TEntity](), false, nil
	}

	val, err := s.entityUnmarshalJSON(jsonBytes)
	return val, true, err

}

func (s txEntityStore[TEntity]) PutEntity(ctx context.Context, id uint64, entity TEntity) error {
	tx, ok := s.tx.(kv.RwTx)

	if !ok {
		return fmt.Errorf("put entity: %s needs an RwTx", s.table)
	}

	jsonBytes, err := json.Marshal(entity)
	if err != nil {
		return err
	}

	key := entityStoreKey(id)
	if err = tx.Put(s.table, key[:], jsonBytes); err != nil {
		return err
	}

	indexer, ok := s.blockNumToIdIndex.(RangeIndexer)
	if !ok {
		return nil
	}

	if txIndexer, ok := s.blockNumToIdIndex.(TransactionalRangeIndexer); ok {
		indexer = txIndexer.WithTx(tx)
	}

	return indexer.Put(ctx, entity.BlockNumRange(), id)
}

func (s txEntityStore[TEntity]) RangeFromId(ctx context.Context, startId uint64) ([]TEntity, error) {
	startKey := entityStoreKey(startId)
	it, err := s.tx.Range(s.table, startKey[:], nil, order.Asc, kv.Unlim)
	if err != nil {
		return nil, err
	}

	var entities []TEntity
	for it.HasNext() {
		_, jsonBytes, err := it.Next()
		if err != nil {
			return nil, err
		}

		entity, err := s.entityUnmarshalJSON(jsonBytes)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

func (s txEntityStore[TEntity]) RangeFromBlockNum(ctx context.Context, startBlockNum uint64) ([]TEntity, error) {
	id, ok, err := s.EntityIdFromBlockNum(ctx, startBlockNum)
	if err != nil {
		return nil, err
	}
	// not found
	if !ok {
		return nil, nil
	}

	return s.RangeFromId(ctx, id)
}

func (s txEntityStore[TEntity]) EntityIdFromBlockNum(ctx context.Context, blockNum uint64) (uint64, bool, error) {
	indexer := s.blockNumToIdIndex

	if txIndexer, ok := indexer.(TransactionalRangeIndexer); ok {
		indexer = txIndexer.WithTx(s.tx)
	}

	return indexer.Lookup(ctx, blockNum)
}

func (s txEntityStore[TEntity]) DeleteToBlockNum(ctx context.Context, unwindPoint uint64, limit int) (int, error) {
	tx, ok := s.tx.(kv.RwTx)

	if !ok {
		return 0, fmt.Errorf("delete %s to %d needs an RwTx", s.table, unwindPoint)
	}

	cursor, err := tx.RwCursor(s.table)
	if err != nil {
		return 0, err
	}

	defer cursor.Close()
	lastEntityToKeep, ok, err := s.EntityIdFromBlockNum(ctx, unwindPoint)
	if err != nil {
		return 0, err
	}

	if !ok {
		return 0, nil
	}

	var deleted int
	for k, _, err := cursor.Next(); err == nil && k != nil; k, _, err = cursor.Next() {
		if entityStoreKeyParse(k) >= lastEntityToKeep {
			break
		}

		if err = cursor.DeleteCurrent(); err != nil {
			return deleted, err
		}
		deleted++
		if limit > 0 && deleted == limit {
			break
		}
	}
	return deleted, err
}

func (s txEntityStore[TEntity]) DeleteFromBlockNum(ctx context.Context, unwindPoint uint64) (int, error) {
	tx, ok := s.tx.(kv.RwTx)

	if !ok {
		return 0, fmt.Errorf("uwind %s to %d needs an RwTx", s.table, unwindPoint)
	}

	cursor, err := tx.RwCursor(s.table)
	if err != nil {
		return 0, err
	}

	defer cursor.Close()
	firstEntityToDelete, ok, err := s.EntityIdFromBlockNum(ctx, unwindPoint+1)
	if err != nil {
		return 0, err
	}

	if !ok {
		return 0, nil
	}

	var entityKey = entityStoreKey(firstEntityToDelete)
	var k []byte
	var deleted int
	for k, _, err = cursor.Seek(entityKey[:]); err == nil && k != nil; k, _, err = cursor.Next() {
		if err = cursor.DeleteCurrent(); err != nil {
			return deleted, err
		}
		deleted++
	}

	return deleted, err
}
