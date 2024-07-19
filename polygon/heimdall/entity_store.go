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
	"sync"

	"github.com/erigontech/erigon-lib/common/generics"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

var databaseTablesCfg = kv.TableCfg{
	kv.BorCheckpoints: {},
	kv.BorMilestones:  {},
	kv.BorSpans:       {},
}

type EntityStore[TEntity Entity] interface {
	Prepare(ctx context.Context) error
	Close()
	GetLastEntityId(ctx context.Context) (uint64, bool, error)
	GetLastEntity(ctx context.Context) (TEntity, error)
	GetEntity(ctx context.Context, id uint64) (TEntity, error)
	PutEntity(ctx context.Context, id uint64, entity TEntity) error
	RangeFromBlockNum(ctx context.Context, startBlockNum uint64) ([]TEntity, error)
}

type RangeIndexFactory func(ctx context.Context) (*RangeIndex, error)

type mdbxEntityStore[TEntity Entity] struct {
	db    *polygoncommon.Database
	label kv.Label
	table string

	makeEntity func() TEntity

	blockNumToIdIndexFactory RangeIndexFactory
	blockNumToIdIndex        *RangeIndex
	prepareOnce              sync.Once
}

func newMdbxEntityStore[TEntity Entity](
	db *polygoncommon.Database,
	label kv.Label,
	table string,
	makeEntity func() TEntity,
	blockNumToIdIndexFactory RangeIndexFactory,
) *mdbxEntityStore[TEntity] {
	return &mdbxEntityStore[TEntity]{
		db:    db,
		label: label,
		table: table,

		makeEntity: makeEntity,

		blockNumToIdIndexFactory: blockNumToIdIndexFactory,
	}
}

func (s *mdbxEntityStore[TEntity]) Prepare(ctx context.Context) error {
	var err error
	s.prepareOnce.Do(func() {
		err = s.db.OpenOnce(ctx, s.label, databaseTablesCfg)
		if err != nil {
			return
		}
		s.blockNumToIdIndex, err = s.blockNumToIdIndexFactory(ctx)
		if err != nil {
			return
		}
		iteratorFactory := func(tx kv.Tx) (stream.KV, error) { return tx.Range(s.table, nil, nil) }
		err = buildBlockNumToIdIndex(ctx, s.blockNumToIdIndex, s.db.BeginRo, iteratorFactory, s.entityUnmarshalJSON)
	})
	return err
}

func (s *mdbxEntityStore[TEntity]) Close() {
	s.blockNumToIdIndex.Close()
}

func (s *mdbxEntityStore[TEntity]) GetLastEntityId(ctx context.Context) (uint64, bool, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()

	cursor, err := tx.Cursor(s.table)
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

func (s *mdbxEntityStore[TEntity]) GetLastEntity(ctx context.Context) (TEntity, error) {
	id, ok, err := s.GetLastEntityId(ctx)
	if err != nil {
		return generics.Zero[TEntity](), err
	}
	// not found
	if !ok {
		return generics.Zero[TEntity](), nil
	}
	return s.GetEntity(ctx, id)
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

func (s *mdbxEntityStore[TEntity]) GetEntity(ctx context.Context, id uint64) (TEntity, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return generics.Zero[TEntity](), err
	}
	defer tx.Rollback()

	key := entityStoreKey(id)
	jsonBytes, err := tx.GetOne(s.table, key[:])
	if err != nil {
		return generics.Zero[TEntity](), err
	}
	// not found
	if jsonBytes == nil {
		return generics.Zero[TEntity](), nil
	}

	return s.entityUnmarshalJSON(jsonBytes)
}

func (s *mdbxEntityStore[TEntity]) PutEntity(ctx context.Context, id uint64, entity TEntity) error {
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	jsonBytes, err := json.Marshal(entity)
	if err != nil {
		return err
	}

	key := entityStoreKey(id)
	if err = tx.Put(s.table, key[:], jsonBytes); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}

	// update blockNumToIdIndex
	return s.blockNumToIdIndex.Put(ctx, entity.BlockNumRange(), id)
}

func (s *mdbxEntityStore[TEntity]) RangeFromId(ctx context.Context, startId uint64) ([]TEntity, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	startKey := entityStoreKey(startId)
	it, err := tx.Range(s.table, startKey[:], nil)
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

func (s *mdbxEntityStore[TEntity]) RangeFromBlockNum(ctx context.Context, startBlockNum uint64) ([]TEntity, error) {
	id, err := s.blockNumToIdIndex.Lookup(ctx, startBlockNum)
	if err != nil {
		return nil, err
	}
	// not found
	if id == 0 {
		return nil, nil
	}

	return s.RangeFromId(ctx, id)
}

func buildBlockNumToIdIndex[TEntity Entity](
	ctx context.Context,
	index *RangeIndex,
	txFactory func(context.Context) (kv.Tx, error),
	iteratorFactory func(tx kv.Tx) (stream.KV, error),
	entityUnmarshalJSON func([]byte) (TEntity, error),
) error {
	tx, err := txFactory(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	it, err := iteratorFactory(tx)
	if err != nil {
		return err
	}
	defer it.Close()

	for it.HasNext() {
		_, jsonBytes, err := it.Next()
		if err != nil {
			return err
		}

		entity, err := entityUnmarshalJSON(jsonBytes)
		if err != nil {
			return err
		}

		if err = index.Put(ctx, entity.BlockNumRange(), entity.RawId()); err != nil {
			return err
		}
	}

	return nil
}
