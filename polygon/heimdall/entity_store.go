package heimdall

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"sync"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
)

type entityStore[TEntity Entity] interface {
	Prepare(ctx context.Context) error
	Close()
	GetLastEntityId(ctx context.Context) (uint64, bool, error)
	GetLastEntity(ctx context.Context) (TEntity, error)
	GetEntity(ctx context.Context, id uint64) (TEntity, error)
	PutEntity(ctx context.Context, id uint64, entity TEntity) error
	FindByBlockNum(ctx context.Context, blockNum uint64) (TEntity, error)
	RangeFromId(ctx context.Context, startId uint64) ([]TEntity, error)
	RangeFromBlockNum(ctx context.Context, startBlockNum uint64) ([]TEntity, error)
}

type entityStoreImpl[TEntity Entity] struct {
	tx    kv.RwTx
	table string

	makeEntity      func() TEntity
	getLastEntityId func(ctx context.Context, tx kv.Tx) (uint64, bool, error)
	loadEntityBytes func(ctx context.Context, tx kv.Getter, id uint64) ([]byte, error)

	blockNumToIdIndexFactory func(ctx context.Context) (*RangeIndex, error)
	blockNumToIdIndex        *RangeIndex
	prepareOnce              sync.Once
}

func newEntityStore[TEntity Entity](
	tx kv.RwTx,
	table string,
	makeEntity func() TEntity,
	getLastEntityId func(ctx context.Context, tx kv.Tx) (uint64, bool, error),
	loadEntityBytes func(ctx context.Context, tx kv.Getter, id uint64) ([]byte, error),
	blockNumToIdIndexFactory func(ctx context.Context) (*RangeIndex, error),
) entityStore[TEntity] {
	return &entityStoreImpl[TEntity]{
		tx:    tx,
		table: table,

		makeEntity:      makeEntity,
		getLastEntityId: getLastEntityId,
		loadEntityBytes: loadEntityBytes,

		blockNumToIdIndexFactory: blockNumToIdIndexFactory,
	}
}

func (s *entityStoreImpl[TEntity]) Prepare(ctx context.Context) error {
	var err error
	s.prepareOnce.Do(func() {
		s.blockNumToIdIndex, err = s.blockNumToIdIndexFactory(ctx)
		if err != nil {
			return
		}
		iteratorFactory := func() (iter.KV, error) { return s.tx.Range(s.table, nil, nil) }
		err = buildBlockNumToIdIndex(ctx, s.blockNumToIdIndex, iteratorFactory, s.entityUnmarshalJSON)
	})
	return err
}

func (s *entityStoreImpl[TEntity]) Close() {
	s.blockNumToIdIndex.Close()
}

func (s *entityStoreImpl[TEntity]) GetLastEntityId(ctx context.Context) (uint64, bool, error) {
	return s.getLastEntityId(ctx, s.tx)
}

// Zero value of any type T
// https://stackoverflow.com/questions/70585852/return-default-value-for-generic-type)
// https://go.dev/ref/spec#The_zero_value
func Zero[T any]() T {
	var value T
	return value
}

func (s *entityStoreImpl[TEntity]) GetLastEntity(ctx context.Context) (TEntity, error) {
	id, ok, err := s.GetLastEntityId(ctx)
	if err != nil {
		return Zero[TEntity](), err
	}
	// not found
	if !ok {
		return Zero[TEntity](), nil
	}
	return s.GetEntity(ctx, id)
}

func entityStoreKey(id uint64) [8]byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], id)
	return key
}

func (s *entityStoreImpl[TEntity]) entityUnmarshalJSON(jsonBytes []byte) (TEntity, error) {
	entity := s.makeEntity()
	if err := json.Unmarshal(jsonBytes, entity); err != nil {
		return Zero[TEntity](), err
	}
	return entity, nil
}

func (s *entityStoreImpl[TEntity]) GetEntity(ctx context.Context, id uint64) (TEntity, error) {
	jsonBytes, err := s.loadEntityBytes(ctx, s.tx, id)
	if err != nil {
		return Zero[TEntity](), err
	}
	// not found
	if jsonBytes == nil {
		return Zero[TEntity](), nil
	}

	return s.entityUnmarshalJSON(jsonBytes)
}

func (s *entityStoreImpl[TEntity]) PutEntity(ctx context.Context, id uint64, entity TEntity) error {
	jsonBytes, err := json.Marshal(entity)
	if err != nil {
		return err
	}

	key := entityStoreKey(id)
	err = s.tx.Put(s.table, key[:], jsonBytes)
	if err != nil {
		return err
	}

	// update blockNumToIdIndex
	return s.blockNumToIdIndex.Put(ctx, entity.BlockNumRange(), id)
}

func (s *entityStoreImpl[TEntity]) FindByBlockNum(ctx context.Context, blockNum uint64) (TEntity, error) {
	id, err := s.blockNumToIdIndex.Lookup(ctx, blockNum)
	if err != nil {
		return Zero[TEntity](), err
	}
	// not found
	if id == 0 {
		return Zero[TEntity](), nil
	}

	return s.GetEntity(ctx, id)
}

func (s *entityStoreImpl[TEntity]) RangeFromId(_ context.Context, startId uint64) ([]TEntity, error) {
	startKey := entityStoreKey(startId)
	it, err := s.tx.Range(s.table, startKey[:], nil)
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

func (s *entityStoreImpl[TEntity]) RangeFromBlockNum(ctx context.Context, startBlockNum uint64) ([]TEntity, error) {
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
	iteratorFactory func() (iter.KV, error),
	entityUnmarshalJSON func([]byte) (TEntity, error),
) error {
	it, err := iteratorFactory()
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
