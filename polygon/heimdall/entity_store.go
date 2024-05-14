package heimdall

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"sync"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
)

type entityStore interface {
	Prepare(ctx context.Context) error
	Close()
	GetLastEntityId(ctx context.Context) (uint64, bool, error)
	GetEntity(ctx context.Context, id uint64) (Entity, error)
	PutEntity(ctx context.Context, id uint64, entity Entity) error
	FindByBlockNum(ctx context.Context, blockNum uint64) (Entity, error)
}

type entityStoreImpl struct {
	tx    kv.RwTx
	table string

	makeEntity      func() Entity
	getLastEntityId func(ctx context.Context, tx kv.Tx) (uint64, bool, error)
	loadEntityBytes func(ctx context.Context, tx kv.Getter, id uint64) ([]byte, error)

	blockNumToIdIndex *RangeIndex
	prepareOnce       sync.Once
}

func newEntityStore(
	tx kv.RwTx,
	table string,
	makeEntity func() Entity,
	getLastEntityId func(ctx context.Context, tx kv.Tx) (uint64, bool, error),
	loadEntityBytes func(ctx context.Context, tx kv.Getter, id uint64) ([]byte, error),
	blockNumToIdIndex *RangeIndex,
) entityStore {
	return &entityStoreImpl{
		tx:    tx,
		table: table,

		makeEntity:      makeEntity,
		getLastEntityId: getLastEntityId,
		loadEntityBytes: loadEntityBytes,

		blockNumToIdIndex: blockNumToIdIndex,
	}
}

func (s *entityStoreImpl) Prepare(ctx context.Context) error {
	var err error
	s.prepareOnce.Do(func() {
		iteratorFactory := func() (iter.KV, error) { return s.tx.Range(s.table, nil, nil) }
		err = buildBlockNumToIdIndex(ctx, s.blockNumToIdIndex, iteratorFactory, s.entityUnmarshalJSON)
	})
	return err
}

func (s *entityStoreImpl) Close() {
	s.blockNumToIdIndex.Close()
}

func (s *entityStoreImpl) GetLastEntityId(ctx context.Context) (uint64, bool, error) {
	return s.getLastEntityId(ctx, s.tx)
}

func entityStoreKey(id uint64) [8]byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], id)
	return key
}

func (s *entityStoreImpl) entityUnmarshalJSON(jsonBytes []byte) (Entity, error) {
	entity := s.makeEntity()
	if err := json.Unmarshal(jsonBytes, entity); err != nil {
		return nil, err
	}
	return entity, nil
}

func (s *entityStoreImpl) GetEntity(ctx context.Context, id uint64) (Entity, error) {
	jsonBytes, err := s.loadEntityBytes(ctx, s.tx, id)
	if err != nil {
		return nil, err
	}
	// not found
	if jsonBytes == nil {
		return nil, nil
	}

	return s.entityUnmarshalJSON(jsonBytes)
}

func (s *entityStoreImpl) PutEntity(ctx context.Context, id uint64, entity Entity) error {
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

func (s *entityStoreImpl) FindByBlockNum(ctx context.Context, blockNum uint64) (Entity, error) {
	id, err := s.blockNumToIdIndex.Lookup(ctx, blockNum)
	if err != nil {
		return nil, err
	}
	// not found
	if id == 0 {
		return nil, nil
	}

	return s.GetEntity(ctx, id)
}

func buildBlockNumToIdIndex(
	ctx context.Context,
	index *RangeIndex,
	iteratorFactory func() (iter.KV, error),
	entityUnmarshalJSON func([]byte) (Entity, error),
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
