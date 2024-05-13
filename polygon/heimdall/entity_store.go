package heimdall

import (
	"context"
	"encoding/binary"
	"encoding/json"

	"github.com/ledgerwatch/erigon-lib/kv"
)

type entityStore interface {
	GetLastEntityId(ctx context.Context) (uint64, bool, error)
	PutEntity(ctx context.Context, id uint64, entity Entity) error
}

type entityStoreImpl struct {
	tx    kv.RwTx
	table string

	makeEntity      func() Entity
	getLastEntityId func(ctx context.Context, tx kv.Tx) (uint64, bool, error)
	loadEntityBytes func(ctx context.Context, tx kv.Getter, id uint64) ([]byte, error)
}

func newEntityStore(
	tx kv.RwTx,
	table string,
	makeEntity func() Entity,
	getLastEntityId func(ctx context.Context, tx kv.Tx) (uint64, bool, error),
	loadEntityBytes func(ctx context.Context, tx kv.Getter, id uint64) ([]byte, error),
) entityStore {
	return &entityStoreImpl{
		tx:    tx,
		table: table,

		makeEntity:      makeEntity,
		getLastEntityId: getLastEntityId,
		loadEntityBytes: loadEntityBytes,
	}
}

func (s *entityStoreImpl) GetLastEntityId(ctx context.Context) (uint64, bool, error) {
	return s.getLastEntityId(ctx, s.tx)
}

func (s *entityStoreImpl) GetEntity(ctx context.Context, id uint64) (Entity, error) {
	jsonBytes, err := s.loadEntityBytes(ctx, s.tx, id)
	if err != nil {
		return nil, err
	}

	entity := s.makeEntity()
	if err := json.Unmarshal(jsonBytes, entity); err != nil {
		return nil, err
	}

	return entity, nil
}

func (s *entityStoreImpl) PutEntity(_ context.Context, id uint64, entity Entity) error {
	jsonBytes, err := json.Marshal(entity)
	if err != nil {
		return err
	}

	var idBytes [8]byte
	binary.BigEndian.PutUint64(idBytes[:], id)

	return s.tx.Put(s.table, idBytes[:], jsonBytes)
}
