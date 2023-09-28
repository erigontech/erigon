package rawdb

import (
	"context"
	"encoding/json"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/borfinality/generics"
	"github.com/ledgerwatch/log/v3"
)

var (
	lastMilestone      = []byte("LastMilestone")
	lockFieldKey       = []byte("LockField")
	futureMilestoneKey = []byte("FutureMilestoneField")
)

type Finality struct {
	Block uint64
	Hash  libcommon.Hash
}

type LockField struct {
	Val    bool
	Block  uint64
	Hash   libcommon.Hash
	IdList map[string]struct{}
}

type FutureMilestoneField struct {
	Order []uint64
	List  map[uint64]libcommon.Hash
}

func (f *Finality) set(block uint64, hash libcommon.Hash) {
	f.Block = block
	f.Hash = hash
}

type Milestone struct {
	Finality
}

func (m *Milestone) clone() *Milestone {
	return &Milestone{}
}

func (m *Milestone) block() (uint64, libcommon.Hash) {
	return m.Block, m.Hash
}

func ReadFinality[T BlockFinality[T]](db kv.RwDB) (uint64, libcommon.Hash, error) {
	lastTV, key := getKey[T]()

	var data []byte

	err := db.View(context.Background(), func(tx kv.Tx) error {
		res, err := tx.GetOne(kv.BorFinality, key)
		data = res
		return err
	})

	if err != nil {
		return 0, libcommon.Hash{}, fmt.Errorf("%w: empty response for %s", err, string(key))
	}

	if len(data) == 0 {
		return 0, libcommon.Hash{}, fmt.Errorf("%w for %s", ErrEmptyLastFinality, string(key))
	}

	if err = json.Unmarshal(data, lastTV); err != nil {
		log.Error(fmt.Sprintf("Unable to unmarshal the last %s block number in database", string(key)), "err", err)

		return 0, libcommon.Hash{}, fmt.Errorf("%w(%v) for %s, data %v(%q)",
			ErrIncorrectFinality, err, string(key), data, string(data))
	}

	block, hash := lastTV.block()

	return block, hash, nil
}

func WriteLastFinality[T BlockFinality[T]](db kv.RwDB, block uint64, hash libcommon.Hash) error {
	lastTV, key := getKey[T]()

	lastTV.set(block, hash)

	enc, err := json.Marshal(lastTV)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to marshal the %s struct", string(key)), "err", err)

		return fmt.Errorf("%w: %v for %s struct", ErrIncorrectFinalityToStore, err, string(key))
	}

	err = db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(kv.BorFinality, key, enc)
	})

	if err != nil {
		log.Error(fmt.Sprintf("Failed to store the %s struct", string(key)), "err", err)

		return fmt.Errorf("%w: %v for %s struct", ErrDBNotResponding, err, string(key))
	}

	return nil
}

type BlockFinality[T any] interface {
	set(uint64, libcommon.Hash)
	clone() T
	block() (uint64, libcommon.Hash)
}

func getKey[T BlockFinality[T]]() (T, []byte) {
	lastT := generics.Empty[T]().clone()

	var key []byte

	switch any(lastT).(type) {
	case *Milestone:
		key = lastMilestone
	case *Checkpoint:
		key = lastCheckpoint
	}

	return lastT, key
}

func WriteLockField(db kv.RwDB, val bool, block uint64, hash libcommon.Hash, idListMap map[string]struct{}) error {

	lockField := LockField{
		Val:    val,
		Block:  block,
		Hash:   hash,
		IdList: idListMap,
	}

	key := lockFieldKey

	enc, err := json.Marshal(lockField)
	if err != nil {
		log.Error("Failed to marshal the lock field struct", "err", err)

		return fmt.Errorf("%w: %v for lock field struct", ErrIncorrectLockFieldToStore, err)
	}

	err = db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(kv.BorFinality, key, enc)
	})

	if err != nil {
		log.Error("Failed to store the lock field struct", "err", err)

		return fmt.Errorf("%w: %v for lock field struct", ErrDBNotResponding, err)
	}

	return nil
}

func ReadLockField(db kv.RwDB) (bool, uint64, libcommon.Hash, map[string]struct{}, error) {
	key := lockFieldKey
	lockField := LockField{}

	var data []byte
	err := db.View(context.Background(), func(tx kv.Tx) error {
		res, err := tx.GetOne(kv.BorFinality, key)
		data = res
		return err
	})

	if err != nil {
		return false, 0, libcommon.Hash{}, nil, fmt.Errorf("%w: empty response for lock field", err)
	}

	if len(data) == 0 {
		return false, 0, libcommon.Hash{}, nil, fmt.Errorf("%w for %s", ErrIncorrectLockField, string(key))
	}

	if err = json.Unmarshal(data, &lockField); err != nil {
		log.Error(fmt.Sprintf("Unable to unmarshal the lock field in database"), "err", err)

		return false, 0, libcommon.Hash{}, nil, fmt.Errorf("%w(%v) for lock field , data %v(%q)",
			ErrIncorrectLockField, err, data, string(data))
	}

	val, block, hash, idList := lockField.Val, lockField.Block, lockField.Hash, lockField.IdList

	return val, block, hash, idList, nil
}

func WriteFutureMilestoneList(db kv.RwDB, order []uint64, list map[uint64]libcommon.Hash) error {
	futureMilestoneField := FutureMilestoneField{
		Order: order,
		List:  list,
	}

	key := futureMilestoneKey

	enc, err := json.Marshal(futureMilestoneField)
	if err != nil {
		log.Error("Failed to marshal the future milestone field struct", "err", err)

		return fmt.Errorf("%w: %v for future milestone field struct", ErrIncorrectFutureMilestoneFieldToStore, err)
	}

	err = db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(kv.BorFinality, key, enc)
	})

	if err != nil {
		log.Error("Failed to store the future milestone field struct", "err", err)

		return fmt.Errorf("%w: %v for future milestone field struct", ErrDBNotResponding, err)
	}

	return nil
}

func ReadFutureMilestoneList(db kv.RwDB) ([]uint64, map[uint64]libcommon.Hash, error) {
	key := futureMilestoneKey
	futureMilestoneField := FutureMilestoneField{}

	var data []byte
	err := db.View(context.Background(), func(tx kv.Tx) error {
		res, err := tx.GetOne(kv.BorFinality, key)
		data = res
		return err
	})

	if err != nil {
		return nil, nil, fmt.Errorf("%w: empty response for future milestone field", err)
	}

	if len(data) == 0 {
		return nil, nil, fmt.Errorf("%w for %s", ErrIncorrectLockField, string(key))
	}

	if err = json.Unmarshal(data, &futureMilestoneField); err != nil {
		log.Error(fmt.Sprintf("Unable to unmarshal the future milestone field in database"), "err", err)

		return nil, nil, fmt.Errorf("%w(%v) for future milestone field, data %v(%q)",
			ErrIncorrectFutureMilestoneField, err, data, string(data))
	}

	order, list := futureMilestoneField.Order, futureMilestoneField.List

	return order, list, nil
}
