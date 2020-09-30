package ethdb

import (
	"errors"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

type StorageMode struct {
	History    bool
	Receipts   bool
	TxIndex    bool
	CallTraces bool
}

var DefaultStorageMode = StorageMode{History: true, Receipts: true, TxIndex: true, CallTraces: false}

func (m StorageMode) ToString() string {
	modeString := ""
	if m.History {
		modeString += "h"
	}
	if m.Receipts {
		modeString += "r"
	}
	if m.TxIndex {
		modeString += "t"
	}
	if m.CallTraces {
		modeString += "c"
	}
	return modeString
}

func StorageModeFromString(flags string) (StorageMode, error) {
	mode := StorageMode{}
	for _, flag := range flags {
		switch flag {
		case 'h':
			mode.History = true
		case 'r':
			mode.Receipts = true
		case 't':
			mode.TxIndex = true
		case 'c':
			mode.CallTraces = true
		default:
			return mode, fmt.Errorf("unexpected flag found: %c", flag)
		}
	}

	return mode, nil
}

func GetStorageModeFromDB(db Database) (StorageMode, error) {
	var (
		sm  StorageMode
		v   []byte
		err error
	)
	v, err = db.Get(dbutils.DatabaseInfoBucket, dbutils.StorageModeHistory)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return StorageMode{}, err
	}
	sm.History = len(v) == 1 && v[0] == 1

	v, err = db.Get(dbutils.DatabaseInfoBucket, dbutils.StorageModeReceipts)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return StorageMode{}, err
	}
	sm.Receipts = len(v) == 1 && v[0] == 1

	v, err = db.Get(dbutils.DatabaseInfoBucket, dbutils.StorageModeTxIndex)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return StorageMode{}, err
	}
	sm.TxIndex = len(v) == 1 && v[0] == 1

	v, err = db.Get(dbutils.DatabaseInfoBucket, dbutils.StorageModeCallTraces)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return StorageMode{}, err
	}
	sm.CallTraces = len(v) == 1 && v[0] == 1

	return sm, nil
}

func SetStorageModeIfNotExist(db Database, sm StorageMode) error {
	var (
		err error
	)
	err = setModeOnEmpty(db, dbutils.StorageModeHistory, sm.History)
	if err != nil {
		return err
	}

	err = setModeOnEmpty(db, dbutils.StorageModeReceipts, sm.Receipts)
	if err != nil {
		return err
	}

	err = setModeOnEmpty(db, dbutils.StorageModeTxIndex, sm.TxIndex)
	if err != nil {
		return err
	}

	err = setModeOnEmpty(db, dbutils.StorageModeCallTraces, sm.CallTraces)
	if err != nil {
		return err
	}

	return nil
}

func setModeOnEmpty(db Database, key []byte, currentValue bool) error {
	_, err := db.Get(dbutils.DatabaseInfoBucket, key)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	if errors.Is(err, ErrKeyNotFound) {
		val := []byte{2}
		if currentValue {
			val = []byte{1}
		}
		if err = db.Put(dbutils.DatabaseInfoBucket, key, val); err != nil {
			return err
		}
	}

	return nil
}
