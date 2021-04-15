package ethdb

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

type StorageMode struct {
	History  bool
	Receipts bool
	TxIndex  bool
}

var DefaultStorageMode = StorageMode{History: true, Receipts: true, TxIndex: true}

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
		default:
			return mode, fmt.Errorf("unexpected flag found: %c", flag)
		}
	}

	return mode, nil
}

func GetStorageModeFromDB(db KVGetter) (StorageMode, error) {
	var (
		sm  StorageMode
		v   []byte
		err error
	)
	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.StorageModeHistory)
	if err != nil {
		return StorageMode{}, err
	}
	sm.History = len(v) == 1 && v[0] == 1

	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.StorageModeReceipts)
	if err != nil {
		return StorageMode{}, err
	}
	sm.Receipts = len(v) == 1 && v[0] == 1

	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.StorageModeTxIndex)
	if err != nil {
		return StorageMode{}, err
	}
	sm.TxIndex = len(v) == 1 && v[0] == 1

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

	return nil
}

func setModeOnEmpty(db Database, key []byte, currentValue bool) error {
	mode, err := db.GetOne(dbutils.DatabaseInfoBucket, key)
	if err != nil {
		return err
	}
	if len(mode) == 0 {
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
