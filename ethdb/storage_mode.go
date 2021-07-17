package ethdb

import (
	"fmt"

	"github.com/ledgerwatch/erigon/common/dbutils"
)

type StorageMode struct {
	Initialised bool // Set when the values are initialised (not default)
	History     bool
	Receipts    bool
	TxIndex     bool
	CallTraces  bool
	TEVM        bool
	SnapshotLayout        bool
}

var DefaultStorageMode = StorageMode{
	Initialised: true,
	History:     true,
	Receipts:    true,
	TxIndex:     true,
	CallTraces:  true,
	TEVM:        false,
	SnapshotLayout:        false,
}

func (m StorageMode) ToString() string {
	if !m.Initialised {
		return "default"
	}
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
	if m.TEVM {
		modeString += "e"
	}
	return modeString
}

func StorageModeFromString(flags string) (StorageMode, error) {
	mode := StorageMode{}
	if flags == "default" {
		return mode, nil
	}
	mode.Initialised = true
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
		case 'e':
			mode.TEVM = true
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
	sm.Initialised = true

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

	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.StorageModeCallTraces)
	if err != nil {
		return StorageMode{}, err
	}
	sm.CallTraces = len(v) == 1 && v[0] == 1

	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.StorageModeTEVM)
	if err != nil {
		return StorageMode{}, err
	}
	sm.TEVM = len(v) == 1 && v[0] == 1
	return sm, nil
}

func OverrideStorageMode(db RwTx, sm StorageMode) error {
	var (
		err error
	)

	err = setMode(db, dbutils.StorageModeHistory, sm.History)
	if err != nil {
		return err
	}

	err = setMode(db, dbutils.StorageModeReceipts, sm.Receipts)
	if err != nil {
		return err
	}

	err = setMode(db, dbutils.StorageModeTxIndex, sm.TxIndex)
	if err != nil {
		return err
	}

	err = setMode(db, dbutils.StorageModeCallTraces, sm.CallTraces)
	if err != nil {
		return err
	}

	err = setMode(db, dbutils.StorageModeTEVM, sm.TEVM)
	if err != nil {
		return err
	}

	return nil
}

func SetStorageModeIfNotExist(db RwTx, sm StorageMode) error {
	var (
		err error
	)
	if !sm.Initialised {
		sm = DefaultStorageMode
	}

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

	err = setModeOnEmpty(db, dbutils.StorageModeTEVM, sm.TEVM)
	if err != nil {
		return err
	}

	return nil
}

func setMode(db RwTx, key []byte, currentValue bool) error {
	val := []byte{2}
	if currentValue {
		val = []byte{1}
	}
	if err := db.Put(dbutils.DatabaseInfoBucket, key, val); err != nil {
		return err
	}
	return nil
}

func setModeOnEmpty(db RwTx, key []byte, currentValue bool) error {
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
