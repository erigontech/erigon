package ethdb

import (
	"fmt"
	"math"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/params"
)

type StorageMode struct {
	Initialised bool // Set when the values are initialised (not default)
	History     bool
	Receipts    bool
	TxIndex     bool
	CallTraces  bool
	TEVM        bool
}

var DefaultStorageMode = StorageMode{
	Initialised: true,
	History:     true,
	Receipts:    true,
	TxIndex:     true,
	CallTraces:  true,
	TEVM:        false,
}

var DefaultPruneMode = Prune{
	Initialised: true,
	History:     math.MaxUint64,
	Receipts:    math.MaxUint64,
	TxIndex:     math.MaxUint64,
	CallTraces:  math.MaxUint64,
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

func PruneModeFromString(flags string) (Prune, error) {
	mode := StorageMode{}
	if flags == "default" {
		return DefaultPruneMode, nil
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
			return DefaultPruneMode, fmt.Errorf("unexpected flag found: %c", flag)
		}
	}

	return invert(mode), nil
}

func GetPruneModeFromDB(db KVGetter) (Prune, error) {
	var (
		sm  StorageMode
		v   []byte
		err error
	)
	sm.Initialised = true

	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.StorageModeHistory)
	if err != nil {
		return Prune{}, err
	}
	sm.History = len(v) == 1 && v[0] == 1
	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.StorageModeReceipts)
	if err != nil {
		return Prune{}, err
	}
	sm.Receipts = len(v) == 1 && v[0] == 1

	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.StorageModeTxIndex)
	if err != nil {
		return Prune{}, err
	}
	sm.TxIndex = len(v) == 1 && v[0] == 1

	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.StorageModeCallTraces)
	if err != nil {
		return Prune{}, err
	}
	sm.CallTraces = len(v) == 1 && v[0] == 1

	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.StorageModeTEVM)
	if err != nil {
		return Prune{}, err
	}
	sm.TEVM = len(v) == 1 && v[0] == 1
	return invert(sm), nil
}

type Prune struct {
	Initialised bool // Set when the values are initialised (not default)
	History     PruneDistance
	Receipts    PruneDistance
	TxIndex     PruneDistance
	CallTraces  PruneDistance
	Experiments Experiments
}

// PruneDistance amount of blocks to keep in DB
// but manual manipulation with such distance is very unsafe
// for example:
//    deleteUntil := currentStageProgress - pruningDistance
// may delete whole db - because of uint64 underflow when pruningDistance > currentStageProgress
type PruneDistance uint64

func (p PruneDistance) Enabled() bool { return p != math.MaxUint64 }
func (p PruneDistance) PruneTo(stageHead uint64) uint64 {
	if p == 0 {
		panic("pruning distance were not set")
	}
	if uint64(p) > stageHead {
		return 0
	}
	return stageHead - uint64(p)
}

func invert(mode StorageMode) Prune {
	pm := Prune{
		Initialised: mode.Initialised,
	}
	if !mode.History {
		pm.History = params.FullImmutabilityThreshold
	} else {
		pm.History = math.MaxUint64
	}
	if !mode.Receipts {
		pm.Receipts = params.FullImmutabilityThreshold
	} else {
		pm.Receipts = math.MaxUint64
	}
	if !mode.TxIndex {
		pm.TxIndex = params.FullImmutabilityThreshold
	} else {
		pm.TxIndex = math.MaxUint64
	}
	if !mode.CallTraces {
		pm.CallTraces = params.FullImmutabilityThreshold
	} else {
		pm.CallTraces = math.MaxUint64
	}
	pm.Experiments.TEVM = mode.TEVM
	return pm
}

func invertBack(mode Prune) StorageMode {
	return StorageMode{
		Initialised: mode.Initialised,
		History:     !mode.History.Enabled(),
		Receipts:    !mode.Receipts.Enabled(),
		TxIndex:     !mode.TxIndex.Enabled(),
		CallTraces:  !mode.CallTraces.Enabled(),
		TEVM:        mode.Experiments.TEVM,
	}
}

func (m Prune) ToString() string {
	if !m.Initialised {
		return "default"
	}
	modeString := ""
	if m.History.Enabled() {
		modeString += "h"
	}
	if m.Receipts.Enabled() {
		modeString += "r"
	}
	if m.TxIndex.Enabled() {
		modeString += "t"
	}
	if m.CallTraces.Enabled() {
		modeString += "c"
	}
	if m.Experiments.TEVM {
		modeString += "e"
	}
	return modeString
}

func OverridePruneMode(db RwTx, pm Prune) error {
	sm := invertBack(pm)
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

func SetPruneModeIfNotExist(db RwTx, pm Prune) error {
	sm := invertBack(pm)
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

type Experiments struct {
	TEVM bool
}
