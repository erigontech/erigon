package ethdb

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/params"
)

var DefaultPruneMode = Prune{
	Initialised: true,
	History:     math.MaxUint64, // all off
	Receipts:    math.MaxUint64,
	TxIndex:     math.MaxUint64,
	CallTraces:  math.MaxUint64,
	Experiments: Experiments{}, // all off
}

func PruneFromString(flags string, experiments []string) (Prune, error) {
	mode := DefaultPruneMode
	if flags == "default" {
		return DefaultPruneMode, nil
	}
	mode.Initialised = true
	for _, flag := range flags {
		switch flag {
		case 'h':
			mode.History = params.FullImmutabilityThreshold
		case 'r':
			mode.Receipts = params.FullImmutabilityThreshold
		case 't':
			mode.TxIndex = params.FullImmutabilityThreshold
		case 'c':
			mode.CallTraces = params.FullImmutabilityThreshold
		default:
			return DefaultPruneMode, fmt.Errorf("unexpected flag found: %c", flag)
		}
	}

	for _, ex := range experiments {
		switch ex {
		case "tevm":
			mode.Experiments.TEVM = true
		default:
			return DefaultPruneMode, fmt.Errorf("unexpected experiment found: %s", ex)
		}
	}

	return mode, nil
}

func PruneMode(db KVGetter) (Prune, error) {
	prune := DefaultPruneMode
	prune.Initialised = true

	v, err := db.GetOne(dbutils.DatabaseInfoBucket, dbutils.PruneDistanceHistory)
	if err != nil {
		return prune, err
	}
	if v != nil {
		prune.History = PruneDistance(binary.BigEndian.Uint64(v))
	} else {
		prune.History = math.MaxUint64
	}
	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.PruneDistanceReceipts)
	if err != nil {
		return prune, err
	}
	if v != nil {
		prune.Receipts = PruneDistance(binary.BigEndian.Uint64(v))
	} else {
		prune.Receipts = math.MaxUint64
	}
	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.PruneDistanceTxIndex)
	if err != nil {
		return prune, err
	}
	if v != nil {
		prune.TxIndex = PruneDistance(binary.BigEndian.Uint64(v))
	} else {
		prune.TxIndex = math.MaxUint64
	}

	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.PruneDistanceCallTraces)
	if err != nil {
		return prune, err
	}
	if v != nil {
		prune.CallTraces = PruneDistance(binary.BigEndian.Uint64(v))
	} else {
		prune.CallTraces = math.MaxUint64
	}

	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.StorageModeTEVM)
	if err != nil {
		return prune, err
	}
	prune.Experiments.TEVM = len(v) == 1 && v[0] == 1
	return prune, nil
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

func OverridePruneMode(db RwTx, sm Prune) error {
	var (
		err error
	)

	err = setDistance(db, dbutils.PruneDistanceHistory, sm.History)
	if err != nil {
		return err
	}

	err = setDistance(db, dbutils.PruneDistanceReceipts, sm.Receipts)
	if err != nil {
		return err
	}

	err = setDistance(db, dbutils.PruneDistanceTxIndex, sm.TxIndex)
	if err != nil {
		return err
	}

	err = setDistance(db, dbutils.PruneDistanceCallTraces, sm.CallTraces)
	if err != nil {
		return err
	}

	err = setMode(db, dbutils.StorageModeTEVM, sm.Experiments.TEVM)
	if err != nil {
		return err
	}

	return nil
}

func SetPruneModeIfNotExist(db GetPut, sm Prune) error {
	var (
		err error
	)
	if !sm.Initialised {
		sm = DefaultPruneMode
	}

	err = setDistanceOnEmpty(db, dbutils.PruneDistanceHistory, sm.History)
	if err != nil {
		return err
	}

	err = setDistanceOnEmpty(db, dbutils.PruneDistanceReceipts, sm.Receipts)
	if err != nil {
		return err
	}

	err = setDistanceOnEmpty(db, dbutils.PruneDistanceTxIndex, sm.TxIndex)
	if err != nil {
		return err
	}

	err = setDistanceOnEmpty(db, dbutils.PruneDistanceCallTraces, sm.CallTraces)
	if err != nil {
		return err
	}

	err = setModeOnEmpty(db, dbutils.StorageModeTEVM, sm.Experiments.TEVM)
	if err != nil {
		return err
	}

	return nil
}

func setDistance(db Putter, key []byte, distance PruneDistance) error {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, uint64(distance))
	if err := db.Put(dbutils.DatabaseInfoBucket, key, v); err != nil {
		return err
	}
	return nil
}

func setDistanceOnEmpty(db GetPut, key []byte, distance PruneDistance) error {
	mode, err := db.GetOne(dbutils.DatabaseInfoBucket, key)
	if err != nil {
		return err
	}
	if len(mode) == 0 {
		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, uint64(distance))
		if err = db.Put(dbutils.DatabaseInfoBucket, key, v); err != nil {
			return err
		}
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

func setModeOnEmpty(db GetPut, key []byte, currentValue bool) error {
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
