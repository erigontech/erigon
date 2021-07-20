package prune

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/params"
)

var DefaultMode = Mode{
	Initialised: true,
	History:     math.MaxUint64, // all off
	Receipts:    math.MaxUint64,
	TxIndex:     math.MaxUint64,
	CallTraces:  math.MaxUint64,
	Experiments: Experiments{}, // all off
}

type Experiments struct {
	TEVM bool
}

func FromCli(flags string, exactHistory, exactReceipts, exactTxIndex, exactCallTraces uint64, experiments []string) (Mode, error) {
	mode := DefaultMode
	if flags == "default" || flags == "disabled" {
		return DefaultMode, nil
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
			return DefaultMode, fmt.Errorf("unexpected flag found: %c", flag)
		}
	}

	if exactHistory > 0 {
		mode.History = Distance(exactHistory)
	}
	if exactReceipts > 0 {
		mode.Receipts = Distance(exactReceipts)
	}
	if exactTxIndex > 0 {
		mode.TxIndex = Distance(exactTxIndex)
	}
	if exactCallTraces > 0 {
		mode.CallTraces = Distance(exactCallTraces)
	}

	for _, ex := range experiments {
		switch ex {
		case "tevm":
			mode.Experiments.TEVM = true
		case "":
			// skip
		default:
			return DefaultMode, fmt.Errorf("unexpected experiment found: %s", ex)
		}
	}

	return mode, nil
}

func Get(db ethdb.KVGetter) (Mode, error) {
	prune := DefaultMode
	prune.Initialised = true

	v, err := db.GetOne(dbutils.DatabaseInfoBucket, dbutils.PruneDistanceHistory)
	if err != nil {
		return prune, err
	}
	if v != nil {
		prune.History = Distance(binary.BigEndian.Uint64(v))
	} else {
		prune.History = math.MaxUint64
	}
	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.PruneDistanceReceipts)
	if err != nil {
		return prune, err
	}
	if v != nil {
		prune.Receipts = Distance(binary.BigEndian.Uint64(v))
	} else {
		prune.Receipts = math.MaxUint64
	}
	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.PruneDistanceTxIndex)
	if err != nil {
		return prune, err
	}
	if v != nil {
		prune.TxIndex = Distance(binary.BigEndian.Uint64(v))
	} else {
		prune.TxIndex = math.MaxUint64
	}

	v, err = db.GetOne(dbutils.DatabaseInfoBucket, dbutils.PruneDistanceCallTraces)
	if err != nil {
		return prune, err
	}
	if v != nil {
		prune.CallTraces = Distance(binary.BigEndian.Uint64(v))
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

type Mode struct {
	Initialised bool // Set when the values are initialised (not default)
	History     Distance
	Receipts    Distance
	TxIndex     Distance
	CallTraces  Distance
	Experiments Experiments
}

// Distance amount of blocks to keep in DB
// but manual manipulation with such distance is very unsafe
// for example:
//    deleteUntil := currentStageProgress - pruningDistance
// may delete whole db - because of uint64 underflow when pruningDistance > currentStageProgress
type Distance uint64

func (p Distance) Enabled() bool { return p != math.MaxUint64 }

func (p Distance) PruneTo(stageHead uint64) uint64 {
	if p == 0 {
		panic("pruning distance were not set")
	}
	if uint64(p) > stageHead {
		return 0
	}
	return stageHead - uint64(p)
}

func (m Mode) ToString() string {
	if !m.Initialised {
		return "default"
	}
	modeString := ""
	if m.History.Enabled() {
		modeString += fmt.Sprintf("history=%d,", m.History)
	} else {
		modeString += "history=no,"
	}
	if m.Receipts.Enabled() {
		modeString += fmt.Sprintf("receipt=%d,", m.Receipts)
	} else {
		modeString += "receipt=no,"
	}
	if m.TxIndex.Enabled() {
		modeString += fmt.Sprintf("txindex=%d,", m.TxIndex)
	} else {
		modeString += "txindex=no,"
	}
	if m.CallTraces.Enabled() {
		modeString += fmt.Sprintf("calltrace=%d,", m.CallTraces)
	} else {
		modeString += "calltrace=no,"
	}
	if m.Experiments.TEVM {
		modeString += "experiments.tevm=enabled"
	} else {
		modeString += "experiments.tevm=disabled"
	}
	return modeString
}

func Override(db ethdb.RwTx, sm Mode) error {
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

func SetIfNotExist(db ethdb.GetPut, pm Mode) error {
	var (
		err error
	)
	if !pm.Initialised {
		pm = DefaultMode
	}

	err = setDistanceOnEmpty(db, dbutils.PruneDistanceHistory, pm.History)
	if err != nil {
		return err
	}

	err = setDistanceOnEmpty(db, dbutils.PruneDistanceReceipts, pm.Receipts)
	if err != nil {
		return err
	}

	err = setDistanceOnEmpty(db, dbutils.PruneDistanceTxIndex, pm.TxIndex)
	if err != nil {
		return err
	}

	err = setDistanceOnEmpty(db, dbutils.PruneDistanceCallTraces, pm.CallTraces)
	if err != nil {
		return err
	}

	err = setModeOnEmpty(db, dbutils.StorageModeTEVM, pm.Experiments.TEVM)
	if err != nil {
		return err
	}

	return nil
}

func setDistance(db ethdb.Putter, key []byte, distance Distance) error {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, uint64(distance))
	if err := db.Put(dbutils.DatabaseInfoBucket, key, v); err != nil {
		return err
	}
	return nil
}

func setDistanceOnEmpty(db ethdb.GetPut, key []byte, distance Distance) error {
	mode, err := db.GetOne(dbutils.DatabaseInfoBucket, key)
	if err != nil {
		return err
	}
	if len(mode) == 0 || binary.BigEndian.Uint64(mode) == math.MaxUint64 {
		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, uint64(distance))
		if err = db.Put(dbutils.DatabaseInfoBucket, key, v); err != nil {
			return err
		}
	}

	return nil
}

func setMode(db ethdb.RwTx, key []byte, currentValue bool) error {
	val := []byte{2}
	if currentValue {
		val = []byte{1}
	}
	if err := db.Put(dbutils.DatabaseInfoBucket, key, val); err != nil {
		return err
	}
	return nil
}

func setModeOnEmpty(db ethdb.GetPut, key []byte, currentValue bool) error {
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
