package prune

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/ledgerwatch/erigon-lib/kv"
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

func Get(db kv.Getter) (Mode, error) {
	prune := DefaultMode
	prune.Initialised = true

	v, err := db.GetOne(kv.DatabaseInfo, kv.PruneDistanceHistory)
	if err != nil {
		return prune, err
	}
	if v != nil {
		prune.History = Distance(binary.BigEndian.Uint64(v))
	} else {
		prune.History = math.MaxUint64
	}
	v, err = db.GetOne(kv.DatabaseInfo, kv.PruneDistanceReceipts)
	if err != nil {
		return prune, err
	}
	if v != nil {
		prune.Receipts = Distance(binary.BigEndian.Uint64(v))
	} else {
		prune.Receipts = math.MaxUint64
	}
	v, err = db.GetOne(kv.DatabaseInfo, kv.PruneDistanceTxIndex)
	if err != nil {
		return prune, err
	}
	if v != nil {
		prune.TxIndex = Distance(binary.BigEndian.Uint64(v))
	} else {
		prune.TxIndex = math.MaxUint64
	}

	v, err = db.GetOne(kv.DatabaseInfo, kv.PruneDistanceCallTraces)
	if err != nil {
		return prune, err
	}
	if v != nil {
		prune.CallTraces = Distance(binary.BigEndian.Uint64(v))
	} else {
		prune.CallTraces = math.MaxUint64
	}

	v, err = db.GetOne(kv.DatabaseInfo, kv.StorageModeTEVM)
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

func (m Mode) String() string {
	if !m.Initialised {
		return "default"
	}
	long := ""
	short := "--prune="
	if m.History.Enabled() {
		if m.History == params.FullImmutabilityThreshold {
			short += "h"
		} else {
			long += fmt.Sprintf(" --prune.h.older=%d", m.History)
		}
	}
	if m.Receipts.Enabled() {
		if m.Receipts == params.FullImmutabilityThreshold {
			short += "r"
		} else {
			long += fmt.Sprintf(" --prune.r.older=%d", m.Receipts)
		}
	}
	if m.TxIndex.Enabled() {
		if m.TxIndex == params.FullImmutabilityThreshold {
			short += "t"
		} else {
			long += fmt.Sprintf(" --prune.t.older=%d", m.TxIndex)
		}
	}
	if m.CallTraces.Enabled() {
		if m.CallTraces == params.FullImmutabilityThreshold {
			short += "c"
		} else {
			long += fmt.Sprintf(" --prune.c.older=%d", m.CallTraces)
		}
	}
	if m.Experiments.TEVM {
		long += " --experiments.tevm=enabled"
	}
	return short + long
}

func Override(db kv.RwTx, sm Mode) error {
	var (
		err error
	)

	err = setDistance(db, kv.PruneDistanceHistory, sm.History)
	if err != nil {
		return err
	}

	err = setDistance(db, kv.PruneDistanceReceipts, sm.Receipts)
	if err != nil {
		return err
	}

	err = setDistance(db, kv.PruneDistanceTxIndex, sm.TxIndex)
	if err != nil {
		return err
	}

	err = setDistance(db, kv.PruneDistanceCallTraces, sm.CallTraces)
	if err != nil {
		return err
	}

	err = setMode(db, kv.StorageModeTEVM, sm.Experiments.TEVM)
	if err != nil {
		return err
	}

	return nil
}

func SetIfNotExist(db kv.GetPut, pm Mode) error {
	var (
		err error
	)
	if !pm.Initialised {
		pm = DefaultMode
	}

	err = setDistanceOnEmpty(db, kv.PruneDistanceHistory, pm.History)
	if err != nil {
		return err
	}

	err = setDistanceOnEmpty(db, kv.PruneDistanceReceipts, pm.Receipts)
	if err != nil {
		return err
	}

	err = setDistanceOnEmpty(db, kv.PruneDistanceTxIndex, pm.TxIndex)
	if err != nil {
		return err
	}

	err = setDistanceOnEmpty(db, kv.PruneDistanceCallTraces, pm.CallTraces)
	if err != nil {
		return err
	}

	err = setModeOnEmpty(db, kv.StorageModeTEVM, pm.Experiments.TEVM)
	if err != nil {
		return err
	}

	return nil
}

func setDistance(db kv.Putter, key []byte, distance Distance) error {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, uint64(distance))
	if err := db.Put(kv.DatabaseInfo, key, v); err != nil {
		return err
	}
	return nil
}

func setDistanceOnEmpty(db kv.GetPut, key []byte, distance Distance) error {
	mode, err := db.GetOne(kv.DatabaseInfo, key)
	if err != nil {
		return err
	}
	if len(mode) == 0 || binary.BigEndian.Uint64(mode) == math.MaxUint64 {
		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, uint64(distance))
		if err = db.Put(kv.DatabaseInfo, key, v); err != nil {
			return err
		}
	}

	return nil
}

func setMode(db kv.RwTx, key []byte, currentValue bool) error {
	val := []byte{2}
	if currentValue {
		val = []byte{1}
	}
	if err := db.Put(kv.DatabaseInfo, key, val); err != nil {
		return err
	}
	return nil
}

func setModeOnEmpty(db kv.GetPut, key []byte, currentValue bool) error {
	mode, err := db.GetOne(kv.DatabaseInfo, key)
	if err != nil {
		return err
	}
	if len(mode) == 0 {
		val := []byte{2}
		if currentValue {
			val = []byte{1}
		}
		if err = db.Put(kv.DatabaseInfo, key, val); err != nil {
			return err
		}
	}

	return nil
}
