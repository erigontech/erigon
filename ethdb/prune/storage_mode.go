package prune

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
)

var DefaultMode = Mode{
	Initialised: true,
	History:     Distance(math.MaxUint64), // all off
	Receipts:    Distance(math.MaxUint64),
	TxIndex:     Distance(math.MaxUint64),
	CallTraces:  Distance(math.MaxUint64),
	Experiments: Experiments{}, // all off
}

var (
	mainnetDepositContractBlock uint64 = 11052984
	sepoliaDepositContractBlock uint64 = 1273020
	goerliDepositContractBlock  uint64 = 4367322
	gnosisDepositContractBlock  uint64 = 19475089
	chiadoDepositContractBlock  uint64 = 155530
)

type Experiments struct {
}

func FromCli(chainId uint64, flags string, exactHistory, exactReceipts, exactTxIndex, exactCallTraces,
	beforeH, beforeR, beforeT, beforeC uint64, experiments []string) (Mode, error) {
	mode := DefaultMode

	if flags != "default" && flags != "disabled" {
		for _, flag := range flags {
			switch flag {
			case 'h':
				mode.History = Distance(params.FullImmutabilityThreshold)
			case 'r':
				mode.Receipts = Distance(params.FullImmutabilityThreshold)
			case 't':
				mode.TxIndex = Distance(params.FullImmutabilityThreshold)
			case 'c':
				mode.CallTraces = Distance(params.FullImmutabilityThreshold)
			default:
				return DefaultMode, fmt.Errorf("unexpected flag found: %c", flag)
			}
		}
	}

	pruneBlockBefore := pruneBlockDefault(chainId)

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

	if beforeH > 0 {
		mode.History = Before(beforeH)
	}
	if beforeR > 0 {
		if pruneBlockBefore != 0 {
			log.Warn("specifying prune.before.r might break CL compatibility")
			if beforeR > pruneBlockBefore {
				log.Warn("the specified prune.before.r block number is higher than the deposit contract contract block number", "highest block number", pruneBlockBefore)
			}
		}
		mode.Receipts = Before(beforeR)
	} else if exactReceipts == 0 && mode.Receipts.Enabled() && pruneBlockBefore != 0 {
		// Default --prune=r to pruning receipts before the Beacon Chain genesis
		mode.Receipts = Before(pruneBlockBefore)
	}
	if beforeT > 0 {
		mode.TxIndex = Before(beforeT)
	}
	if beforeC > 0 {
		mode.CallTraces = Before(beforeC)
	}

	for _, ex := range experiments {
		switch ex {
		case "":
			// skip
		default:
			return DefaultMode, fmt.Errorf("unexpected experiment found: %s", ex)
		}
	}
	return mode, nil
}

func pruneBlockDefault(chainId uint64) uint64 {
	switch chainId {
	case 1 /* mainnet */ :
		return mainnetDepositContractBlock
	case 11155111 /* sepolia */ :
		return sepoliaDepositContractBlock
	case 5 /* goerli */ :
		return goerliDepositContractBlock
	case 10200 /* chiado */ :
		return chiadoDepositContractBlock
	case 100 /* gnosis */ :
		return gnosisDepositContractBlock
	}

	return 0
}

func Get(db kv.Getter) (Mode, error) {
	prune := DefaultMode
	prune.Initialised = true

	blockAmount, err := get(db, kv.PruneHistory)
	if err != nil {
		return prune, err
	}
	if blockAmount != nil {
		prune.History = blockAmount
	}

	blockAmount, err = get(db, kv.PruneReceipts)
	if err != nil {
		return prune, err
	}
	if blockAmount != nil {
		prune.Receipts = blockAmount
	}

	blockAmount, err = get(db, kv.PruneTxIndex)
	if err != nil {
		return prune, err
	}
	if blockAmount != nil {
		prune.TxIndex = blockAmount
	}

	blockAmount, err = get(db, kv.PruneCallTraces)
	if err != nil {
		return prune, err
	}
	if blockAmount != nil {
		prune.CallTraces = blockAmount
	}

	return prune, nil
}

type Mode struct {
	Initialised bool // Set when the values are initialised (not default)
	History     BlockAmount
	Receipts    BlockAmount
	TxIndex     BlockAmount
	CallTraces  BlockAmount
	Experiments Experiments
}

type BlockAmount interface {
	PruneTo(stageHead uint64) uint64
	Enabled() bool
	toValue() uint64
	dbType() []byte
	useDefaultValue() bool
}

// Distance amount of blocks to keep in DB
// but manual manipulation with such distance is very unsafe
// for example:
//
//	deleteUntil := currentStageProgress - pruningDistance
//
// may delete whole db - because of uint64 underflow when pruningDistance > currentStageProgress
type Distance uint64

func (p Distance) Enabled() bool         { return p != math.MaxUint64 }
func (p Distance) toValue() uint64       { return uint64(p) }
func (p Distance) useDefaultValue() bool { return uint64(p) == params.FullImmutabilityThreshold }
func (p Distance) dbType() []byte        { return kv.PruneTypeOlder }

func (p Distance) PruneTo(stageHead uint64) uint64 {
	if p == 0 {
		panic("pruning distance were not set")
	}
	if uint64(p) > stageHead {
		return 0
	}
	return stageHead - uint64(p)
}

// Before number after which keep in DB
type Before uint64

func (b Before) Enabled() bool         { return b > 0 }
func (b Before) toValue() uint64       { return uint64(b) }
func (b Before) useDefaultValue() bool { return uint64(b) == 0 }
func (b Before) dbType() []byte        { return kv.PruneTypeBefore }

func (b Before) PruneTo(uint64) uint64 {
	if b == 0 {
		return uint64(b)
	}

	return uint64(b) - 1
}

func (m Mode) String() string {
	if !m.Initialised {
		return "default"
	}
	const defaultVal uint64 = params.FullImmutabilityThreshold
	long := ""
	short := ""
	if m.History.Enabled() {
		if m.History.useDefaultValue() {
			short += fmt.Sprintf(" --prune.h.older=%d", defaultVal)
		} else {
			long += fmt.Sprintf(" --prune.h.%s=%d", m.History.dbType(), m.History.toValue())
		}
	}
	if m.Receipts.Enabled() {
		if m.Receipts.useDefaultValue() {
			short += fmt.Sprintf(" --prune.r.older=%d", defaultVal)
		} else {
			long += fmt.Sprintf(" --prune.r.%s=%d", m.Receipts.dbType(), m.Receipts.toValue())
		}
	}
	if m.TxIndex.Enabled() {
		if m.TxIndex.useDefaultValue() {
			short += fmt.Sprintf(" --prune.t.older=%d", defaultVal)
		} else {
			long += fmt.Sprintf(" --prune.t.%s=%d", m.TxIndex.dbType(), m.TxIndex.toValue())
		}
	}
	if m.CallTraces.Enabled() {
		if m.CallTraces.useDefaultValue() {
			short += fmt.Sprintf(" --prune.c.older=%d", defaultVal)
		} else {
			long += fmt.Sprintf(" --prune.c.%s=%d", m.CallTraces.dbType(), m.CallTraces.toValue())
		}
	}

	return strings.TrimLeft(short+long, " ")
}

func Override(db kv.RwTx, sm Mode) error {
	var (
		err error
	)

	err = set(db, kv.PruneHistory, sm.History)
	if err != nil {
		return err
	}

	err = set(db, kv.PruneReceipts, sm.Receipts)
	if err != nil {
		return err
	}

	err = set(db, kv.PruneTxIndex, sm.TxIndex)
	if err != nil {
		return err
	}

	err = set(db, kv.PruneCallTraces, sm.CallTraces)
	if err != nil {
		return err
	}

	return nil
}

// EnsureNotChanged - prohibit change some configs after node creation. prohibit from human mistakes
func EnsureNotChanged(tx kv.GetPut, pruneMode Mode) (Mode, error) {
	err := setIfNotExist(tx, pruneMode)
	if err != nil {
		return pruneMode, err
	}

	pm, err := Get(tx)
	if err != nil {
		return pruneMode, err
	}

	if pruneMode.Initialised {
		// If storage mode is not explicitly specified, we take whatever is in the database
		if !reflect.DeepEqual(pm, pruneMode) {
			if bytes.Equal(pm.Receipts.dbType(), kv.PruneTypeOlder) && bytes.Equal(pruneMode.Receipts.dbType(), kv.PruneTypeBefore) {
				log.Error("--prune=r flag has been changed to mean pruning of receipts before the Beacon Chain genesis. Please re-sync Erigon from scratch. " +
					"Alternatively, enforce the old behaviour explicitly by --prune.r.older=90000 flag at the risk of breaking the Consensus Layer.")
			}
			return pm, errors.New("not allowed change of --prune flag, last time you used: " + pm.String())
		}
	}
	return pm, nil
}

func setIfNotExist(db kv.GetPut, pm Mode) error {
	var (
		err error
	)
	if !pm.Initialised {
		pm = DefaultMode
	}

	pruneDBData := map[string]BlockAmount{
		string(kv.PruneHistory):    pm.History,
		string(kv.PruneReceipts):   pm.Receipts,
		string(kv.PruneTxIndex):    pm.TxIndex,
		string(kv.PruneCallTraces): pm.CallTraces,
	}

	for key, value := range pruneDBData {
		err = setOnEmpty(db, []byte(key), value)
		if err != nil {
			return err
		}
	}

	return nil
}

func createBlockAmount(pruneType []byte, v []byte) (BlockAmount, error) {
	var blockAmount BlockAmount

	switch string(pruneType) {
	case string(kv.PruneTypeOlder):
		blockAmount = Distance(binary.BigEndian.Uint64(v))
	case string(kv.PruneTypeBefore):
		blockAmount = Before(binary.BigEndian.Uint64(v))
	default:
		return nil, fmt.Errorf("unexpected block amount type: %s", string(pruneType))
	}

	return blockAmount, nil
}

func get(db kv.Getter, key []byte) (BlockAmount, error) {
	v, err := db.GetOne(kv.DatabaseInfo, key)
	if err != nil {
		return nil, err
	}

	vType, err := db.GetOne(kv.DatabaseInfo, keyType(key))
	if err != nil {
		return nil, err
	}

	if v != nil {
		blockAmount, err := createBlockAmount(vType, v)
		if err != nil {
			return nil, err
		}
		return blockAmount, nil
	}

	return nil, nil
}

func set(db kv.Putter, key []byte, blockAmount BlockAmount) error {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, blockAmount.toValue())
	if err := db.Put(kv.DatabaseInfo, key, v); err != nil {
		return err
	}

	keyType := keyType(key)

	if err := db.Put(kv.DatabaseInfo, keyType, blockAmount.dbType()); err != nil {
		return err
	}

	return nil
}

func keyType(name []byte) []byte {
	return append(name, []byte("Type")...)
}

func setOnEmpty(db kv.GetPut, key []byte, blockAmount BlockAmount) error {
	mode, err := db.GetOne(kv.DatabaseInfo, key)
	if err != nil {
		return err
	}
	if len(mode) == 0 {
		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, blockAmount.toValue())
		if err = db.Put(kv.DatabaseInfo, key, v); err != nil {
			return err
		}
		if err = db.Put(kv.DatabaseInfo, keyType(key), blockAmount.dbType()); err != nil {
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
