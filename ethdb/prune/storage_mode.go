// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package prune

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/erigontech/erigon/erigon-lib/kv"
	"github.com/erigontech/erigon/params"
)

var DefaultMode = Mode{
	Initialised: true,
	History:     Distance(math.MaxUint64),
	Blocks:      Distance(math.MaxUint64),
	Experiments: Experiments{}, // all off
}

type Experiments struct {
}

func FromCli(chainId uint64, distanceHistory, distanceBlocks uint64, experiments []string) (Mode, error) {
	mode := DefaultMode

	mode.History = Distance(distanceHistory)
	mode.Blocks = Distance(distanceBlocks)

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

	blockAmount, err = get(db, kv.PruneBlocks)
	if err != nil {
		return prune, err
	}
	if blockAmount != nil {
		prune.Blocks = blockAmount
	}

	return prune, nil
}

type Mode struct {
	Initialised bool // Set when the values are initialised (not default)
	History     BlockAmount
	Blocks      BlockAmount
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
	if uint64(p) > stageHead {
		return 0
	}
	return stageHead - uint64(p)
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
	if m.Blocks.Enabled() {
		if m.Blocks.useDefaultValue() {
			short += fmt.Sprintf(" --prune.b.older=%d", defaultVal)
		} else {
			long += fmt.Sprintf(" --prune.b.%s=%d", m.Blocks.dbType(), m.Blocks.toValue())
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

	err = set(db, kv.PruneBlocks, sm.Blocks)
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
		string(kv.PruneHistory): pm.History,
		string(kv.PruneBlocks):  pm.Blocks,
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
