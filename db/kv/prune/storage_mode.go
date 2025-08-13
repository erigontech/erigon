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

	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/kv"
)

var (
	ArchiveMode = Mode{
		Initialised: true,
		History:     Distance(math.MaxUint64),
		Blocks:      KeepAllBlocksPruneMode,
	}
	FullMode = Mode{
		Initialised: true,
		Blocks:      DefaultBlocksPruneMode,
		History:     Distance(config3.DefaultPruneDistance),
	}
	BlocksMode = Mode{
		Initialised: true,
		Blocks:      KeepAllBlocksPruneMode,
		History:     Distance(config3.DefaultPruneDistance),
	}
	MinimalMode = Mode{
		Initialised: true,
		Blocks:      Distance(config3.DefaultPruneDistance),
		History:     Distance(config3.DefaultPruneDistance),
	}

	DefaultMode = ArchiveMode
	MockMode    = Mode{
		Initialised: true,
		History:     Distance(math.MaxUint64),
		Blocks:      Distance(math.MaxUint64),
	}

	ErrUnknownPruneMode       = fmt.Errorf("--prune.mode must be one of %s, %s, %s", archiveModeStr, fullModeStr, minimalModeStr)
	ErrDistanceOnlyForArchive = fmt.Errorf("--prune.distance and --prune.distance.blocks are only allowed with --prune.mode=%s", archiveModeStr)
)

const (
	archiveModeStr = "archive"
	blockModeStr   = "blocks"
	fullModeStr    = "full"
	minimalModeStr = "minimal"
)

type Mode struct {
	Initialised bool // Set when the values are initialised (not default)
	History     BlockAmount
	Blocks      BlockAmount
}

func (m Mode) String() string {
	if !m.Initialised {
		return archiveModeStr
	}
	if m.History.toValue() == FullMode.History.toValue() && m.Blocks.toValue() == FullMode.Blocks.toValue() {
		return fullModeStr
	}
	if m.History.toValue() == MinimalMode.History.toValue() && m.Blocks.toValue() == MinimalMode.Blocks.toValue() {
		return minimalModeStr
	}

	if m.Blocks.toValue() == BlocksMode.Blocks.toValue() && m.History.toValue() == BlocksMode.History.toValue() {
		return blockModeStr
	}

	short := archiveModeStr
	if m.History.toValue() != DefaultMode.History.toValue() {
		short += fmt.Sprintf(" --prune.distance=%d", m.History.toValue())
	}
	if m.Blocks.toValue() != DefaultMode.Blocks.toValue() {
		short += fmt.Sprintf(" --prune.distance.blocks=%d", m.Blocks.toValue())
	}
	return strings.TrimLeft(short, " ")
}

func FromCli(pruneMode string, distanceHistory, distanceBlocks uint64) (Mode, error) {
	var mode Mode
	switch pruneMode {
	case archiveModeStr, "":
		mode = ArchiveMode
	case fullModeStr:
		mode = FullMode
	case minimalModeStr:
		mode = MinimalMode
	case blockModeStr:
		mode = BlocksMode
	default:
		return Mode{}, ErrUnknownPruneMode
	}

	if distanceHistory > 0 {
		mode.History = Distance(distanceHistory)
	}
	if distanceBlocks > 0 {
		mode.Blocks = Distance(distanceBlocks)
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

const (
	DefaultBlocksPruneMode = Distance(math.MaxUint64)     // Use chain-specific history pruning (aka. history-expiry)
	KeepAllBlocksPruneMode = Distance(math.MaxUint64 - 1) // Keep all history
)

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
func (p Distance) useDefaultValue() bool { return uint64(p) == config3.FullImmutabilityThreshold }
func (p Distance) dbType() []byte        { return kv.PruneTypeOlder }

func (p Distance) PruneTo(stageHead uint64) uint64 {
	if uint64(p) > stageHead {
		return 0
	}
	return stageHead - uint64(p)
}

// EnsureNotChanged - prohibit change some configs after node creation. prohibit from human mistakes
func EnsureNotChanged(tx kv.GetPut, pruneMode Mode) (Mode, error) {
	if err := setIfNotExist(tx, pruneMode); err != nil {
		return pruneMode, err
	}

	pm, err := Get(tx)
	if err != nil {
		return pruneMode, err
	}

	if pruneMode.Initialised {
		// Little initial design flaw: we used maxUint64 as default value for prune distance so history expiry was not accounted for.
		// We need to use because we are changing defaults in archive node from DefaultBlocksPruneMode to KeepAllBlocksPruneMode which is a different value so it would fail if we are running --prune.mode=archive.
		if (pm.History == DefaultBlocksPruneMode && pruneMode.History == DefaultBlocksPruneMode) &&
			(pm.Blocks == DefaultBlocksPruneMode && pruneMode.Blocks == KeepAllBlocksPruneMode) {
			return pruneMode, nil
		}
		// If storage mode is not explicitly specified, we take whatever is in the database
		if !reflect.DeepEqual(pm, pruneMode) {
			return pm, errors.New("changing --prune.* flags is prohibited, last time you used: --prune.mode=" + pm.String())
		}
	}
	return pm, nil
}

func setIfNotExist(db kv.GetPut, pm Mode) (err error) {
	if !pm.Initialised {
		pm = DefaultMode
	}

	pruneDBData := map[string]BlockAmount{
		string(kv.PruneHistory): pm.History,
		string(kv.PruneBlocks):  pm.Blocks,
	}

	for key, value := range pruneDBData {
		if err = setOnEmpty(db, []byte(key), value); err != nil {
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
