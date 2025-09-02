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

package state_accessors

import (
	"bytes"
	"encoding/binary"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync"
)

type GetValFn func(table string, key []byte) ([]byte, error)

func GetValFnTxAndSnapshot(tx kv.Tx, snapshotRoTx *snapshotsync.CaplinStateView) GetValFn {
	return func(table string, key []byte) ([]byte, error) {
		if snapshotRoTx != nil {
			slot := uint64(binary.BigEndian.Uint32(key))
			segment, ok := snapshotRoTx.VisibleSegment(slot, table)
			if ok {
				return segment.Get(slot)
			}
		}
		return tx.GetOne(table, key)
	}
}

func GetStateProcessingProgress(tx kv.Tx) (uint64, error) {
	progressByytes, err := tx.GetOne(kv.StatesProcessingProgress, kv.StatesProcessingKey)
	if err != nil {
		return 0, err
	}
	if len(progressByytes) == 0 {
		return 0, nil
	}
	return base_encoding.Decode64FromBytes4(progressByytes), nil
}

func SetStateProcessingProgress(tx kv.RwTx, progress uint64) error {
	return tx.Put(kv.StatesProcessingProgress, kv.StatesProcessingKey, base_encoding.Encode64ToBytes4(progress))
}

func ReadSlotData(getFn GetValFn, slot uint64, cfg *clparams.BeaconChainConfig) (*SlotData, error) {
	sd := &SlotData{}
	v, err := getFn(kv.SlotData, base_encoding.Encode64ToBytes4(slot))
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, nil
	}
	buf := bytes.NewBuffer(v)

	return sd, sd.ReadFrom(buf, cfg)
}

func ReadEpochData(getFn GetValFn, slot uint64, beaconConfig *clparams.BeaconChainConfig) (*EpochData, error) {
	ed := &EpochData{
		BeaconConfig: beaconConfig,
		Version:      beaconConfig.GetCurrentStateVersion(slot / beaconConfig.SlotsPerEpoch),
	}
	v, err := getFn(kv.EpochData, base_encoding.Encode64ToBytes4(slot))
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, nil
	}
	buf := bytes.NewBuffer(v)

	return ed, ed.ReadFrom(buf)
}

// ReadCheckpoints reads the checkpoints from the database, Current, Previous and Finalized
func ReadCheckpoints(getFn GetValFn, slot uint64, beaconConfig *clparams.BeaconChainConfig) (current solid.Checkpoint, previous solid.Checkpoint, finalized solid.Checkpoint, ok bool, err error) {
	ed, err := ReadEpochData(getFn, slot, beaconConfig)
	if err != nil {
		return solid.Checkpoint{}, solid.Checkpoint{}, solid.Checkpoint{}, false, err
	}
	if ed == nil {
		return solid.Checkpoint{}, solid.Checkpoint{}, solid.Checkpoint{}, false, nil
	}
	return ed.CurrentJustifiedCheckpoint, ed.PreviousJustifiedCheckpoint, ed.FinalizedCheckpoint, true, nil
}

// ReadCheckpoints reads the checkpoints from the database, Current, Previous and Finalized
func ReadNextSyncCommittee(getFn GetValFn, slot uint64) (committee *solid.SyncCommittee, err error) {
	v, err := getFn(kv.NextSyncCommittee, base_encoding.Encode64ToBytes4(slot))
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, nil
	}
	committee = &solid.SyncCommittee{}
	copy(committee[:], v)
	return
}

// ReadCheckpoints reads the checkpoints from the database, Current, Previous and Finalized
func ReadCurrentSyncCommittee(getFn GetValFn, slot uint64) (committee *solid.SyncCommittee, err error) {
	v, err := getFn(kv.CurrentSyncCommittee, base_encoding.Encode64ToBytes4(slot))
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, nil
	}
	committee = &solid.SyncCommittee{}
	copy(committee[:], v)
	return
}

func ReadValidatorsTable(tx kv.Tx, out *StaticValidatorTable) error {
	cursor, err := tx.Cursor(kv.StaticValidators)
	if err != nil {
		return err
	}
	defer cursor.Close()

	var buf bytes.Buffer
	for k, v, err := cursor.First(); err == nil && k != nil; k, v, err = cursor.Next() {
		staticValidator := &StaticValidator{}
		buf.Reset()
		if _, err := buf.Write(v); err != nil {
			return err
		}
		if err := staticValidator.ReadFrom(&buf); err != nil {
			return err
		}
		out.validatorTable = append(out.validatorTable, staticValidator)
	}
	if err != nil { //nolint:govet
		return err
	}
	slot, err := GetStateProcessingProgress(tx)
	if err != nil {
		return err
	}
	out.slot = slot
	return err
}

func ReadActiveIndicies(getFn GetValFn, slot uint64) ([]uint64, error) {
	key := base_encoding.Encode64ToBytes4(slot)
	v, err := getFn(kv.ActiveValidatorIndicies, key)
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, nil
	}
	buf := bytes.NewBuffer(v)
	return base_encoding.ReadRabbits(nil, buf)
}
