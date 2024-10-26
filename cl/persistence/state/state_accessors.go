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

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"

	libcommon "github.com/erigontech/erigon-lib/common"
)

type GetValFn func(table string, key []byte) ([]byte, error)

func GetValFnTxAndSnapshot(tx kv.Tx, snapshot *freezeblocks.CaplinStateSnapshots) GetValFn {
	return func(table string, key []byte) ([]byte, error) {
		if snapshot != nil {
			v, err := snapshot.Get(table, uint64(binary.BigEndian.Uint32(key)))
			if err != nil {
				return nil, err
			}
			if v != nil {
				return v, nil
			}
		}
		return tx.GetOne(table, key)
	}
}

// InitializeValidatorTable initializes the validator table in the database.
func InitializeStaticTables(tx kv.RwTx, state *state.CachingBeaconState) error {
	var err error
	if err = tx.ClearBucket(kv.ValidatorPublicKeys); err != nil {
		return err
	}
	if err = tx.ClearBucket(kv.HistoricalRoots); err != nil {
		return err
	}
	if err = tx.ClearBucket(kv.HistoricalSummaries); err != nil {
		return err
	}
	state.ForEachValidator(func(v solid.Validator, idx, total int) bool {
		key := base_encoding.Encode64ToBytes4(uint64(idx))
		if err = tx.Append(kv.ValidatorPublicKeys, key, v.PublicKeyBytes()); err != nil {
			return false
		}
		if err = tx.Put(kv.InvertedValidatorPublicKeys, v.PublicKeyBytes(), key); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	for i := 0; i < int(state.HistoricalRootsLength()); i++ {
		key := base_encoding.Encode64ToBytes4(uint64(i))
		root := state.HistoricalRoot(i)
		if err = tx.Append(kv.HistoricalRoots, key, root[:]); err != nil {
			return err
		}
	}
	var temp []byte
	for i := 0; i < int(state.HistoricalSummariesLength()); i++ {
		temp = temp[:0]
		key := base_encoding.Encode64ToBytes4(uint64(i))
		summary := state.HistoricalSummary(i)
		temp, err = summary.EncodeSSZ(temp)
		if err != nil {
			return err
		}
		if err = tx.Append(kv.HistoricalSummaries, key, temp); err != nil {
			return err
		}
	}
	return err
}

// IncrementValidatorTable increments the validator table in the database, by ignoring all the preverified indices.
func IncrementPublicKeyTable(tx kv.RwTx, state *state.CachingBeaconState, preverifiedIndicies uint64) error {
	valLength := state.ValidatorLength()
	for i := preverifiedIndicies; i < uint64(valLength); i++ {
		key := base_encoding.Encode64ToBytes4(i)
		pubKey, err := state.ValidatorPublicKey(int(i))
		if err != nil {
			return err
		}
		// We put as there could be reorgs and thus some of overwriting
		if err := tx.Put(kv.ValidatorPublicKeys, key, pubKey[:]); err != nil {
			return err
		}
		if err := tx.Put(kv.InvertedValidatorPublicKeys, pubKey[:], key); err != nil {
			return err
		}
	}
	return nil
}

func IncrementHistoricalRootsTable(tx kv.RwTx, state *state.CachingBeaconState, preverifiedIndicies uint64) error {
	for i := preverifiedIndicies; i < state.HistoricalRootsLength(); i++ {
		key := base_encoding.Encode64ToBytes4(i)
		root := state.HistoricalRoot(int(i))
		if err := tx.Put(kv.HistoricalRoots, key, root[:]); err != nil {
			return err
		}
	}
	return nil
}

func IncrementHistoricalSummariesTable(tx kv.RwTx, state *state.CachingBeaconState, preverifiedIndicies uint64) error {
	var temp []byte
	var err error
	for i := preverifiedIndicies; i < state.HistoricalSummariesLength(); i++ {
		temp = temp[:0]
		key := base_encoding.Encode64ToBytes4(i)
		summary := state.HistoricalSummary(int(i))
		temp, err = summary.EncodeSSZ(temp)
		if err != nil {
			return err
		}
		if err = tx.Put(kv.HistoricalSummaries, key, temp); err != nil {
			return err
		}
	}
	return nil
}

func ReadPublicKeyByIndex(tx kv.Tx, index uint64) (libcommon.Bytes48, error) {
	var pks []byte
	var err error
	key := base_encoding.Encode64ToBytes4(index)
	if pks, err = tx.GetOne(kv.ValidatorPublicKeys, key); err != nil {
		return libcommon.Bytes48{}, err
	}
	var ret libcommon.Bytes48
	copy(ret[:], pks)
	return ret, err
}

func ReadValidatorIndexByPublicKey(tx kv.Tx, key libcommon.Bytes48) (uint64, bool, error) {
	var index []byte
	var err error
	if index, err = tx.GetOne(kv.InvertedValidatorPublicKeys, key[:]); err != nil {
		return 0, false, err
	}
	if len(index) == 0 {
		return 0, false, nil
	}
	return base_encoding.Decode64FromBytes4(index), true, nil
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

func ReadSlotData(getFn GetValFn, slot uint64) (*SlotData, error) {
	sd := &SlotData{}
	v, err := getFn(kv.SlotData, base_encoding.Encode64ToBytes4(slot))
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, nil
	}
	buf := bytes.NewBuffer(v)

	return sd, sd.ReadFrom(buf)
}

func ReadEpochData(getFn GetValFn, slot uint64) (*EpochData, error) {
	ed := &EpochData{}
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
func ReadCheckpoints(getFn GetValFn, slot uint64) (current solid.Checkpoint, previous solid.Checkpoint, finalized solid.Checkpoint, ok bool, err error) {
	ed := &EpochData{}
	var v []byte
	v, err = getFn(kv.EpochData, base_encoding.Encode64ToBytes4(slot))
	if err != nil {
		return
	}
	if len(v) == 0 {
		return
	}
	buf := bytes.NewBuffer(v)

	if err = ed.ReadFrom(buf); err != nil {
		return
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

func ReadHistoricalRoots(tx kv.Tx, l uint64, fn func(idx int, root libcommon.Hash) error) error {
	for i := 0; i < int(l); i++ {
		key := base_encoding.Encode64ToBytes4(uint64(i))
		v, err := tx.GetOne(kv.HistoricalRoots, key)
		if err != nil {
			return err
		}
		if err := fn(i, libcommon.BytesToHash(v)); err != nil {
			return err
		}
	}
	return nil
}

func ReadHistoricalSummaries(tx kv.Tx, l uint64, fn func(idx int, historicalSummary *cltypes.HistoricalSummary) error) error {
	for i := 0; i < int(l); i++ {
		key := base_encoding.Encode64ToBytes4(uint64(i))
		v, err := tx.GetOne(kv.HistoricalSummaries, key)
		if err != nil {
			return err
		}
		historicalSummary := &cltypes.HistoricalSummary{}
		if err := historicalSummary.DecodeSSZ(v, 0); err != nil {
			return err
		}
		if err := fn(i, historicalSummary); err != nil {
			return err
		}
	}
	return nil
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
	if err != nil {
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
