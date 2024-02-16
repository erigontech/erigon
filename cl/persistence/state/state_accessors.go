package state_accessors

import (
	"bytes"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/base_encoding"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

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

func ReadSlotData(tx kv.Tx, slot uint64) (*SlotData, error) {
	sd := &SlotData{}
	v, err := tx.GetOne(kv.SlotData, base_encoding.Encode64ToBytes4(slot))
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, nil
	}
	buf := bytes.NewBuffer(v)

	return sd, sd.ReadFrom(buf)
}

func ReadEpochData(tx kv.Tx, slot uint64) (*EpochData, error) {
	ed := &EpochData{}
	v, err := tx.GetOne(kv.EpochData, base_encoding.Encode64ToBytes4(slot))
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
func ReadCheckpoints(tx kv.Tx, slot uint64) (current solid.Checkpoint, previous solid.Checkpoint, finalized solid.Checkpoint, err error) {
	ed := &EpochData{}
	v, err := tx.GetOne(kv.EpochData, base_encoding.Encode64ToBytes4(slot))
	if err != nil {
		return nil, nil, nil, err
	}
	if len(v) == 0 {
		return nil, nil, nil, nil
	}
	buf := bytes.NewBuffer(v)

	if err := ed.ReadFrom(buf); err != nil {
		return nil, nil, nil, err
	}
	// Current, Pre
	return ed.CurrentJustifiedCheckpoint, ed.PreviousJustifiedCheckpoint, ed.FinalizedCheckpoint, nil
}

// ReadCheckpoints reads the checkpoints from the database, Current, Previous and Finalized
func ReadNextSyncCommittee(tx kv.Tx, slot uint64) (committee *solid.SyncCommittee, err error) {
	v, err := tx.GetOne(kv.NextSyncCommittee, base_encoding.Encode64ToBytes4(slot))
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
func ReadCurrentSyncCommittee(tx kv.Tx, slot uint64) (committee *solid.SyncCommittee, err error) {
	v, err := tx.GetOne(kv.CurrentSyncCommittee, base_encoding.Encode64ToBytes4(slot))
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

func ReadActiveIndicies(tx kv.Tx, slot uint64) ([]uint64, error) {
	key := base_encoding.Encode64ToBytes4(slot)
	v, err := tx.GetOne(kv.ActiveValidatorIndicies, key)
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, nil
	}
	buf := bytes.NewBuffer(v)
	return base_encoding.ReadRabbits(nil, buf)
}
