package state_accessors

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/base_encoding"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

// InitializeValidatorTable initializes the validator table in the database.
func InitializePublicKeyTable(tx kv.RwTx, state *state.CachingBeaconState) error {
	var err error
	if err = tx.ClearBucket(kv.ValidatorPublicKeys); err != nil {
		return err
	}
	state.ForEachValidator(func(v solid.Validator, idx, total int) bool {
		key := base_encoding.Encode64ToBytes4(uint64(idx))
		if err2 := tx.Append(kv.ValidatorPublicKeys, key, v.PublicKeyBytes()); err2 != nil {
			err = err2
			return false
		}
		return true
	})
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
