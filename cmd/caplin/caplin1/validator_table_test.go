package caplin1

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/stretchr/testify/require"
)

func TestLoadStaticValidatorTableWithoutReplay(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	validator := solid.NewValidator()
	key := [48]byte{0x42}
	validator.SetPublicKey(key)
	var data bytes.Buffer
	require.NoError(t, state_accessors.NewStaticValidatorFromValidator(validator, 0).Serialize(&data))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := tx.Put(kv.StaticValidators, base_encoding.Encode64ToBytes4(0), data.Bytes()); err != nil {
			return err
		}
		return state_accessors.SetStateProcessingProgress(tx, 96)
	}))
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		config := clparams.CaplinConfig{ArchiveStates: true}
		table, err := state_accessors.LoadStaticValidatorTable(tx, config.ArchiveStates, antiquary.StateHistoryEnabled(config.ArchiveStates, config.ArchiveBlocks, &clparams.MainnetBeaconConfig))
		require.NoError(t, err)
		require.False(t, table.ReplayEnabled())
		require.Equal(t, uint64(96), table.Slot())
		validators, err := table.CopyValidatorsAtSlot(96, 1, 10)
		require.NoError(t, err)
		require.Equal(t, key, validators.Get(0).PublicKey())
		return nil
	}))
}
