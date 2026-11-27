package historical_states_reader

import (
	"fmt"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/stretchr/testify/require"
)

func TestMissingDumpReportsSlotRead(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error { return state_accessors.SetStateProcessingProgress(tx, 4000) }))
	reader := &HistoricalStatesReader{cfg: &clparams.MainnetBeaconConfig}
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		for _, slot := range []uint64{1600, 2560} {
			expected := uint64(1536)
			if slot == 2560 {
				expected = 3072
			}
			getter := func(_ string, key []byte) ([]byte, error) {
				require.Equal(t, expected, base_encoding.Decode64FromBytes4(key))
				return nil, nil
			}
			for name, read := range map[string]func() ([]byte, error){
				"effective": func() ([]byte, error) {
					return reader.reconstructDiffedUint64List(tx, getter, 1, slot, kv.ValidatorEffectiveBalance, kv.EffectiveBalancesDump)
				},
				"balances": func() ([]byte, error) {
					return reader.reconstructBalances(tx, getter, 1, slot, kv.ValidatorBalance, kv.BalancesDump)
				},
			} {
				t.Run(fmt.Sprintf("%s/%d", name, slot), func(t *testing.T) {
					_, err := read()
					require.EqualError(t, err, fmt.Sprintf("dump not found for slot %d", expected))
				})
			}
		}
		return nil
	}))
}
