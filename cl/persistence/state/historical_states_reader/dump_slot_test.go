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
	"github.com/klauspost/compress/zstd"
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
			for name, read := range map[string]func(state_accessors.GetValFn) ([]byte, error){
				"effective": func(getter state_accessors.GetValFn) ([]byte, error) {
					return reader.reconstructDiffedUint64List(tx, getter, 1, slot, kv.ValidatorEffectiveBalance, kv.EffectiveBalancesDump)
				},
				"balances": func(getter state_accessors.GetValFn) ([]byte, error) {
					return reader.reconstructBalances(tx, getter, 1, slot, kv.ValidatorBalance, kv.BalancesDump)
				},
			} {
				t.Run(fmt.Sprintf("%s/%d", name, slot), func(t *testing.T) {
					getter := func(_ string, key []byte) ([]byte, error) {
						require.Equal(t, expected, base_encoding.Decode64FromBytes4(key))
						return nil, nil
					}
					_, err := read(getter)
					require.EqualError(t, err, fmt.Sprintf("dump not found for slot %d", expected))
				})
			}
		}
		return nil
	}))
}

func TestEffectiveBalanceDumpPartialValue(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error { return state_accessors.SetStateProcessingProgress(tx, 3073) }))
	reader := &HistoricalStatesReader{cfg: &clparams.MainnetBeaconConfig}
	for _, tc := range []struct {
		name      string
		slot      uint64
		length    int
		wantError bool
	}{
		{"forward whole values", 1568, 8, false},
		{"forward partial value", 1568, 9, true},
		{"exact partial value", 1536, 9, true},
		{"backward partial value", 2560, 9, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoder, err := zstd.NewWriter(nil)
			require.NoError(t, err)
			dump := encoder.EncodeAll(make([]byte, tc.length), nil)
			encoder.Close()
			getter := func(table string, _ []byte) ([]byte, error) {
				if table == kv.EffectiveBalancesDump {
					return dump, nil
				}
				return nil, nil
			}
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				values, err := reader.reconstructDiffedUint64List(tx, getter, 2, tc.slot, kv.ValidatorEffectiveBalance, kv.EffectiveBalancesDump)
				if tc.wantError {
					require.ErrorIs(t, err, state_accessors.ErrHistoryRebuildRequired)
				} else {
					require.NoError(t, err)
					require.Len(t, values, 16)
				}
				return nil
			}))
		})
	}
}
