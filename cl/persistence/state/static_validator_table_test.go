package state_accessors

import (
	"bytes"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestStaticValidatorTable(t *testing.T) {
	// Make 5 validators.
	vals := []solid.Validator{solid.NewValidator(), solid.NewValidator(), solid.NewValidator(), solid.NewValidator(), solid.NewValidator()}

	table := NewStaticValidatorTable()
	for i := range vals {
		require.NoError(t, table.AddValidator(vals[i], uint64(i), 0))
	}
	// Now let us play with this
	require.NoError(t, table.AddExitEpoch(1, 32, 34))
	require.NoError(t, table.AddExitEpoch(1, 38, 35))
	require.NoError(t, table.AddExitEpoch(1, 2000, 500))

	require.Equal(t, table.ExitEpoch(1, 31), uint64(0))
	require.Equal(t, table.ExitEpoch(1, 32), uint64(34))
	require.Equal(t, table.ExitEpoch(1, 37), uint64(34))
	require.Equal(t, table.ExitEpoch(1, 38), uint64(35))
	require.Equal(t, table.ExitEpoch(1, 450), uint64(35))
	require.Equal(t, table.ExitEpoch(1, 1_000_000), uint64(500))
	// do the same for withdrawable epoch
	require.NoError(t, table.AddWithdrawableEpoch(1, 32, 34))
	require.NoError(t, table.AddWithdrawableEpoch(1, 38, 35))
	require.NoError(t, table.AddWithdrawableEpoch(1, 2000, 500))

	require.Equal(t, table.WithdrawableEpoch(1, 31), uint64(0))
	require.Equal(t, table.WithdrawableEpoch(1, 32), uint64(34))
	require.Equal(t, table.WithdrawableEpoch(1, 37), uint64(34))
	require.Equal(t, table.WithdrawableEpoch(1, 38), uint64(35))
	require.Equal(t, table.WithdrawableEpoch(1, 450), uint64(35))
	require.Equal(t, table.WithdrawableEpoch(1, 1_000_000), uint64(500))
	// now for withdrawal credentials
	require.NoError(t, table.AddWithdrawalCredentials(1, 32, common.HexToHash("0x2")))
	require.NoError(t, table.AddWithdrawalCredentials(1, 38, common.HexToHash("0x3")))
	require.NoError(t, table.AddWithdrawalCredentials(1, 2000, common.HexToHash("0x40")))
	require.Equal(t, table.WithdrawalCredentials(1, 31), common.Hash{})
	require.Equal(t, table.WithdrawalCredentials(1, 32), common.HexToHash("0x2"))
	require.Equal(t, table.WithdrawalCredentials(1, 37), common.HexToHash("0x2"))
	require.Equal(t, table.WithdrawalCredentials(1, 38), common.HexToHash("0x3"))
	require.Equal(t, table.WithdrawalCredentials(1, 450), common.HexToHash("0x3"))
	require.Equal(t, table.WithdrawalCredentials(1, 1_000_000), common.HexToHash("0x40"))
	// Now lets try to get the validator at a specific epoch
	new := solid.NewValidator()
	table.GetInPlace(1, 38, new)
	require.Equal(t, new.WithdrawableEpoch(), uint64(35))
	require.Equal(t, new.ExitEpoch(), uint64(35))
	require.Equal(t, new.WithdrawalCredentials(), common.HexToHash("0x3"))
	// Lastly serialize and deserialization
	table.ForEach(func(validatorIndex uint64, validator *StaticValidator) bool {
		var b bytes.Buffer
		require.NoError(t, validator.WriteTo(&b))
		tmp := &StaticValidator{}
		require.NoError(t, tmp.ReadFrom(&b))
		require.Equal(t, validator, tmp)
		return true
	})
}
