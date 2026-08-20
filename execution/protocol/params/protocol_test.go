package params

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEIP8038GasSchedule(t *testing.T) {
	require.Equal(t, uint64(2_100), ColdStorageAccessCostEIP8038)
	require.Equal(t, uint64(9_000), AccountWriteCostEIP8038)
	require.Equal(t, uint64(11_300), CallValueTransferGasEIP8038)
	require.Equal(t, uint64(12_000), CreateAccessEIP8038)
	require.Equal(t, uint64(11_616), SstoreClearsScheduleRefundEIP8038)
	require.Equal(t, uint64(2_900), TxAccessListAddressGasEIP8038)
	require.Equal(t, uint64(2_000), TxAccessListStorageKeyGasEIP8038)
	require.Equal(t, uint64(12_000), CreateAccessEIP2780)
}
