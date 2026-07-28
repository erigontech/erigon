package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// An account a tx leaves empty must be published as a delete on the versioned
// path too, not as an existing empty record: the write-set is flushed to the
// version map, so a later tx in the same block reads it. Before EIP-161 the
// touched account is created and persists, so the clearing is rules-gated.
func TestVersionedWritesClearTouchedEmptyAccount(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xe1"))
	spuriousDragon := &chain.Rules{IsSpuriousDragon: true}

	touchedWrites := func(rules *chain.Rules) *WriteSet {
		t.Helper()
		ibs := NewWithVersionMap(&minimalStateReader{}, NewVersionMap(nil))
		ibs.SetNoMaterialize(true)
		ibs.SetTxContext(1, 0)
		ibs.SetVersion(0)
		require.NoError(t, ibs.TouchAccount(addr))
		return ibs.FinalizedWrites(rules)
	}

	t.Run("serial baseline emits a delete", func(t *testing.T) {
		ibs := New(&minimalStateReader{})
		require.NoError(t, ibs.TouchAccount(addr))
		ibs.SoftFinalise()
		collector := NewLightCollector()
		require.NoError(t, ibs.MakeWriteSet(spuriousDragon, collector))
		sd, ok := collector.TakeWrites().GetSelfDestruct(addr)
		require.True(t, ok)
		require.True(t, sd.Val)
	})

	t.Run("versioned path withholds the created account", func(t *testing.T) {
		writes := touchedWrites(spuriousDragon)
		_, hasAddress := writes.GetAddress(addr)
		require.False(t, hasAddress, "the create must not reach the write-set")
		_, hasBalance := writes.GetBalance(addr)
		require.False(t, hasBalance)
		_, hasDelete := writes.GetSelfDestruct(addr)
		require.False(t, hasDelete, "nothing was published, so nothing to delete")
	})

	t.Run("next tx does not observe the account", func(t *testing.T) {
		vm := NewVersionMap(nil)
		vm.FlushVersionedWrites(touchedWrites(spuriousDragon), true, "")

		next := NewWithVersionMap(&minimalStateReader{}, vm)
		next.SetNoMaterialize(true)
		next.SetTxContext(1, 1)
		next.SetVersion(0)
		exists, err := next.Exist(addr)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("pre-SpuriousDragon keeps the touched account", func(t *testing.T) {
		writes := touchedWrites(&chain.Rules{})
		_, hasDelete := writes.GetSelfDestruct(addr)
		require.False(t, hasDelete)
		_, hasAddress := writes.GetAddress(addr)
		require.True(t, hasAddress)
	})
}
