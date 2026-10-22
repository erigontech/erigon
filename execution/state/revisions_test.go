package state

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestPushSnapshotHotPathNoAllocs(t *testing.T) {
	construct := testing.AllocsPerRun(200, func() {
		ibs := New(nil)
		defer ibs.Close()
	})
	constructAndPush := testing.AllocsPerRun(200, func() {
		ibs := New(nil)
		defer ibs.Close()
		var ids [16]int
		for i := range ids {
			ids[i] = ibs.PushSnapshot()
		}
		for i := len(ids) - 1; i >= 0; i-- {
			ibs.PopSnapshot(ids[i])
		}
	})
	if delta := constructAndPush - construct; delta >= 1 {
		t.Fatalf("16 PushSnapshots on a fresh IntraBlockState allocated %.1f times", delta)
	}
}

// A reverted first write of a slot must leave no versioned write behind, whether
// the slot's prior read was the only touch of the address or the address was
// already dirty from a write to another slot.
func TestVersionedRevertFirstStorageWriteAfterRead(t *testing.T) {
	t.Parallel()
	for _, dirtyFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("dirty=%v", dirtyFirst), func(t *testing.T) {
			addr := accounts.InternAddress(common.HexToAddress("0x5A1"))
			keyA := accounts.InternKey(common.HexToHash("0xA"))
			keyB := accounts.InternKey(common.HexToHash("0xB"))
			ibs := New(newAccountStateReader())
			ibs.SetTxContext(0, 0)
			ibs.SetVersion(0)
			ibs.SetVersionMap(NewVersionMap(nil))

			_, err := ibs.GetState(addr, keyA)
			require.NoError(t, err)
			if dirtyFirst {
				require.NoError(t, ibs.SetState(addr, keyB, *uint256.NewInt(2)))
			}
			snap := ibs.PushSnapshot()
			require.NoError(t, ibs.SetState(addr, keyA, *uint256.NewInt(1)))
			ibs.RevertToSnapshot(snap, nil)

			_, ok := ibs.versionedWrites.GetStorage(addr, keyA)
			require.False(t, ok, "the reverted write must not stay in the write set")
		})
	}
}
