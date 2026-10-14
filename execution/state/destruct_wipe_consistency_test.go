package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// A storage slot wiped by an in-block self-destruct reads zero, and that read
// must be recorded (survives a call-frame revert) so it appears in the BAL.
func TestDestructWipe_WipedStorageReadRecorded(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc8})
	key := accounts.InternKey([32]byte{0x01})
	vm := NewVersionMap(nil)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 0}, true, true)
	ibs := NewWithVersionMap(&emptyReader{}, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 1)
	snapshot := ibs.PushSnapshot()
	require.NoError(t, ibs.CreateAccount(addr, true))
	value, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.True(t, value.IsZero())
	ibs.RevertToSnapshot(snapshot, nil)
	reads := ibs.VersionedReads()
	read, ok := reads.GetStorage(addr, key)
	require.True(t, ok, "wiped storage read must be recorded")
	require.True(t, read.Val.IsZero())
	io := NewVersionedIO(2)
	io.RecordReads(Version{TxIndex: 1}, reads)
	bal := io.AsBlockAccessList()
	require.Len(t, bal, 1)
	require.Equal(t, []accounts.StorageKey{key}, bal[0].StorageReads, "slot must appear in BAL storage_reads")
}

// versionedStateReader must be revival-aware: a slot wiped by a destruct that a
// later revival (SelfDestruct=false) hides from latest-only probing must still
// read zero, not the pre-destruct value.
func TestDestructWipe_ReaderRangeScansHiddenDestruct(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xd7})
	key := accounts.InternKey([32]byte{0x09})
	vm := NewVersionMap(nil)
	vm.WriteStorage(addr, key, Version{TxIndex: 0}, *uint256.NewInt(5), true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true)
	vm.WriteIncarnation(addr, Version{TxIndex: 1}, 1, true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 2}, false, true) // revival hides the destruct from latest-only
	vm.WriteAddress(addr, Version{TxIndex: 2}, &accounts.Account{Nonce: 1, CodeHash: accounts.EmptyCodeHash}, true)
	vr := NewVersionedStateReader(3, ReadSet{}, vm, &emptyReader{}, false)
	val, _, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.True(t, val.IsZero(), "slot wiped by destruct hidden under a revival must read 0, got %s", val.String())
}

// A post-destruct write to the slot (revival era) must win over the wipe.
func TestDestructWipe_ReaderKeepsPostDestructWrite(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xd8})
	key := accounts.InternKey([32]byte{0x0a})
	vm := NewVersionMap(nil)
	vm.WriteStorage(addr, key, Version{TxIndex: 0}, *uint256.NewInt(5), true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true)
	vm.WriteIncarnation(addr, Version{TxIndex: 1}, 1, true)
	vm.WriteStorage(addr, key, Version{TxIndex: 2}, *uint256.NewInt(9), true) // revival-era write
	vr := NewVersionedStateReader(3, ReadSet{}, vm, &emptyReader{}, false)
	val, _, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(9), val, "a write above the destruct must survive the wipe")
}
