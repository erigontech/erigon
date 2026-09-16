package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestSetStateParallel_NoMaterialize verifies that an SSTORE on the parallel
// (versionMap) path records the storage write through versioned-write cells
// without materializing/caching a stateObject.
func TestSetStateParallel_NoMaterialize(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	key := accounts.InternKey([32]byte{0x01})
	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1

	reader := &storageReader{
		addr:    addr,
		account: &acc,
		storage: map[accounts.StorageKey]uint256.Int{key: *uint256.NewInt(5)},
	}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	require.NoError(t, ibs.SetState(addr, key, *uint256.NewInt(42)))

	writes := ibs.VersionedWrites()
	vw, ok := writes.GetStorage(addr, key)
	require.True(t, ok, "StoragePath write expected")
	assert.Equal(t, uint256.NewInt(42), &vw.Val)
	assert.Empty(t, ibs.stateObjects, "parallel SSTORE must not materialize a stateObject")

	// Reading the slot back within the tx returns the written value.
	got, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	assert.Equal(t, uint256.NewInt(42), &got)
	assert.Empty(t, ibs.stateObjects)
}

// TestSetStateParallel_NoOpToCommitted verifies that writing the current
// committed value records no versioned write (matches stateObject.SetState's
// set decision).
func TestSetStateParallel_NoOpToCommitted(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	key := accounts.InternKey([32]byte{0x02})
	acc := accounts.NewAccount()
	acc.Nonce = 1

	reader := &storageReader{
		addr:    addr,
		account: &acc,
		storage: map[accounts.StorageKey]uint256.Int{key: *uint256.NewInt(9)},
	}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	require.NoError(t, ibs.SetState(addr, key, *uint256.NewInt(9)))

	writes := ibs.VersionedWrites()
	_, ok := writes.GetStorage(addr, key)
	assert.False(t, ok, "writing the committed value should record no storage write")
	assert.Empty(t, ibs.stateObjects)
}

// TestGetCommittedStateParallel_ValidatedFloor verifies that a later tx's
// committed-storage read (used by SSTORE gas metering as `original`) resolves a
// predecessor's Validated write, not the stale domain value. Under early-break a
// predecessor's storage cell is Validated (pre-seal); a Done-only floor guard
// would make GetCommittedState fall back to the domain, misclassifying the
// SSTORE gas (clean-reset vs dirty) by SstoreWriteExistingEIP2929 (2800).
func TestGetCommittedStateParallel_ValidatedFloor(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	key := accounts.InternKey([32]byte{0x07})
	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1
	domainVal := uint256.NewInt(5)
	predVal := uint256.NewInt(42)

	reader := &storageReader{
		addr:    addr,
		account: &acc,
		storage: map[accounts.StorageKey]uint256.Int{key: *domainVal},
	}
	vm := NewVersionMap(nil)

	// tx0: SSTORE the slot, flushed Validated (pre-seal early-break state).
	ibs0 := NewWithVersionMap(reader, vm)
	ibs0.SetNoMaterialize(true)
	ibs0.SetTxContext(100, 0)
	ibs0.SetVersion(0)
	require.NoError(t, ibs0.SetState(addr, key, *predVal))
	writes0 := ibs0.VersionedWrites()
	vm.FlushVersionedWrites(writes0, false, "")
	vm.MarkWritesValidated(writes0, nil)

	_, rr, ok := vm.ReadStorage(addr, key, 5)
	require.True(t, ok)
	require.Equal(t, MVReadResultValidated, rr.Status(), "tx0 floor must be Validated")

	// tx1: committed read must see tx0's Validated write (42), not domain (5).
	ibs1 := NewWithVersionMap(reader, vm)
	ibs1.SetNoMaterialize(true)
	ibs1.SetTxContext(100, 5)
	ibs1.SetVersion(0)

	committed, err := ibs1.GetCommittedState(addr, key)
	require.NoError(t, err)
	assert.Equal(t, predVal, &committed, "GetCommittedState must resolve the Validated predecessor write, not the domain")

	current, err := ibs1.GetState(addr, key)
	require.NoError(t, err)
	assert.Equal(t, predVal, &current, "GetState must resolve the Validated predecessor write")
}

// TestBaseStateReadConsistency_NoReResolve pins the base-state read invariant:
// within one execution, every read of a slot's BASE state (before the tx writes
// it locally) must resolve to the same dependency version+value, even if the
// version-map floor moves under the tx. GetState (current) and GetCommittedState
// (original) must not diverge on the base — a divergence miscomputes SSTORE gas
// and, worse, rewriting the recorded read to the moved floor hides a
// consumed-but-abandoned value from seal validation. The pinned dep lets
// validation catch the version change and re-execute.
func TestBaseStateReadConsistency_NoReResolve(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	key := accounts.InternKey([32]byte{0x11})
	base := uint256.NewInt(1000)
	v1 := uint256.NewInt(7777) // predecessor's in-flight write (later abandoned)
	v2 := uint256.NewInt(5555) // predecessor's re-executed value

	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1
	reader := &storageReader{addr: addr, account: &acc, storage: map[accounts.StorageKey]uint256.Int{key: *base}}
	vm := NewVersionMap(nil)

	// Predecessor tx0 inc0 writes v1 (Done/visible), the value the reader consumes.
	vm.WriteStorage(addr, key, Version{TxIndex: 0, Incarnation: 0}, *v1, true)

	ibs := NewWithVersionMap(reader, vm)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	// First base read (as GetState/current would do in SSTORE gas): consumes v1,
	// records dep 0.0.
	cur, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, v1, &cur, "first base read should see predecessor tx0's write")

	// The floor moves under the tx: tx0 re-executes to inc1 with a different value.
	vm.WriteStorage(addr, key, Version{TxIndex: 0, Incarnation: 1}, *v2, true)

	// Second base read (as GetCommittedState/original would do): must return the
	// SAME pinned value the tx already consumed, not re-resolve to the moved floor.
	committed, err := ibs.GetCommittedState(addr, key)
	require.NoError(t, err)
	assert.Equal(t, &cur, &committed, "base reads must be consistent within a tx (no re-resolve to moved floor)")

	// The recorded read must stay pinned at the consumed version (0.0) so seal
	// validation catches the floor change (now 0.1) and re-executes.
	tr, ok := ibs.versionedReads.GetStorage(addr, key)
	require.True(t, ok)
	assert.Equal(t, 0, tr.Version.TxIndex)
	assert.Equal(t, 0, tr.Version.Incarnation, "recorded dep must remain the consumed incarnation, not re-resolve to 0.1")
}
