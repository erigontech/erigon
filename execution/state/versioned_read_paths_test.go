package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// These tests pin the observable behavior of versionedRead[T] across the
// ~25 distinct branches in its body so that a planned restructure (split
// into a non-generic core + per-path read* wrappers) can be validated
// against an identical surface. Each test maps to one of the branch
// labels enumerated in project_versionedread_refactor_plan.md.

// ------------------------------------------------------------------
// Section A: versionMap == nil (legacy / serial path)
// ------------------------------------------------------------------

// A.1: legacy path with a backing stateObject returns the storage value.
func TestVersionedRead_A1_LegacyWithStorage(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xa1})

	// IBS without versionMap → legacy/serial path
	ibs := New(&emptyReader{})
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	// emptyReader returns nil account → zero balance, no error
	assert.True(t, bal.IsZero(), "legacy path with empty reader returns zero")
}

// A.2: legacy path, GetCode triggers the readStorage==nil branch.
// (When versionMap is nil, getStateObject is consulted directly; readStorage
// closure on CodePath is non-nil so this exercises the storage branch too.)
func TestVersionedRead_A2_LegacyGetCodeReturnsEmpty(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xa2})

	ibs := New(&emptyReader{})
	code, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Empty(t, code, "empty reader => empty code")
}

// ------------------------------------------------------------------
// Section B: deleted stateObject short-circuit
// ------------------------------------------------------------------

// B: a deleted stateObject in the local map short-circuits to defaultV.
// We trigger this by Selfdestruct-ing the address inside the same IBS,
// then reading another field (which sees so.deleted in the local map).
func TestVersionedRead_B_DeletedStateObjectReturnsDefault(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 0)

	addr := accounts.InternAddress(common.HexToAddress("0xdead"))
	// Create + selfdestruct → state object marked deleted at FinalizeTx
	ibs.CreateAccount(addr, true)
	err := ibs.SetBalance(addr, *uint256.NewInt(50), 0)
	require.NoError(t, err)
	_, err = ibs.Selfdestruct(addr)
	require.NoError(t, err)
	// After Selfdestruct, GetBalance on the same address returns zero per
	// EIP semantics (deleted in this tx).
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.True(t, bal.IsZero(), "balance after selfdestruct is zero")
}

// ------------------------------------------------------------------
// Section C: SelfDestruct active in versionMap
// ------------------------------------------------------------------

// C.5: a prior tx marked the address selfdestructed (in versionMap), then
// GetCommittedState (commited=true) must return zero immediately.
func TestVersionedRead_C5_DestructedCommittedReturnsZero(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xc5})
	key := accounts.InternKey([32]byte{0x01})
	// tx 1 wrote a slot value
	mvhm.Write(addr, StoragePath, key, Version{TxIndex: 1, Incarnation: 0}, *uint256.NewInt(99), true)
	// tx 2 selfdestructed the account
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 2, Incarnation: 0}, true, true)

	// Read at tx 5 with commited=true: must return zero, ignoring slot value.
	v, err := ibs.GetCommittedState(addr, key)
	require.NoError(t, err)
	assert.True(t, v.IsZero(), "committed read past selfdestruct returns zero")
}

// C.6: a prior tx marked the address selfdestructed; non-commited read
// at a path != CodePath records the SelfDestructPath dependency in the
// readSet and returns zero.
func TestVersionedRead_C6_DestructedRecordsDepAndReturnsZero(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xc6})
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 2, Incarnation: 0}, true, true)

	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.True(t, bal.IsZero(), "non-commited balance read past selfdestruct returns zero")

	// SelfDestructPath dep must be recorded in readSet.
	reads, ok := ibs.versionedReads[addr]
	require.True(t, ok, "readSet entry for addr must exist")
	_, ok = reads[AccountKey{Path: SelfDestructPath, Key: accounts.NilKey}]
	assert.True(t, ok, "SelfDestructPath dependency must be recorded")
}

// C.4: CodePath is exempt from the SelfDestruct short-circuit. Even if SD
// is active, a CodePath read must fall through to the actual code-read
// branches rather than returning zero.
func TestVersionedRead_C4_CodePathBypassesSelfDestruct(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xc4})
	code := []byte{0x60, 0x42, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}
	mvhm.WriteCode(addr, Version{TxIndex: 2, Incarnation: 0}, code, true)
	// Selfdestruct at a LATER tx than the code write so the SD doesn't
	// trump the code per E.3a (the CodePath+SD trump check uses sdres.DepIdx).
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 1, Incarnation: 0}, true, true)

	got, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Equal(t, code, got, "CodePath should bypass SelfDestruct short-circuit")
}

// C.1/2/3: revival. After SelfDestruct, a later write to Balance / Nonce /
// CodeHash at a higher txIndex revives the account. Subsequent reads must
// see the revived value, not zero.
func TestVersionedRead_C1_RevivalViaBalance(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 10)

	addr := accounts.InternAddress([20]byte{0xc1})
	revivedBalance := uint256.NewInt(777)
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 2, Incarnation: 0}, true, true)
	mvhm.WriteBalance(addr, Version{TxIndex: 5, Incarnation: 0}, *revivedBalance, true)

	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, *revivedBalance, bal, "balance after revival must be the revived value, not zero")
}

// ------------------------------------------------------------------
// Section D: writeSet hit (intra-tx writes)
// ------------------------------------------------------------------

// D.2: a write in the current tx is read back via versionedWrites.
func TestVersionedRead_D2_WriteSetHit(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 0)

	addr := accounts.InternAddress([20]byte{0xd2})
	target := uint256.NewInt(123)
	err := ibs.SetBalance(addr, *target, 0)
	require.NoError(t, err)

	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, *target, bal, "writeSet hit returns the intra-tx written value")
}

// ------------------------------------------------------------------
// Section E: MVReadResultDone (versionMap has a definite value)
// ------------------------------------------------------------------

// E.1: versionMap MapRead hit on first call; second read at same tx hits
// the readSet (via the pr.Version == vr.Version branch).
func TestVersionedRead_E1_MapHitThenReadSetSameVersion(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xe1})
	target := uint256.NewInt(42)
	mvhm.WriteBalance(addr, Version{TxIndex: 1, Incarnation: 0}, *target, true)

	// First read populates the readSet via MapRead.
	bal1, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, *target, bal1)

	// Second read: same tx, same version → readSet hit (E.1 path).
	bal2, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, *target, bal2)
}

// E.3a: a Done MVReadResult for CodePath is trumped if a SelfDestruct at
// a >= DepIdx is also Done — return defaultV (nil code).
func TestVersionedRead_E3a_CodePathTrumpedBySelfDestruct(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 10)

	addr := accounts.InternAddress([20]byte{0xe3})
	code := []byte{0xfe, 0xfe}
	// Code at tx 2; SelfDestruct at tx 3 (strictly later → trumps code).
	mvhm.WriteCode(addr, Version{TxIndex: 2, Incarnation: 0}, code, true)
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 3, Incarnation: 0}, true, true)

	got, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Empty(t, got, "CodePath trumped by SelfDestruct returns nil/empty code")
}

// ------------------------------------------------------------------
// Section G: MVReadResultNone (no entry in versionMap for this key)
// ------------------------------------------------------------------

// G.1: readSet hit at MVReadResultNone (ReadSetRead source). Second read
// of an unwritten slot hits readSet from the first read's record.
func TestVersionedRead_G1_ReadSetReadOnSecondCall(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0x91})
	key := accounts.InternKey([32]byte{0x99})

	// First read of an unwritten slot: versionMap None → storage fallback →
	// records defaultV in readSet.
	v1, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	assert.True(t, v1.IsZero())

	// Second read: same tx, same key → readSet hit.
	v2, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	assert.True(t, v2.IsZero())

	// readSet must hold the StoragePath entry.
	reads, ok := ibs.versionedReads[addr]
	require.True(t, ok, "readSet entry for addr must exist after a storage read")
	_, ok = reads[AccountKey{Path: StoragePath, Key: key}]
	assert.True(t, ok, "StoragePath read must be recorded")
}

// G.6: StoragePath read on an unwritten slot with IncarnationPath written
// by a prior tx → returns zero (account was created/destroyed this block,
// all unwritten slots must be zero) and records the IncarnationPath dep.
func TestVersionedRead_G6_StorageZeroOnIncarnationWritten(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0x96})
	key := accounts.InternKey([32]byte{0xab})
	// Prior tx wrote IncarnationPath (CreateAccount / Selfdestruct event)
	mvhm.WriteIncarnation(addr, Version{TxIndex: 2, Incarnation: 0}, 1, true)

	got, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	assert.True(t, got.IsZero(), "unwritten slot reads zero after Incarnation rewrite")

	// IncarnationPath dep must be recorded.
	reads, ok := ibs.versionedReads[addr]
	require.True(t, ok, "readSet entry for addr must exist")
	_, ok = reads[AccountKey{Path: IncarnationPath, Key: accounts.NilKey}]
	assert.True(t, ok, "IncarnationPath dependency must be recorded")
}

// G.7: BalancePath read with no map/writeSet/readSet entry but a prior tx
// wrote an AddressPath account → use that account's balance.
func TestVersionedRead_G7_BalanceViaResolvedAddressPath(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0x97})
	priorAcc := &accounts.Account{Balance: *uint256.NewInt(555), Nonce: 3}
	mvhm.WriteAddress(addr, Version{TxIndex: 2, Incarnation: 0}, priorAcc, true)

	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, *uint256.NewInt(555), bal, "balance resolved via AddressPath account")
}

// G.8: storage fallback. Empty reader + no prior writes/reads → falls through
// to the readStorage callback (returns zero from emptyReader) and records
// defaultV in readSet.
func TestVersionedRead_G8_StorageFallbackEmptyReader(t *testing.T) {
	t.Parallel()
	mvhm := NewVersionMap(nil)
	ibs := NewWithVersionMap(&emptyReader{}, mvhm)
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0x98})
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.True(t, bal.IsZero(), "empty reader storage fallback returns zero")
}

// C.2: revival via NoncePath rewrite at a higher TxIdx than the SD.
func TestVersionedRead_C2_RevivalViaNonce(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 10)

	addr := accounts.InternAddress([20]byte{0xc2})
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 2, Incarnation: 0}, true, true)
	mvhm.WriteNonce(addr, Version{TxIndex: 5, Incarnation: 0}, 7, true)

	n, err := ibs.GetNonce(addr)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), n, "nonce after revival via NoncePath rewrite")
}

// C.3: revival via CodeHashPath rewrite at a higher TxIdx than the SD.
func TestVersionedRead_C3_RevivalViaCodeHash(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 10)

	addr := accounts.InternAddress([20]byte{0xc3})
	revivedHash := accounts.InternCodeHash([32]byte{0xab, 0xcd, 0xef})
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 2, Incarnation: 0}, true, true)
	mvhm.WriteCodeHash(addr, Version{TxIndex: 5, Incarnation: 0}, revivedHash, true)

	got, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	assert.Equal(t, revivedHash, got, "codehash after revival via CodeHashPath rewrite")
}

// E.2: when the readSet already records a different Version than the current
// versionMap value at MapRead, versionedRead panics with ErrDependency so
// the executor knows to re-execute. Captured via recover().
func TestVersionedRead_E2_MapReadDifferentVersionPanics(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 10)

	addr := accounts.InternAddress([20]byte{0xe2})
	// Two writes at different versions; the later version wins in versionMap.
	mvhm.WriteBalance(addr, Version{TxIndex: 2, Incarnation: 0}, *uint256.NewInt(10), true)

	// Manually seed readSet with a stale version (TxIndex 1) so that the
	// pr.Version != vr.Version branch fires.
	ibs.versionedReads = ReadSet{}
	ibs.versionedReads.Set(VersionedRead{
		Address: addr,
		Path:    BalancePath,
		Key:     accounts.NilKey,
		Source:  MapRead,
		Version: Version{TxIndex: 1, Incarnation: 0},
		ValU256: *uint256.NewInt(99),
	})

	defer func() {
		r := recover()
		require.NotNil(t, r, "must panic on version mismatch")
		err, ok := r.(error)
		require.True(t, ok, "panic must carry an error")
		assert.ErrorIs(t, err, ErrDependency, "panic carries ErrDependency")
	}()
	_, _ = ibs.GetBalance(addr)
}

// F: MVReadResultDependency status (versionMap saw an in-progress dep at a
// higher TxIdx than the current tx) → panic ErrDependency. Triggered by
// writing at our own txIndex's Done-but-Estimate state. Simplest reliable
// trigger: an Estimate-status entry (complete=false) at a TxIdx <= ours.
func TestVersionedRead_F_MVReadResultDependencyPanics(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xf0})
	// Estimate entry (complete=false) at txIndex 2 < 5 → Dependency.
	mvhm.WriteBalance(addr, Version{TxIndex: 2, Incarnation: 0}, *uint256.NewInt(50), false)

	defer func() {
		r := recover()
		require.NotNil(t, r, "must panic on Dependency status")
		err, ok := r.(error)
		require.True(t, ok)
		assert.ErrorIs(t, err, ErrDependency)
	}()
	_, _ = ibs.GetBalance(addr)
}

// D.1: writeSet hit at MVReadResultDone, but readSet has a stale version
// → panic ErrDependency (a write was based on a stale read). The current
// tx wrote, but the readSet for the same path holds a version older than
// the versionMap's Done entry.
func TestVersionedRead_D1_WriteSetHitWithStaleReadSetPanics(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xd1})
	// versionMap Done at tx 3 — higher than the readSet's stale tx-1 entry.
	mvhm.WriteBalance(addr, Version{TxIndex: 3, Incarnation: 0}, *uint256.NewInt(30), true)

	// Seed a stale readSet entry at a lower version.
	ibs.versionedReads = ReadSet{}
	ibs.versionedReads.Set(VersionedRead{
		Address: addr,
		Path:    BalancePath,
		Key:     accounts.NilKey,
		Source:  MapRead,
		Version: Version{TxIndex: 1, Incarnation: 0},
		ValU256: *uint256.NewInt(99),
	})
	// Seed a current-tx writeSet entry (intra-tx write) so the writeSet
	// branch fires.
	err := ibs.SetBalance(addr, *uint256.NewInt(77), 0)
	require.NoError(t, err)

	defer func() {
		r := recover()
		require.NotNil(t, r, "must panic when writeSet hit conflicts with stale readSet at versionMap Done")
		err, ok := r.(error)
		require.True(t, ok)
		assert.ErrorIs(t, err, ErrDependency)
	}()
	_, _ = ibs.GetBalance(addr)
}
