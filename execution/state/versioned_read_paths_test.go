package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// These tests pin the observable behavior of the versioned read across its
// distinct branches (non-generic core + per-path read* wrappers) so a
// restructure can be validated against an identical surface.

// ------------------------------------------------------------------
// Section A: versionMap == nil (legacy / serial path)
// ------------------------------------------------------------------

// legacy path with a backing stateObject returns the storage value.
func TestVersionedRead_A1_LegacyWithStorage(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xa1})

	// IBS without versionMap → legacy/serial path
	ibs := New(&emptyReader{})
	defer ibs.Close()
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	// emptyReader returns nil account → zero balance, no error
	assert.True(t, bal.IsZero(), "legacy path with empty reader returns zero")
}

// legacy path, GetCode triggers the readStorage==nil branch.
// (When versionMap is nil, getStateObject is consulted directly; readStorage
// closure on CodePath is non-nil so this exercises the storage branch too.)
func TestVersionedRead_A2_LegacyGetCodeReturnsEmpty(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xa2})

	ibs := New(&emptyReader{})
	defer ibs.Close()
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
	defer ibs.Close()
	ibs.SetTxContext(1, 0)

	addr := accounts.InternAddress(common.HexToAddress("0xdead"))
	// Create + selfdestruct → state object marked deleted at FinalizeTx
	require.NoError(t, ibs.CreateAccount(addr, true))
	err := ibs.SetBalance(addr, *uint256.NewInt(50), 0)
	require.NoError(t, err)
	_, err = ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	// After Selfdestruct, GetBalance on the same address returns zero per
	// EIP semantics (deleted in this tx).
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.True(t, bal.IsZero(), "balance after selfdestruct is zero")
}

// warmReadIBS builds a materialized versioned IBS over one committed account,
// so a later selfdestruct in the version map makes getStateObject park a
// deleted resident object for addr — the state the read-once fast path must
// not read through.
func warmReadIBS(t *testing.T, addr accounts.Address) (*IntraBlockState, *VersionMap) {
	t.Helper()
	_, tx, domains := NewTestRwTx(t)
	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1
	acc.Balance = *uint256.NewInt(1234)
	addrValue := addr.Value()
	domains.SetTxNum(10)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addrValue[:], accounts.SerialiseV3(&acc), 10, nil))

	mvhm := NewVersionMap(nil)
	ibs := NewWithVersionMap(NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})), mvhm)
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 5)
	return ibs, mvhm
}

// B: a deleted stateObject wins over a slot this tx already read. The read-once
// fast path must not serve the pre-destruct value.
func TestVersionedRead_B_DeletedStateObjectBeatsWarmStorageRead(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xb2})
	ibs, mvhm := warmReadIBS(t, addr)

	key := accounts.InternKey([32]byte{0x01})
	prior := Version{TxIndex: 1, Incarnation: 0}
	mvhm.WriteStorage(addr, key, prior, *uint256.NewInt(99), true)

	v, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.EqualValues(t, 99, v.Uint64(), "first read resolves from the version map")

	// Snapshot this tx's reads for OCC validation.
	io := NewVersionedIO(6)
	io.RecordReads(Version{TxIndex: 5, Incarnation: 0}, ibs.VersionedReads())

	// A prior tx destroys the account after this tx's warm read. The cache-free
	// path keeps no deleted stateObject to shadow the recorded read; the staleness
	// is caught by OCC validation, which fails and forces re-execution rather than
	// letting the pre-destruct value stand.
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 2, Incarnation: 0}, true, true)
	require.Equal(t, VersionInvalid, mvhm.ValidateVersion(5, io, validateEqualVersion, false, ""))
}

func TestVersionedRead_B_DeletedStateObjectBeatsWarmCodeSizeRead(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xb4})
	ibs, mvhm := warmReadIBS(t, addr)

	mvhm.WriteCodeSize(addr, Version{TxIndex: 1, Incarnation: 0}, 7, true)
	size, err := ibs.GetCodeSize(addr)
	require.NoError(t, err)
	require.Equal(t, 7, size, "first read resolves from the version map")

	io := NewVersionedIO(6)
	io.RecordReads(Version{TxIndex: 5, Incarnation: 0}, ibs.VersionedReads())

	// A prior tx destroys the account after the warm read: OCC validation catches it.
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 2, Incarnation: 0}, true, true)
	require.Equal(t, VersionInvalid, mvhm.ValidateVersion(5, io, validateEqualVersion, false, ""))
}

// The read-once fast path must return exactly what the read-set-hit branch it
// replaces returns: same value, source, version and clean flag.
func TestVersionedRead_B_WarmStorageReadReturnsReadSetTuple(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xb3})
	ibs, mvhm := warmReadIBS(t, addr)

	key := accounts.InternKey([32]byte{0x01})
	version := Version{TxIndex: 1, Incarnation: 0}
	mvhm.WriteStorage(addr, key, version, *uint256.NewInt(99), true)

	_, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.True(t, ibs.warmReadable(addr), "fast path is armed")

	v, source, gotVersion, clean, err := readStateForSet(ibs, addr, key)
	require.NoError(t, err)
	assert.EqualValues(t, 99, v.Uint64())
	assert.Equal(t, MapRead, source)
	assert.Equal(t, version, gotVersion)
	assert.True(t, clean, "a read-set hit is clean: this tx has no write of the slot, so a revert deletes the write")
}

func newTouchTestIBS(t *testing.T, addr accounts.Address, acc accounts.Account, versioned bool) *IntraBlockState {
	_, tx, domains := NewTestRwTx(t)
	addrValue := addr.Value()
	domains.SetTxNum(10)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addrValue[:], accounts.SerialiseV3(&acc), 10, nil))
	reader := NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	var ibs *IntraBlockState
	if versioned {
		ibs = NewWithVersionMap(reader, NewVersionMap(nil))
	} else {
		ibs = New(reader)
	}
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 5)
	return ibs
}

func TestVersionedTouch_RepeatedTouchIsNoop(t *testing.T) {
	t.Parallel()
	for _, versioned := range []bool{false, true} {
		addr := accounts.InternAddress([20]byte{0xb6})
		acc := accounts.NewAccount()
		acc.Incarnation = 1
		ibs := newTouchTestIBS(t, addr, acc, versioned)

		require.NoError(t, ibs.TouchAccount(addr))
		n := ibs.journal.length()
		require.Positive(t, n)
		require.NoError(t, ibs.TouchAccount(addr))
		assert.Equal(t, n, ibs.journal.length(), "versioned=%v", versioned)
	}
}

// The touch result does not depend on the other fields once the tx wrote the balance.
func TestVersionedTouch_OwnBalanceWriteReadsNoFields(t *testing.T) {
	t.Parallel()
	for _, versioned := range []bool{false, true} {
		addr := accounts.InternAddress([20]byte{0xb7})
		acc := accounts.NewAccount()
		acc.Incarnation = 1
		acc.Nonce = 1
		acc.Balance = *uint256.NewInt(5)
		ibs := newTouchTestIBS(t, addr, acc, versioned)

		require.NoError(t, ibs.SetBalance(addr, uint256.Int{}, 0))
		require.NoError(t, ibs.TouchAccount(addr))
		_, ok := ibs.versionedReads.GetNonce(addr)
		assert.False(t, ok, "nonce read, versioned=%v", versioned)
	}
}

// ------------------------------------------------------------------
// Section C: SelfDestruct active in versionMap
// ------------------------------------------------------------------

// a prior tx marked the address selfdestructed (in versionMap), then
// GetCommittedState (committed=true) must return zero immediately.
func TestVersionedRead_C5_DestructedCommittedReturnsZero(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xc5})
	key := accounts.InternKey([32]byte{0x01})
	// tx 1 wrote a slot value
	mvhm.WriteStorage(addr, key, Version{TxIndex: 1, Incarnation: 0}, *uint256.NewInt(99), true)
	// tx 2 selfdestructed the account
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 2, Incarnation: 0}, true, true)

	// Read at tx 5 with committed=true: must return zero, ignoring slot value.
	v, err := ibs.GetCommittedState(addr, key)
	require.NoError(t, err)
	assert.True(t, v.IsZero(), "committed read past selfdestruct returns zero")
}

// a prior tx marked the address selfdestructed; non-committed read
// at a path != CodePath records the SelfDestructPath dependency in the
// readSet and returns zero.
func TestVersionedRead_C6_DestructedRecordsDepAndReturnsZero(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xc6})
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 2, Incarnation: 0}, true, true)

	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.True(t, bal.IsZero(), "non-committed balance read past selfdestruct returns zero")

	// SelfDestructPath dep must be recorded in readSet.
	_, ok := ibs.versionedReads.getHeader(addr, SelfDestructPath, accounts.NilKey)
	assert.True(t, ok, "SelfDestructPath dependency must be recorded")
}

func TestVersionedRead_DestructedStorageRecordsBALReadAfterRevert(t *testing.T) {
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
	require.True(t, ok)
	require.True(t, read.Val.IsZero())
	io := NewVersionedIO(2)
	io.RecordReads(Version{TxIndex: 1}, reads)
	require.Equal(t, VersionValid, vm.ValidateVersion(1, io, validateEqualVersion, true, ""))
	blockAccessList := io.AsBlockAccessList()
	require.Len(t, blockAccessList, 1)
	require.Equal(t, addr.Value(), blockAccessList[0].Address)
	require.Equal(t, []common.Hash{key.Value()}, blockAccessList[0].StorageReads)
}

// A fee credit revives a destroyed account without writing a code-hash cell, so
// the keccak("") read must carry the version of the stale cell below the destruct.
func TestVersionedRead_RevivedStaleCodeHashUsesCodeHashVersion(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xca})
	vm := NewVersionMap(nil)
	codeHashVersion := Version{TxIndex: 0}
	vm.WriteCodeHash(addr, codeHashVersion, accounts.InternCodeHash(common.Hash{0x01}), true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true)
	revived := accounts.NewAccount()
	revived.Balance.SetUint64(1)
	vm.WriteAddress(addr, Version{TxIndex: 2}, &revived, true)
	vm.WriteBalance(addr, Version{TxIndex: 2}, revived.Balance, true)
	ibs := NewWithVersionMap(&emptyReader{}, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 3)
	codeHash, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, accounts.EmptyCodeHash, codeHash)
	reads := ibs.VersionedReads()
	io := NewVersionedIO(4)
	io.RecordReads(Version{TxIndex: 3}, reads)
	require.Equal(t, VersionValid, vm.ValidateVersion(3, io, validateEqualVersion, true, ""))
}

// A field read past a self-destruct returns zero and records the wiped read at
// the destruct's version, so a lower tx that later revives the field invalidates it.
func TestVersionedRead_RevivedFieldWipedReadCaughtAtCommit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		read    func(*IntraBlockState, accounts.Address) (bool, error)
		version func(ReadSet, accounts.Address) (Version, bool)
		revive  func(*VersionMap, accounts.Address, Version)
	}{
		{
			"balance",
			func(ibs *IntraBlockState, a accounts.Address) (bool, error) {
				v, err := ibs.GetBalance(a)
				return v.IsZero(), err
			},
			func(rs ReadSet, a accounts.Address) (Version, bool) {
				r, ok := rs.GetBalance(a)
				return r.Version, ok
			},
			func(vm *VersionMap, a accounts.Address, v Version) { vm.WriteBalance(a, v, *uint256.NewInt(777), true) },
		},
		{
			"nonce",
			func(ibs *IntraBlockState, a accounts.Address) (bool, error) {
				v, err := ibs.GetNonce(a)
				return v == 0, err
			},
			func(rs ReadSet, a accounts.Address) (Version, bool) {
				r, ok := rs.GetNonce(a)
				return r.Version, ok
			},
			func(vm *VersionMap, a accounts.Address, v Version) { vm.WriteNonce(a, v, 1, true) },
		},
		{
			"code size",
			func(ibs *IntraBlockState, a accounts.Address) (bool, error) {
				v, err := ibs.GetCodeSize(a)
				return v == 0, err
			},
			func(rs ReadSet, a accounts.Address) (Version, bool) {
				r, ok := rs.GetCodeSize(a)
				return r.Version, ok
			},
			func(vm *VersionMap, a accounts.Address, v Version) { vm.WriteCodeSize(a, v, 3, true) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			addr := accounts.InternAddress([20]byte{0xcb})
			vm := NewVersionMap(nil)
			destructor := newNoMaterializeTx(vm, 1)
			require.NoError(t, destructor.CreateAccount(addr, true))
			_, err := destructor.Selfdestruct(addr, false)
			require.NoError(t, err)
			vm.FlushVersionedWrites(destructor.VersionedWrites(), true)
			ibs := NewWithVersionMap(&emptyReader{}, vm)
			defer ibs.Close()
			ibs.SetTxContext(1, 3)
			zero, err := tt.read(ibs, addr)
			require.NoError(t, err)
			require.True(t, zero)
			reads := ibs.VersionedReads()

			io := NewVersionedIO(4)
			io.RecordReads(Version{TxIndex: 3}, reads)
			require.Equal(t, VersionValid, vm.ValidateVersion(3, io, validateEqualVersion, true, ""))
			tt.revive(vm, addr, Version{TxIndex: 2})
			require.Equal(t, VersionInvalid, vm.ValidateVersion(3, io, validateEqualVersion, true, ""))
		})
	}
}

// CodePath is exempt from the SelfDestruct short-circuit. Even if SD
// is active, a CodePath read must fall through to the actual code-read
// branches rather than returning zero.
func TestVersionedRead_C4_CodePathBypassesSelfDestruct(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xc4})
	code := []byte{0x60, 0x42, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}
	mvhm.WriteCode(addr, Version{TxIndex: 2, Incarnation: 0}, accounts.NewCode(code), true)
	// Selfdestruct at an EARLIER tx than the code write so the SD doesn't
	// trump the code (the CodePath+SD trump check uses sdres.DepIdx).
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 1, Incarnation: 0}, true, true)

	got, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Equal(t, code, got, "CodePath should bypass SelfDestruct short-circuit")
}

// revival. After SelfDestruct, a later write to Balance / Nonce /
// CodeHash at a higher txIndex revives the account. Subsequent reads must
// see the revived value, not zero.
func TestVersionedRead_C1_RevivalViaBalance(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 10)

	addr := accounts.InternAddress([20]byte{0xc1})
	revivedBalance := uint256.NewInt(777)
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 2, Incarnation: 0}, true, true)
	mvhm.WriteBalance(addr, Version{TxIndex: 5, Incarnation: 0}, *revivedBalance, true)

	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, *revivedBalance, bal, "balance after revival must be the revived value, not zero")
}

// Regression for the refresh* wrappers' missing outcomeReturnZero case: after a
// self-destruct, the per-field refresh (refreshBalance/refreshNonce/
// refreshIncarnation/refreshCodeHash, the per-field account refresh that rebuilds
// an account) must return the typed ZERO, not the caller's stale pre-destruct
// value. Returning the stale value gives the rebuilt account a phantom pre-SD
// codeHash/balance, so the codeHash-change check compares the stale value
// against itself and validation never catches the divergence — a silent wrong
// state root.
func TestVersionedRead_C7_RefreshReturnsZeroPastSelfDestruct(t *testing.T) {
	t.Parallel()

	r := &refreshReader{
		account: &accounts.Account{
			Balance:     *uint256.NewInt(1234),
			Nonce:       7,
			Incarnation: 3,
			CodeHash:    accounts.InternCodeHash([32]byte{0xab}),
		},
	}
	mvhm := NewVersionMap(nil)
	ibs := NewWithVersionMap(r, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xc7})
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 2, Incarnation: 0}, true, true)

	bal, _, _, err := refreshBalance(ibs, addr, *uint256.NewInt(1234))
	require.NoError(t, err)
	assert.True(t, bal.IsZero(), "refreshBalance past SD must return zero, not the stale 1234")

	nonce, _, _, err := refreshNonce(ibs, addr, 7)
	require.NoError(t, err)
	assert.Zero(t, nonce, "refreshNonce past SD must return zero, not the stale 7")

	inc, _, _, err := refreshIncarnation(ibs, addr, 3)
	require.NoError(t, err)
	assert.Zero(t, inc, "refreshIncarnation past SD must return zero, not the stale 3")

	ch, _, _, err := refreshCodeHash(ibs, addr, accounts.InternCodeHash([32]byte{0xab}))
	require.NoError(t, err)
	assert.Equal(t, accounts.NilCodeHash, ch, "refreshCodeHash past SD must return the zero codeHash, not the phantom 0xab")
}

// ------------------------------------------------------------------
// Section D: writeSet hit (intra-tx writes)
// ------------------------------------------------------------------

// a write in the current tx is read back via versionedWrites.
func TestVersionedRead_D2_WriteSetHit(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
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

// versionMap MapRead hit on first call; second read at same tx hits
// the readSet (via the pr.Version == vr.Version branch).
func TestVersionedRead_E1_MapHitThenReadSetSameVersion(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xe1})
	target := uint256.NewInt(42)
	mvhm.WriteBalance(addr, Version{TxIndex: 1, Incarnation: 0}, *target, true)

	// First read populates the readSet via MapRead.
	bal1, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, *target, bal1)

	// Second read: same tx, same version → readSet hit.
	bal2, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, *target, bal2)
}

// a Done MVReadResult for CodePath is trumped if a SelfDestruct at
// a >= DepIdx is also Done — return defaultV (nil code).
func TestVersionedRead_E3a_CodePathTrumpedBySelfDestruct(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 10)

	addr := accounts.InternAddress([20]byte{0xe3})
	code := []byte{0xfe, 0xfe}
	// Code at tx 2; SelfDestruct at tx 3 (strictly later → trumps code).
	mvhm.WriteCode(addr, Version{TxIndex: 2, Incarnation: 0}, accounts.NewCode(code), true)
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 3, Incarnation: 0}, true, true)

	got, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Empty(t, got, "CodePath trumped by SelfDestruct returns nil/empty code")
}

// ------------------------------------------------------------------
// Section G: MVReadResultNone (no entry in versionMap for this key)
// ------------------------------------------------------------------

// readSet hit at MVReadResultNone (ReadSetRead source). Second read
// of an unwritten slot hits readSet from the first read's record.
func TestVersionedRead_G1_ReadSetReadOnSecondCall(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
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
	_, ok := ibs.versionedReads.getHeader(addr, StoragePath, key)
	assert.True(t, ok, "StoragePath read must be recorded")
}

// StoragePath read on an unwritten slot with IncarnationPath written
// by a prior tx → returns zero (account was created/destroyed this block,
// all unwritten slots must be zero) and records the IncarnationPath dep.
func TestVersionedRead_G6_StorageZeroOnIncarnationWritten(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0x96})
	key := accounts.InternKey([32]byte{0xab})
	// Prior tx wrote IncarnationPath (CreateAccount / Selfdestruct event)
	mvhm.WriteIncarnation(addr, Version{TxIndex: 2, Incarnation: 0}, 1, true)

	got, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	assert.True(t, got.IsZero(), "unwritten slot reads zero after Incarnation rewrite")

	// IncarnationPath dep must be recorded.
	_, ok := ibs.versionedReads.getHeader(addr, IncarnationPath, accounts.NilKey)
	assert.True(t, ok, "IncarnationPath dependency must be recorded")
}

// BalancePath read with no map/writeSet/readSet entry but a prior tx
// wrote an AddressPath account → use that account's balance.
func TestVersionedRead_G7_BalanceViaResolvedAddressPath(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0x97})
	priorAcc := &accounts.Account{Balance: *uint256.NewInt(555), Nonce: 3}
	mvhm.WriteAddress(addr, Version{TxIndex: 2, Incarnation: 0}, priorAcc, true)

	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, *uint256.NewInt(555), bal, "balance resolved via AddressPath account")
}

// A lower tx creates the account and is flushed after the field probe missed
// but before the fallbacks probe the version map again. The account record and
// the self-destruct cell hold creation-time state at the same version as the
// field cell flushed with them, so validation cannot catch a stale field: the
// read must return and record the cell.
func TestVersionedRead_G9_FieldCellFlushedBeforeFallback(t *testing.T) {
	t.Parallel()
	codeHash := accounts.InternCodeHash([32]byte{0xc9})
	tests := []struct {
		name     string
		write    func(ws *WriteSet, addr accounts.Address, hdr WriteHeader)
		read     func(ibs *IntraBlockState, addr accounts.Address) (any, error)
		recorded func(rs *ReadSet, addr accounts.Address) (ReadHeader, any)
		want     any
	}{
		{
			name: "balance",
			write: func(ws *WriteSet, addr accounts.Address, hdr WriteHeader) {
				hdr.Path = BalancePath
				ws.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: hdr, Val: *uint256.NewInt(1)})
			},
			read: func(ibs *IntraBlockState, addr accounts.Address) (any, error) { return ibs.GetBalance(addr) },
			recorded: func(rs *ReadSet, addr accounts.Address) (ReadHeader, any) {
				vr, _ := rs.GetBalance(addr)
				return vr.ReadHeader, vr.Val
			},
			want: *uint256.NewInt(1),
		},
		{
			name: "nonce",
			write: func(ws *WriteSet, addr accounts.Address, hdr WriteHeader) {
				hdr.Path = NoncePath
				ws.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: hdr, Val: 1})
			},
			read: func(ibs *IntraBlockState, addr accounts.Address) (any, error) { return ibs.GetNonce(addr) },
			recorded: func(rs *ReadSet, addr accounts.Address) (ReadHeader, any) {
				vr, _ := rs.GetNonce(addr)
				return vr.ReadHeader, vr.Val
			},
			want: uint64(1),
		},
		{
			name: "code hash",
			write: func(ws *WriteSet, addr accounts.Address, hdr WriteHeader) {
				hdr.Path = CodeHashPath
				ws.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: hdr, Val: codeHash})
			},
			read: func(ibs *IntraBlockState, addr accounts.Address) (any, error) { return ibs.GetCodeHash(addr) },
			recorded: func(rs *ReadSet, addr accounts.Address) (ReadHeader, any) {
				vr, _ := rs.GetCodeHash(addr)
				return vr.ReadHeader, vr.Val
			},
			want: codeHash,
		},
		{
			name: "incarnation",
			write: func(ws *WriteSet, addr accounts.Address, hdr WriteHeader) {
				hdr.Path = IncarnationPath
				ws.SetIncarnation(addr, &VersionedWrite[uint64]{WriteHeader: hdr, Val: 1})
			},
			read: func(ibs *IntraBlockState, addr accounts.Address) (any, error) { return ibs.GetIncarnation(addr) },
			recorded: func(rs *ReadSet, addr accounts.Address) (ReadHeader, any) {
				vr, _ := rs.GetIncarnation(addr)
				return vr.ReadHeader, vr.Val
			},
			want: uint64(1),
		},
		{
			name: "balance kept by a self-destruct to itself",
			write: func(ws *WriteSet, addr accounts.Address, hdr WriteHeader) {
				hdr.Path = SelfDestructPath
				ws.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: hdr, Val: true})
				hdr.Path = BalancePath
				ws.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: hdr, Val: *uint256.NewInt(1)})
			},
			read: func(ibs *IntraBlockState, addr accounts.Address) (any, error) { return ibs.GetBalance(addr) },
			recorded: func(rs *ReadSet, addr accounts.Address) (ReadHeader, any) {
				vr, _ := rs.GetBalance(addr)
				return vr.ReadHeader, vr.Val
			},
			want: *uint256.NewInt(1),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			addr := accounts.InternAddress([20]byte{0x99})
			vm := NewVersionMap(nil)
			ibs := NewWithVersionMap(&emptyReader{}, vm)
			defer ibs.Close()
			ibs.SetTxContext(1, 1)

			hdr := WriteHeader{Address: addr, Path: AddressPath, Version: Version{TxIndex: 0}}
			ws := &WriteSet{}
			ws.SetAddress(addr, &VersionedWrite[*accounts.Account]{WriteHeader: hdr, Val: &accounts.Account{CodeHash: accounts.EmptyCodeHash}})
			tt.write(ws, addr, hdr)
			vm.fieldMissHook = func() {
				vm.fieldMissHook = nil
				vm.FlushVersionedWrites(ws, true)
			}

			got, err := tt.read(ibs, addr)
			require.NoError(t, err)
			require.Nil(t, vm.fieldMissHook, "the flush must land after the field probe")
			assert.Equal(t, tt.want, got)
			recHdr, recVal := tt.recorded(&ibs.versionedReads, addr)
			assert.Equal(t, ReadHeader{Source: MapRead, Version: Version{TxIndex: 0}}, recHdr)
			assert.Equal(t, tt.want, recVal)
		})
	}
}

// creditedDestructAndRevival returns a version map where tx 0 created and destroyed addr, and its
// fee credit wrote the account record with a balance of 5 at the destruct. It also returns the
// writes of tx 1's fee credit, which re-create addr with the given account.
func creditedDestructAndRevival(addr accounts.Address, revived *accounts.Account) (*VersionMap, *WriteSet) {
	vm := NewVersionMap(nil)
	credited := &accounts.Account{Balance: *uint256.NewInt(5), CodeHash: accounts.EmptyCodeHash}
	vm.WriteSelfDestruct(addr, Version{TxIndex: 0}, true, true)
	vm.WriteAddress(addr, Version{TxIndex: 0}, credited, true)
	vm.WriteBalance(addr, Version{TxIndex: 0}, credited.Balance, true)
	revival := &WriteSet{}
	revival.SetAddress(addr, &VersionedWrite[*accounts.Account]{WriteHeader: WriteHeader{Address: addr, Path: AddressPath, Version: Version{TxIndex: 1}}, Val: revived})
	revival.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 1}}, Val: revived.Balance})
	return vm, revival
}

// Tx 1's re-creation is flushed after tx 2's account read found no account: the read must return
// the re-created account and record it, not fall back to the committed state.
func TestVersionedRead_RevivalFlushedAfterAbsentAccountRead(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x9a})
	revived := &accounts.Account{Balance: *uint256.NewInt(12), CodeHash: accounts.EmptyCodeHash}
	vm, revival := creditedDestructAndRevival(addr, revived)
	vm.absentAccountHook = func() {
		vm.absentAccountHook = nil
		vm.FlushVersionedWrites(revival, true)
	}

	ibs := NewWithVersionMap(&emptyReader{}, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 2)
	account, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	require.Nil(t, vm.absentAccountHook, "the flush must land after the account read")
	require.Equal(t, revived, account)
	recorded, ok := ibs.versionedReads.GetAddress(addr)
	require.True(t, ok)
	require.Equal(t, VersionedRead[AccountView]{ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 1}}, Val: NewAccountView(revived)}, recorded)
}

// An earlier read of the account filled the self-destruct memo before tx 1's destruct was flushed
// and validated. The account read must still see the destruct and record it, so that tx 3 fails
// validation when tx 1's re-execution withdraws the destruct.
func TestVersionedRead_DestructFlushedAfterMemoIsRecorded(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x9d})
	vm := NewVersionMap(nil)
	vm.WriteBalance(addr, Version{TxIndex: 0}, *uint256.NewInt(109), true)
	ibs := NewWithVersionMap(newAccountStateReader(addr), vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 3)
	_, err := ibs.GetBalance(addr)
	require.NoError(t, err)

	destruct := &WriteSet{}
	destruct.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: Version{TxIndex: 1}}, Val: true})
	destruct.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 1}, valStatus: ValueDeleted}})
	destruct.SetIncarnation(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath, Version: Version{TxIndex: 1}}})
	vm.FlushVersionedWrites(destruct, false)
	vm.MarkWritesValidated(destruct, nil)

	_, err = ibs.Exist(addr)
	require.NoError(t, err)
	for _, path := range []AccountPath{SelfDestructPath, BalancePath, IncarnationPath} {
		vm.Delete(addr, path, accounts.NilKey, 1, false)
	}
	require.Equal(t, VersionInvalid, validateReads(vm, ibs, 3))
}

// Tx 0 created the account and self-destructed it to itself, which keeps its balance (EIP-8246) but
// drops the account record. Tx 2's first load records an absence and returns the kept account.
// Tx 1's re-creation of the account record is flushed before the second load and then withdrawn:
// the second load must also return the kept account, which fails validation only while the
// re-creation exists.
func TestVersionedRead_KeptBalanceSurvivesWithdrawnRecreation(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x9e})
	vm := NewVersionMap(nil)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 0}, true, true)
	vm.WriteBalance(addr, Version{TxIndex: 0}, *uint256.NewInt(5), true)
	ibs := NewWithVersionMap(&emptyReader{}, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 2)
	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists)

	vm.WriteAddress(addr, Version{TxIndex: 1}, &accounts.Account{CodeHash: accounts.EmptyCodeHash}, false)
	account, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Equal(t, *uint256.NewInt(5), account.Balance)
	require.Equal(t, VersionInvalid, validateReads(vm, ibs, 2))
	vm.Delete(addr, AddressPath, accounts.NilKey, 1, false)
	require.Equal(t, VersionValid, validateReads(vm, ibs, 2))
}

// Tx 0's destruct removed the account: its balance cell is stamped deleted, although it holds a
// residual, as when the account is credited after the destruct in the same tx. Tx 2's recorded
// absence is final, so tx 1's re-creation, flushed after the read, is not adopted mid-execution,
// and the read fails validation.
func TestVersionedRead_DeletedBalanceKeepsRecordedAbsence(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x9f})
	vm := NewVersionMap(nil)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 0}, true, true)
	removal := &WriteSet{}
	removal.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 0}, valStatus: ValueDeleted}, Val: *uint256.NewInt(5)})
	vm.FlushVersionedWrites(removal, true)
	recreated := &accounts.Account{Balance: *uint256.NewInt(12), CodeHash: accounts.EmptyCodeHash}
	recreation := &WriteSet{}
	recreation.SetAddress(addr, &VersionedWrite[*accounts.Account]{WriteHeader: WriteHeader{Address: addr, Path: AddressPath, Version: Version{TxIndex: 1}}, Val: recreated})
	recreation.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 1}}, Val: recreated.Balance})
	vm.absentAccountHook = func() {
		vm.absentAccountHook = nil
		vm.FlushVersionedWrites(recreation, true)
	}

	ibs := NewWithVersionMap(&emptyReader{}, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 2)
	account, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	require.Nil(t, vm.absentAccountHook, "the flush must land after the account read")
	require.Nil(t, account)
	require.Equal(t, VersionInvalid, validateReads(vm, ibs, 2))
}

// storage fallback. Empty reader + no prior writes/reads → falls through
// to the readStorage callback (returns zero from emptyReader) and records
// defaultV in readSet.
func TestVersionedRead_G8_StorageFallbackEmptyReader(t *testing.T) {
	t.Parallel()
	mvhm := NewVersionMap(nil)
	ibs := NewWithVersionMap(&emptyReader{}, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0x98})
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.True(t, bal.IsZero(), "empty reader storage fallback returns zero")
}

// refresh path: the legacy readStorage==nil branch for
// BalancePath/NoncePath/CodeHashPath/IncarnationPath records the
// caller-supplied defaultV (the "current" account field) into the
// versionedReads ReadSet, with vr.Source = StorageRead.  ValidateVersion
// and tiebreaker logic depend on the RECORDED value matching what
// downstream tx readers will see — recording the typed zero instead of
// the caller's currentBalance is a behavioral divergence that breaks
// integration tests (wrong trie root in execution/engineapi).
//
// This test calls per-field-account-refresh-style scenarios by going
// through getVersionedAccount which triggers refreshBalance/refreshNonce/
// refreshCodeHash/refreshIncarnation with the storage-read account's
// fields as defaultV.  Asserts that subsequent reads through the
// versionedReads ReadSet see the same typed defaultV that the legacy
// code recorded.
func TestVersionedRead_G4_RefreshRecordsTypedDefaultInReadSet(t *testing.T) {
	t.Parallel()

	// emptyReader returns nil account — the per-field refresh won't
	// run for nil-account paths.  We need a reader that returns a
	// non-nil account so the per-field refresh*
	// calls run with non-zero defaultV.
	r := &refreshReader{
		account: &accounts.Account{
			Balance:     *uint256.NewInt(1234),
			Nonce:       7,
			Incarnation: 3,
			CodeHash:    accounts.InternCodeHash([32]byte{0xab}),
		},
	}
	mvhm := NewVersionMap(nil)
	ibs := NewWithVersionMap(r, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xa4})

	// Lean read footprint: GetBalance reads only the balance field from the
	// cell pipeline — it no longer drags in a whole-account refresh, so nonce /
	// incarnation / codeHash are NOT recorded. Balance is recorded because
	// GetBalance genuinely read it.
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, *uint256.NewInt(1234), bal,
		"GetBalance returns the storage-read account's balance")

	balRead, ok := ibs.versionedReads.GetBalance(addr)
	require.True(t, ok, "BalancePath read must be recorded")
	assert.Equal(t, *uint256.NewInt(1234), balRead.Val,
		"recorded BalancePath value must be the read value, not zero")

	_, ok = ibs.versionedReads.GetNonce(addr)
	require.False(t, ok, "NoncePath must NOT be recorded by a balance-only read")
	_, ok = ibs.versionedReads.GetIncarnation(addr)
	require.False(t, ok, "IncarnationPath must NOT be recorded by a balance-only read")
	_, ok = ibs.versionedReads.GetCodeHash(addr)
	require.False(t, ok, "CodeHashPath must NOT be recorded by a balance-only read")
}

// refreshReader returns a fixed account for ReadAccountData and zero/empty
// for everything else.  Used by TestVersionedRead_G4 to exercise the
// refresh path with non-zero typed defaultVs.
type refreshReader struct {
	account      *accounts.Account
	accountReads int
}

func (r *refreshReader) ReadAccountData(accounts.Address) (*accounts.Account, error) {
	r.accountReads++
	return r.account, nil
}

func (r *refreshReader) ReadAccountByPlainAddr(address common.Address) (*accounts.Account, error) {
	return r.ReadAccountData(accounts.InternAddress(address))
}

func (r *refreshReader) HasAccount(address common.Address) (bool, error) {
	acc, err := r.ReadAccountByPlainAddr(address)
	return acc != nil, err
}

func (r *refreshReader) ReadAccountDataForDebug(accounts.Address) (*accounts.Account, error) {
	return r.account, nil
}

func (r *refreshReader) ReadAccountStorage(accounts.Address, accounts.StorageKey) (uint256.Int, bool, error) {
	return uint256.Int{}, false, nil
}
func (r *refreshReader) ReadAccountCode(accounts.Address) ([]byte, error)        { return nil, nil }
func (r *refreshReader) ReadAccountCodeSize(accounts.Address) (int, error)       { return 0, nil }
func (r *refreshReader) ReadAccountIncarnation(accounts.Address) (uint64, error) { return 0, nil }
func (r *refreshReader) SetTrace(bool, string)                                   {}
func (r *refreshReader) Trace() bool                                             { return false }
func (r *refreshReader) TracePrefix() string                                     { return "" }

func TestVersionedRead_B_TouchReadsAnAbsentAccountOnce(t *testing.T) {
	t.Parallel()
	r := &refreshReader{}
	ibs := NewWithVersionMap(r, NewVersionMap(nil))
	defer ibs.Close()
	ibs.SetTxContext(1, 5)
	addr := accounts.InternAddress([20]byte{0xb9})

	require.NoError(t, ibs.TouchAccount(addr))
	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, 1, r.accountReads)
}

// revival via NoncePath rewrite at a higher TxIdx than the SD.
func TestVersionedRead_C2_RevivalViaNonce(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 10)

	addr := accounts.InternAddress([20]byte{0xc2})
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 2, Incarnation: 0}, true, true)
	mvhm.WriteNonce(addr, Version{TxIndex: 5, Incarnation: 0}, 7, true)

	n, err := ibs.GetNonce(addr)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), n, "nonce after revival via NoncePath rewrite")
}

// revival via CodeHashPath rewrite at a higher TxIdx than the SD.
func TestVersionedRead_C3_RevivalViaCodeHash(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 10)

	addr := accounts.InternAddress([20]byte{0xc3})
	revivedHash := accounts.InternCodeHash([32]byte{0xab, 0xcd, 0xef})
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 2, Incarnation: 0}, true, true)
	mvhm.WriteCodeHash(addr, Version{TxIndex: 5, Incarnation: 0}, revivedHash, true)

	got, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	assert.Equal(t, revivedHash, got, "codehash after revival via CodeHashPath rewrite")
}

// when the readSet already records a different Version than the current
// versionMap value at MapRead, versionedRead panics with ErrDependency so
// the executor knows to re-execute. Captured via recover().
func TestVersionedRead_E2_StaleMapReadCaughtAtCommit(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 10)

	addr := accounts.InternAddress([20]byte{0xe2})
	// Two writes at different versions; the later version wins in versionMap.
	mvhm.WriteBalance(addr, Version{TxIndex: 2, Incarnation: 0}, *uint256.NewInt(10), true)

	// Manually seed readSet with a stale version (TxIndex 1).
	ibs.versionedReads.SetBalance(addr, VersionedRead[uint256.Int]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 1, Incarnation: 0}},
		Val:        *uint256.NewInt(99),
	})

	// Read-once (Block-STM): a path already recorded this execution attempt is
	// served from the read-set without re-probing the version map, so a repeat
	// read no longer aborts eagerly. The stale read is caught at commit —
	// ValidateVersion re-checks it against the newer version-map write (TxIndex 2)
	// and returns VersionInvalid, which is what drives re-execution.
	got, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, *uint256.NewInt(99), got, "read-once returns the recorded value")

	var io VersionedIO
	ibs.MergeTxIOInto(&io, ibs.VersionedWrites())
	valid := mvhm.ValidateVersion(10, &io, func(rv, wv Version) VersionValidity {
		if rv == wv {
			return VersionValid
		}
		return VersionInvalid
	}, false, "")
	assert.Equal(t, VersionInvalid, valid, "commit-time validation catches the stale read")
}

// F: MVReadResultDependency status (versionMap saw an in-progress dep at a
// higher TxIdx than the current tx) records the dependency as a validation
// verdict (HadInvalidRead) rather than panicking. With no pause hook the read
// falls through to the in-flight value. Trigger: an Estimate-status entry
// (complete=false) at a TxIdx <= ours.
func TestVersionedRead_F_MVReadResultDependencyRecordsDep(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xf0})
	// Estimate entry (complete=false) at txIndex 2 < 5 → Dependency.
	mvhm.WriteBalance(addr, Version{TxIndex: 2, Incarnation: 0}, *uint256.NewInt(50), false)

	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.True(t, ibs.HadInvalidRead(), "dependency read must set the invalid-read verdict")
	assert.Equal(t, 2, ibs.DepTxIndex(), "verdict names the in-flight predecessor")
	assert.Equal(t, uint256.NewInt(50), &bal, "no pause hook: falls through to the in-flight value")
}

// A run that skips the wait on an in-flight self-destruct must still record it as a dependency: the
// destruct burns the balance even when the balance cell already reads as Done.
func TestVersionedRead_InFlightSelfDestructWithoutWaitRecordsDep(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	ibs := NewWithVersionMap(NewReaderV3(domains.AsGetter(tx)), mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xf1})
	writer := Version{TxIndex: 2}
	destruct := &WriteSet{}
	destruct.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: writer}, Val: true})
	burn := &WriteSet{}
	burn.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: writer, valStatus: ValueDeleted}, Val: *uint256.NewInt(1)})
	mvhm.FlushVersionedWrites(destruct, false)
	mvhm.FlushVersionedWrites(burn, true)

	_, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.True(t, ibs.HadInvalidRead(), "an unawaited in-flight self-destruct must set the invalid-read verdict")
	assert.Equal(t, 2, ibs.DepTxIndex(), "verdict names the self-destructing predecessor")
}

// writeSet hit at MVReadResultDone, but readSet has a stale version:
// records the dependency verdict (a write was based on a stale read) rather
// than panicking. The current tx wrote, but the readSet for the same path
// holds a version older than the versionMap's Done entry.
func TestVersionedRead_D1_WriteSetHitWithStaleReadSetRecordsDep(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xd1})
	// versionMap Done at tx 3 — higher than the readSet's stale tx-1 entry.
	mvhm.WriteBalance(addr, Version{TxIndex: 3, Incarnation: 0}, *uint256.NewInt(30), true)

	// Seed a stale readSet entry at a lower version.
	ibs.versionedReads.SetBalance(addr, VersionedRead[uint256.Int]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 1, Incarnation: 0}},
		Val:        *uint256.NewInt(99),
	})
	// Seed a current-tx writeSet entry (intra-tx write) so the writeSet
	// branch fires.
	err := ibs.SetBalance(addr, *uint256.NewInt(77), 0)
	require.NoError(t, err)

	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.True(t, ibs.HadInvalidRead(), "stale-read writeSet hit must set the invalid-read verdict")
	assert.Equal(t, 3, ibs.DepTxIndex(), "verdict names the versionMap Done predecessor")
	assert.Equal(t, uint256.NewInt(77), &bal, "no pause hook: returns the tx's own intra-tx write")
}

// E2 for a slot this tx did not write on an address it did: the repeat SLOAD
// serves the recorded read, and commit-time validation catches it if stale.
func TestVersionedRead_E2_StorageOnWrittenAddressCaughtAtCommit(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xe3})
	written, read := accounts.InternKey([32]byte{0x01}), accounts.InternKey([32]byte{0x02})
	mvhm.WriteStorage(addr, read, Version{TxIndex: 3, Incarnation: 0}, *uint256.NewInt(30), true)

	require.NoError(t, ibs.SetState(addr, written, *uint256.NewInt(77)))
	ibs.versionedReads.SetStorage(addr, read, VersionedRead[uint256.Int]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 1, Incarnation: 0}},
		Val:        *uint256.NewInt(99),
	})
	got, err := ibs.GetState(addr, read)
	require.NoError(t, err)
	assert.EqualValues(t, 99, got.Uint64(), "read-once returns the recorded value")

	var io VersionedIO
	ibs.MergeTxIOInto(&io, ibs.VersionedWrites())
	assert.Equal(t, VersionInvalid, mvhm.ValidateVersion(5, &io, validateEqualVersion, true, ""), "commit-time validation catches the stale read")
}

// E2 for the code size of an address this tx wrote another field of.
func TestVersionedRead_E2_CodeSizeOnWrittenAddressCaughtAtCommit(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xe4})
	mvhm.WriteCodeSize(addr, Version{TxIndex: 3, Incarnation: 0}, 30, true)

	require.NoError(t, ibs.SetBalance(addr, *uint256.NewInt(77), 0))
	ibs.versionedReads.SetCodeSize(addr, VersionedRead[int]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 1, Incarnation: 0}},
		Val:        99,
	})
	got, err := ibs.GetCodeSize(addr)
	require.NoError(t, err)
	assert.Equal(t, 99, got, "read-once returns the recorded value")

	var io VersionedIO
	ibs.MergeTxIOInto(&io, ibs.VersionedWrites())
	assert.Equal(t, VersionInvalid, mvhm.ValidateVersion(5, &io, validateEqualVersion, true, ""), "commit-time validation catches the stale read")
}

// CREATE over an account with storage but no code or nonce: the slot read
// before the CREATE belongs to the old incarnation, so committed storage is zero.
func TestVersionedRead_B_CommittedStorageAfterCreateIsZero(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	addr := accounts.InternAddress([20]byte{0xb7})
	key := accounts.InternKey([32]byte{0x01})
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(1)
	acc.Incarnation = 1
	addrValue, keyValue := addr.Value(), key.Value()
	domains.SetTxNum(10)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addrValue[:], accounts.SerialiseV3(&acc), 10, nil))
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, append(addrValue[:], keyValue[:]...), []byte{0x07}, 10, nil))
	ibs := NewWithVersionMap(NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})), NewVersionMap(nil))
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 5)

	v, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.EqualValues(t, 7, v.Uint64())
	require.NoError(t, ibs.CreateAccount(addr, true))
	committed, err := ibs.GetCommittedState(addr, key)
	require.NoError(t, err)
	assert.True(t, committed.IsZero())
}

// A dirty address serves the recorded read of a field it has not written, as a
// clean one does: a later estimate in the version map is left to validation.
func TestVersionedRead_B_DirtyAddressServesRecordedRead(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	addr := accounts.InternAddress([20]byte{0xb4})
	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1
	acc.Balance = *uint256.NewInt(1234)
	addrValue := addr.Value()
	domains.SetTxNum(10)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addrValue[:], accounts.SerialiseV3(&acc), 10, nil))
	mvhm := NewVersionMap(nil)
	ibs := NewWithVersionMap(NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})), mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	nonce, err := ibs.GetNonce(addr)
	require.NoError(t, err)
	require.EqualValues(t, 1, nonce)
	require.NoError(t, ibs.SetBalance(addr, *uint256.NewInt(77), 0))
	require.False(t, ibs.warmReadable(addr), "the balance write makes the address dirty")

	mvhm.WriteNonce(addr, Version{TxIndex: 2}, 9, false)
	nonce, err = ibs.GetNonce(addr)
	require.NoError(t, err)
	assert.EqualValues(t, 1, nonce)

	balance, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.EqualValues(t, 77, balance.Uint64(), "an own write still wins")

	_, err = ibs.GetCode(addr)
	require.NoError(t, err)
	mvhm.WriteCode(addr, Version{TxIndex: 2}, accounts.Code{Bytes: []byte{0x60}}, false)
	code, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Empty(t, code, "code is served from the recorded read too")
}

// Touching an account whose own balance write is already zero changes nothing,
// so it adds no journal entry.
func TestVersionedRead_B_RepeatedTouchIsNoop(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	addr := accounts.InternAddress([20]byte{0xb6})
	acc := accounts.NewAccount()
	acc.Incarnation = 1
	addrValue := addr.Value()
	domains.SetTxNum(10)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addrValue[:], accounts.SerialiseV3(&acc), 10, nil))
	ibs := NewWithVersionMap(NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})), NewVersionMap(nil))
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	require.NoError(t, ibs.TouchAccount(addr))
	n := ibs.journal.length()
	require.Positive(t, n)
	require.NoError(t, ibs.TouchAccount(addr))
	assert.Equal(t, n, ibs.journal.length())
	balance, ok := ibs.versionedWrites.GetBalance(addr)
	require.True(t, ok)
	assert.True(t, balance.Val.IsZero())
}

// A CREATE2 onto an account a prior tx destroyed carries the balance a later tx's
// fee revived it with, even when this tx first read the account while it was
// destroyed.
func TestVersionedRead_CreateCarriesRevivedBalanceAfterEarlierDestroyedRead(t *testing.T) {
	t.Parallel()
	for _, firstRead := range []string{"nonce", "balance", "codehash"} {
		t.Run(firstRead, func(t *testing.T) {
			_, tx, domains := NewTestRwTx(t)
			addr := accounts.InternAddress([20]byte{0xc7})
			vm := NewVersionMap(nil)
			vm.WriteSelfDestruct(addr, Version{TxIndex: 0}, true, true)
			ibs := NewWithVersionMap(NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})), vm)
			t.Cleanup(ibs.Close)
			ibs.SetTxContext(1, 2)

			var err error
			switch firstRead {
			case "nonce":
				_, err = ibs.GetNonce(addr)
			case "balance":
				_, err = ibs.GetBalance(addr)
			case "codehash":
				_, err = ibs.GetCodeHash(addr)
			}
			require.NoError(t, err)

			revived := accounts.NewAccount()
			revived.Balance = *uint256.NewInt(1000)
			vm.WriteBalance(addr, Version{TxIndex: 1}, revived.Balance, true)
			vm.WriteAddress(addr, Version{TxIndex: 1}, &revived, true)

			require.NoError(t, ibs.CreateAccount(addr, true))
			bal, err := ibs.GetBalance(addr)
			require.NoError(t, err)
			require.EqualValues(t, 1000, bal.Uint64())
		})
	}
}
