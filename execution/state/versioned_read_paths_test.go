package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestVersionedRead_A1_LegacyWithStorage(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xa1})

	ibs := New(&emptyReader{})
	defer ibs.Close()
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.True(t, bal.IsZero(), "legacy path with empty reader returns zero")
}

func TestVersionedRead_A2_LegacyGetCodeReturnsEmpty(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xa2})

	ibs := New(&emptyReader{})
	defer ibs.Close()
	code, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Empty(t, code, "empty reader => empty code")
}

// Selfdestructing then reading another field triggers the local-map deleted short-circuit.
func TestVersionedRead_B_DeletedStateObjectReturnsDefault(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 0)

	addr := accounts.InternAddress(common.HexToAddress("0xdead"))
	ibs.CreateAccount(addr, true)
	err := ibs.SetBalance(addr, *uint256.NewInt(50), 0)
	require.NoError(t, err)
	_, err = ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.True(t, bal.IsZero(), "balance after selfdestruct is zero")
}

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
	mvhm.WriteStorage(addr, key, Version{TxIndex: 1, Incarnation: 0}, *uint256.NewInt(99), true)
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 2, Incarnation: 0}, true, true)

	v, err := ibs.GetCommittedState(addr, key)
	require.NoError(t, err)
	assert.True(t, v.IsZero(), "committed read past selfdestruct returns zero")
}

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

	_, ok := ibs.versionedReads.getHeader(addr, SelfDestructPath, accounts.NilKey)
	assert.True(t, ok, "SelfDestructPath dependency must be recorded")
}

// CodePath is exempt from the SelfDestruct short-circuit and must fall through to the real code-read branch.
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
	// SD is earlier than the code write here, so it doesn't trump the code (trump check uses sdres.DepIdx).
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 1, Incarnation: 0}, true, true)

	got, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Equal(t, code, got, "CodePath should bypass SelfDestruct short-circuit")
}

// A write to Balance/Nonce/CodeHash after a SelfDestruct, at a higher txIndex, revives the account.
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

// After a self-destruct, refresh* must return the typed zero, not the stale pre-destruct
// value — otherwise validation can't detect the divergence and the state root is silently wrong.
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

	bal1, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, *target, bal1)

	bal2, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, *target, bal2)
}

// A SelfDestruct at DepIdx >= the code write's DepIdx trumps CodePath, returning nil code.
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
	mvhm.WriteCode(addr, Version{TxIndex: 2, Incarnation: 0}, accounts.NewCode(code), true)
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 3, Incarnation: 0}, true, true)

	got, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Empty(t, got, "CodePath trumped by SelfDestruct returns nil/empty code")
}

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

	v1, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	assert.True(t, v1.IsZero())

	v2, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	assert.True(t, v2.IsZero())

	_, ok := ibs.versionedReads.getHeader(addr, StoragePath, key)
	assert.True(t, ok, "StoragePath read must be recorded")
}

// An IncarnationPath rewrite means the account was created/destroyed this block, so unwritten slots must read zero.
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
	mvhm.WriteIncarnation(addr, Version{TxIndex: 2, Incarnation: 0}, 1, true)

	got, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	assert.True(t, got.IsZero(), "unwritten slot reads zero after Incarnation rewrite")

	_, ok := ibs.versionedReads.getHeader(addr, IncarnationPath, accounts.NilKey)
	assert.True(t, ok, "IncarnationPath dependency must be recorded")
}

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

// The legacy readStorage==nil refresh records the caller's defaultV in the ReadSet, not
// the typed zero — ValidateVersion's tiebreaker depends on the recorded value matching what downstream reads see.
func TestVersionedRead_G4_RefreshRecordsTypedDefaultInReadSet(t *testing.T) {
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

	addr := accounts.InternAddress([20]byte{0xa4})

	// GetBalance has a lean read footprint: it records only BalancePath, not a whole-account refresh.
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

type refreshReader struct {
	account *accounts.Account
}

func (r *refreshReader) ReadAccountData(accounts.Address) (*accounts.Account, error) {
	return r.account, nil
}
func (r *refreshReader) ReadAccountDataForDebug(accounts.Address) (*accounts.Account, error) {
	return r.account, nil
}
func (r *refreshReader) ReadAccountStorage(accounts.Address, accounts.StorageKey) (uint256.Int, bool, error) {
	return uint256.Int{}, false, nil
}
func (r *refreshReader) HasStorage(accounts.Address) (bool, error)               { return false, nil }
func (r *refreshReader) ReadAccountCode(accounts.Address) ([]byte, error)        { return nil, nil }
func (r *refreshReader) ReadAccountCodeSize(accounts.Address) (int, error)       { return 0, nil }
func (r *refreshReader) ReadAccountIncarnation(accounts.Address) (uint64, error) { return 0, nil }
func (r *refreshReader) SetTrace(bool, string)                                   {}
func (r *refreshReader) Trace() bool                                             { return false }
func (r *refreshReader) TracePrefix() string                                     { return "" }

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

func TestVersionedRead_E2_StaleMapReadCaughtAtCommit(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 10)

	addr := accounts.InternAddress([20]byte{0xe2})
	mvhm.WriteBalance(addr, Version{TxIndex: 2, Incarnation: 0}, *uint256.NewInt(10), true)

	ibs.versionedReads.SetBalance(addr, VersionedRead[uint256.Int]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 1, Incarnation: 0}},
		Val:        *uint256.NewInt(99),
	})

	// Read-once (Block-STM): a path already recorded this attempt is served from the read-set
	// without re-probing the version map; the stale read is instead caught at commit-time validation.
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
	}, true, false, false, "")
	assert.Equal(t, VersionInvalid, valid, "commit-time validation catches the stale read")
}

// An Estimate entry (complete=false) at a TxIdx <= ours reports MVReadResultDependency and panics with ErrDependency.
func TestVersionedRead_F_MVReadResultDependencyPanics(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xf0})
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

// A writeSet hit whose readSet entry is older than the versionMap's Done write panics with ErrDependency.
func TestVersionedRead_D1_WriteSetHitWithStaleReadSetPanics(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)

	addr := accounts.InternAddress([20]byte{0xd1})
	mvhm.WriteBalance(addr, Version{TxIndex: 3, Incarnation: 0}, *uint256.NewInt(30), true)

	err := ibs.SetBalance(addr, *uint256.NewInt(77), 0)
	require.NoError(t, err)

	// Seeded after the write: seeding first would panic inside SetBalance's own account refresh before this branch runs.
	ibs.versionedReads.SetBalance(addr, VersionedRead[uint256.Int]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 1, Incarnation: 0}},
		Val:        *uint256.NewInt(99),
	})
	defer func() {
		r := recover()
		require.NotNil(t, r, "must panic when writeSet hit conflicts with stale readSet at versionMap Done")
		err, ok := r.(error)
		require.True(t, ok)
		assert.ErrorIs(t, err, ErrDependency)
	}()
	_, _ = ibs.GetBalance(addr)
}

// nil≡empty in readValueUnchanged is gated like validation's dead-equivalence: none pre-EIP-161, none for AuRa's SystemAddress.
func TestReadValueUnchanged_NilEmptyArmGated(t *testing.T) {
	newIBS := func(addr accounts.Address, eip161 bool, isAura bool) *IntraBlockState {
		ibs := NewWithVersionMap(&emptyReader{}, NewVersionMap(nil))
		t.Cleanup(func() { ibs.Release(false) })
		ibs.SetTxContext(0, 2)
		ibs.eip161 = eip161
		ibs.isAura = isAura
		ibs.versionedReads.SetAddress(addr, VersionedRead[AccountView]{
			ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion},
		})
		return ibs
	}
	r := &readPathResult{mapAddressVal: &accounts.Account{CodeHash: accounts.EmptyCodeHash}}
	sys := params.SystemAddress
	other := accounts.InternAddress([20]byte{0x18, 0x01})
	require.True(t, newIBS(other, true, false).readValueUnchanged(other, AddressPath, accounts.NilKey, r))
	require.False(t, newIBS(other, false, false).readValueUnchanged(other, AddressPath, accounts.NilKey, r))
	require.False(t, newIBS(sys, true, true).readValueUnchanged(sys, AddressPath, accounts.NilKey, r))
	require.True(t, newIBS(sys, true, false).readValueUnchanged(sys, AddressPath, accounts.NilKey, r))
	require.True(t, newIBS(other, true, true).readValueUnchanged(other, AddressPath, accounts.NilKey, r))
}
