package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type sdAccountReader struct {
	emptyReader
	addr    accounts.Address
	account *accounts.Account
}

func (r *sdAccountReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if addr == r.addr && r.account != nil {
		a := &accounts.Account{}
		a.Copy(r.account)
		return a, nil
	}
	return nil, nil
}

// TestSelfdestructParallel_NoMaterialize verifies that on the parallel
// (versionMap) path Selfdestruct records the self-destruct through
// versioned-write cells without materializing/caching a stateObject.
func TestSelfdestructParallel_NoMaterialize(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(100)
	acc.Nonce = 1
	acc.Incarnation = 1

	reader := &sdAccountReader{addr: addr, account: &acc}
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(reader, vm)
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	destroyed, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	assert.True(t, destroyed, "existing account should be destroyed")

	writes := ibs.VersionedWrites()
	sd, ok := writes.GetSelfDestruct(addr)
	require.True(t, ok, "SelfDestructPath write expected")
	assert.True(t, sd.Val, "self-destruct flag should be set")

	bal, ok := writes.GetBalance(addr)
	require.True(t, ok, "BalancePath write expected")
	assert.True(t, bal.Val.IsZero(), "balance should be cleared")

	assert.Empty(t, ibs.stateObjects, "parallel Selfdestruct must not materialize a stateObject")
}

// TestSelfdestructParallel_RepeatedSameTx verifies that a second SELFDESTRUCT of
// the same account within a tx still proceeds (returns true and re-clears a
// balance credited in between) — matching the serial object, whose deleted flag
// stays false until finalize.
func TestSelfdestructParallel_RepeatedSameTx(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(100)
	acc.Nonce = 1

	reader := &sdAccountReader{addr: addr, account: &acc}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	destroyed, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	require.True(t, destroyed)

	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(30), tracing.BalanceChangeUnspecified))

	destroyed, err = ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	assert.True(t, destroyed, "repeat same-tx SELFDESTRUCT should still report destroyed")

	writes := ibs.VersionedWrites()
	bal, ok := writes.GetBalance(addr)
	require.True(t, ok)
	assert.True(t, bal.Val.IsZero(), "balance credited between the two SELFDESTRUCTs must be re-cleared")
}

// A second SELFDESTRUCT with nothing credited in between changes no cell, so it
// leaves the journal as it was.
func TestSelfdestructParallel_RepeatWithNothingChangedJournalsNothing(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(100)

	reader := &sdAccountReader{addr: addr, account: &acc}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetTxContext(100, 5)

	_, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	entries := ibs.journal.length()

	destroyed, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	assert.True(t, destroyed)
	assert.Equal(t, entries, ibs.journal.length())
}

// TestSelfdestructParallel_AbsentAccount verifies that self-destructing an
// account that does not exist returns false and records nothing.
func TestSelfdestructParallel_AbsentAccount(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xBE, 0xEF})

	reader := &sdAccountReader{addr: addr, account: nil}
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(reader, vm)
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	destroyed, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	assert.False(t, destroyed, "absent account cannot be destroyed")
	assert.Empty(t, ibs.stateObjects, "absent-account Selfdestruct must not materialize a stateObject")
}

// A self-destructed address whose write set also holds field, code and storage
// writes must be applied as a pure delete: no account record is left, and its
// code and its pre-block and same-tx storage are gone.
func TestApplyWrites_SelfDestructedAddressIsPureDelete(t *testing.T) {
	_, tx, domains := NewTestRwTx(t)
	addr := accounts.InternAddress(common.HexToAddress("0x5DEAD"))
	av := addr.Value()
	kDomain := accounts.InternKey(common.HexToHash("0x02"))
	kRaw := accounts.InternKey(common.HexToHash("0x03"))
	preCode := []byte{0x60, 0x01}
	pre := accounts.NewAccount()
	pre.Nonce = 1
	pre.Incarnation = 1
	pre.Balance.SetUint64(9)
	pre.CodeHash = accounts.NewCode(preCode).Hash
	domains.SetTxNum(1)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, av[:], accounts.SerialiseV3(&pre), 1, nil))
	require.NoError(t, domains.DomainPut(kv.CodeDomain, tx, av[:], preCode, 1, nil))
	kd := kDomain.Value()
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, append(append([]byte{}, av[:]...), kd[:]...), []byte{0x42}, 1, nil))

	code := accounts.NewCode([]byte{0x60, 0x00})
	ver := Version{TxIndex: 1}
	ws := &WriteSet{}
	ws.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: ver}, Val: true})
	ws.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: ver}, Val: uint256.Int{}})
	ws.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: ver}, Val: 7})
	ws.SetIncarnation(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath, Version: ver}, Val: 3})
	ws.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath, Version: ver}, Val: code.Hash})
	ws.SetCode(addr, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr, Path: CodePath, Version: ver}, Val: code})
	ws.SetStorage(addr, kRaw, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: kRaw, Version: ver}, Val: *uint256.NewInt(42)})

	domains.SetTxNum(2)
	require.NoError(t, ApplyWrites(ws, domains, tx, nil, 1, 2, false))

	enc, _, err := domains.GetLatest(kv.AccountsDomain, tx, av[:])
	require.NoError(t, err)
	assert.Empty(t, enc, "no account record may remain")
	c, _, err := domains.GetLatest(kv.CodeDomain, tx, av[:])
	require.NoError(t, err)
	assert.Empty(t, c, "code must be deleted")
	s1, _, err := domains.GetLatest(kv.StorageDomain, tx, append(append([]byte{}, av[:]...), kd[:]...))
	require.NoError(t, err)
	assert.Empty(t, s1, "pre-block slot must be deleted")
	kr := kRaw.Value()
	s2, _, err := domains.GetLatest(kv.StorageDomain, tx, append(append([]byte{}, av[:]...), kr[:]...))
	require.NoError(t, err)
	assert.Empty(t, s2, "same-tx slot write must be dropped")
}

// An address with no committed account owns no committed storage, so its self-destruct must not walk
// the storage prefix: the walk seeks every storage file. The orphan slot below breaks that invariant on
// purpose, so it survives only if the walk is skipped.
func TestApplyWrites_SelfDestructWithoutCommittedAccountSkipsStorageWalk(t *testing.T) {
	assertWas := dbg.AssertEnabled
	dbg.AssertEnabled = false // the assert panics on the orphan slot this test plants
	t.Cleanup(func() { dbg.AssertEnabled = assertWas })
	_, tx, domains := NewTestRwTx(t)
	addr := accounts.InternAddress(common.HexToAddress("0x5DEAD"))
	av := addr.Value()
	slot := common.HexToHash("0x02")
	storageKey := append(append([]byte{}, av[:]...), slot[:]...)
	domains.SetTxNum(1)
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, storageKey, []byte{0x42}, 1, nil))

	ver := Version{TxIndex: 1}
	ws := &WriteSet{}
	ws.SetCreateContract(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: CreateContractPath, Version: ver}, Val: true})
	ws.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: ver}, Val: true})
	// Paying out to another beneficiary leaves a plain zero balance write, not a burn.
	ws.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: ver}, Val: uint256.Int{}})

	domains.SetTxNum(2)
	require.NoError(t, ApplyWrites(ws, domains, tx, nil, 1, 2, false))

	enc, _, err := domains.GetLatest(kv.AccountsDomain, tx, av[:])
	require.NoError(t, err)
	assert.Empty(t, enc, "a pure delete must not write an account record")
	v, _, err := domains.GetLatest(kv.StorageDomain, tx, storageKey)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x42}, v, "self-destruct of an address without a committed account walked its storage prefix")
}

// A same-tx create+destruct that keeps a non-zero EIP-8246 balance takes the absent-account path
// without the storage walk and still writes a balance-only account.
func TestApplyWrites_SelfDestructWithoutCommittedAccountKeepsPreservedBalance(t *testing.T) {
	assertWas := dbg.AssertEnabled
	dbg.AssertEnabled = false // the assert panics on the orphan slot this test plants
	t.Cleanup(func() { dbg.AssertEnabled = assertWas })
	_, tx, domains := NewTestRwTx(t)
	addr := accounts.InternAddress(common.HexToAddress("0x5DEAD"))
	av := addr.Value()
	slot := common.HexToHash("0x02")
	storageKey := append(append([]byte{}, av[:]...), slot[:]...)
	domains.SetTxNum(1)
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, storageKey, []byte{0x42}, 1, nil))

	ver := Version{TxIndex: 1}
	ws := &WriteSet{}
	ws.SetCreateContract(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: CreateContractPath, Version: ver}, Val: true})
	ws.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: ver}, Val: true})
	ws.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: ver}, Val: *uint256.NewInt(5)})

	domains.SetTxNum(2)
	require.NoError(t, ApplyWrites(ws, domains, tx, nil, 1, 2, false))

	enc, _, err := domains.GetLatest(kv.AccountsDomain, tx, av[:])
	require.NoError(t, err)
	require.NotEmpty(t, enc, "the preserved balance must keep an account record")
	var acc accounts.Account
	require.NoError(t, accounts.DeserialiseV3(&acc, enc))
	assert.Equal(t, uint64(5), acc.Balance.Uint64())
	assert.Zero(t, acc.Nonce)
	assert.True(t, acc.CodeHash.IsEmpty(), "a destructed account keeps no code")
	v, _, err := domains.GetLatest(kv.StorageDomain, tx, storageKey)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x42}, v, "self-destruct of an address without a committed account walked its storage prefix")
}

// A committed account that self-destructs with a non-zero EIP-8246 balance and no create marker
// keeps a balance-only record and loses its committed storage.
func TestApplyWrites_SelfDestructPreservedBalanceDeletesCommittedStorage(t *testing.T) {
	_, tx, domains := NewTestRwTx(t)
	addr := accounts.InternAddress(common.HexToAddress("0x5DEAD"))
	av := addr.Value()
	preCode := []byte{0x60, 0x01}
	pre := accounts.NewAccount()
	pre.Nonce = 1
	pre.Incarnation = 1
	pre.Balance.SetUint64(9)
	pre.CodeHash = accounts.NewCode(preCode).Hash
	slot := common.HexToHash("0x02")
	storageKey := append(append([]byte{}, av[:]...), slot[:]...)
	domains.SetTxNum(1)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, av[:], accounts.SerialiseV3(&pre), 1, nil))
	require.NoError(t, domains.DomainPut(kv.CodeDomain, tx, av[:], preCode, 1, nil))
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, storageKey, []byte{0x42}, 1, nil))

	ver := Version{TxIndex: 1}
	ws := &WriteSet{}
	ws.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: ver}, Val: true})
	ws.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: ver}, Val: *uint256.NewInt(9)})

	domains.SetTxNum(2)
	require.NoError(t, ApplyWrites(ws, domains, tx, nil, 1, 2, false))

	enc, _, err := domains.GetLatest(kv.AccountsDomain, tx, av[:])
	require.NoError(t, err)
	require.NotEmpty(t, enc, "the preserved balance must keep an account record")
	var acc accounts.Account
	require.NoError(t, accounts.DeserialiseV3(&acc, enc))
	assert.Equal(t, uint64(9), acc.Balance.Uint64())
	assert.Zero(t, acc.Nonce)
	assert.True(t, acc.CodeHash.IsEmpty(), "a destructed account keeps no code")
	c, _, err := domains.GetLatest(kv.CodeDomain, tx, av[:])
	require.NoError(t, err)
	assert.Empty(t, c, "code must be deleted")
	v, _, err := domains.GetLatest(kv.StorageDomain, tx, storageKey)
	require.NoError(t, err)
	assert.Empty(t, v, "committed storage must be deleted")
}

// A system-contract predeploy can have code with no account record, so skipping the storage walk of
// an address without an account must still delete its code.
func TestApplyWrites_SelfDestructWithoutCommittedAccountDeletesCode(t *testing.T) {
	_, tx, domains := NewTestRwTx(t)
	addr := accounts.InternAddress(common.HexToAddress("0x5DEAD"))
	av := addr.Value()
	domains.SetTxNum(1)
	require.NoError(t, domains.DomainPut(kv.CodeDomain, tx, av[:], []byte{0x60, 0x01}, 1, nil))

	ver := Version{TxIndex: 1}
	ws := &WriteSet{}
	ws.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: ver}, Val: true})

	domains.SetTxNum(2)
	require.NoError(t, ApplyWrites(ws, domains, tx, nil, 1, 2, false))

	c, _, err := domains.GetLatest(kv.CodeDomain, tx, av[:])
	require.NoError(t, err)
	assert.Empty(t, c, "self-destruct left the code of an address without an account record")
}
