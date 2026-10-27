package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
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
	require.NoError(t, ApplyWrites(ws, domains, tx, 1, 2, nil, &chain.Rules{IsSpuriousDragon: true}, false))

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
