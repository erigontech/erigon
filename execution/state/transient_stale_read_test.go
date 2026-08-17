package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type fieldReader struct {
	emptyReader
	addr    accounts.Address
	account *accounts.Account
	code    []byte
}

func (r *fieldReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if addr == r.addr && r.account != nil {
		a := &accounts.Account{}
		a.Copy(r.account)
		return a, nil
	}
	return nil, nil
}
func (r *fieldReader) ReadAccountDataForDebug(addr accounts.Address) (*accounts.Account, error) {
	return r.ReadAccountData(addr)
}
func (r *fieldReader) ReadAccountCode(addr accounts.Address) ([]byte, error) {
	if addr == r.addr {
		return r.code, nil
	}
	return nil, nil
}
func (r *fieldReader) ReadAccountCodeSize(addr accounts.Address) (int, error) {
	if addr == r.addr {
		return len(r.code), nil
	}
	return 0, nil
}

// Field getters on the noMaterialize path must read the versionMap's per-field cell, not a stale AddressPath record.
func TestTransientStale_FieldGettersPreferCells(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x35, 0x85, 0x97, 0xa2})
	delegationCode := types.AddressToDelegation(accounts.InternAddress([20]byte{0x55, 0xe5, 0xb3, 0x85}))
	delegHash := accounts.InternCodeHash(crypto.Keccak256Hash(delegationCode))

	committed := accounts.NewAccount()
	reader := &fieldReader{addr: addr, account: &committed}

	vm := NewVersionMap(nil)

	stale := accounts.NewAccount()
	v1 := Version{TxIndex: 1, Incarnation: 0}
	vm.WriteAddress(addr, v1, &stale, true)
	vm.WriteNonce(addr, v1, uint64(7), true)
	vm.WriteBalance(addr, v1, *uint256.NewInt(777), true)
	vm.WriteCodeHash(addr, v1, delegHash, true)
	vm.WriteCode(addr, v1, accounts.NewCode(delegationCode), true)
	vm.WriteCodeSize(addr, v1, len(delegationCode), true)
	vm.WriteIncarnation(addr, v1, uint64(3), true)

	ibs := NewWithVersionMap(reader, vm)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(100, 2)
	ibs.SetVersion(0)

	nonce, err := ibs.GetNonce(addr)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), nonce, "nonce must come from the NoncePath cell, not the stale record")

	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, uint64(777), bal.Uint64(), "balance must come from the BalancePath cell")

	ch, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	assert.Equal(t, delegHash, ch, "codehash must come from the CodeHashPath cell")

	code, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Equal(t, delegationCode, code, "code must come from the CodePath cell")

	sz, err := ibs.GetCodeSize(addr)
	require.NoError(t, err)
	assert.Equal(t, len(delegationCode), sz, "code size must come from the CodeSizePath cell")

	inc, err := ibs.GetIncarnation(addr)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), inc, "incarnation must come from the IncarnationPath cell")
}

// GetDelegatedDesignation must reconcile a transient rebuilt from a stale
// AddressPath record with the CodePath cell, or it misses an active EIP-7702 delegation.
func TestTransientStale_GetDelegatedDesignation(t *testing.T) {
	authority := accounts.InternAddress([20]byte{0x35, 0x85, 0x97, 0xa2})
	target := accounts.InternAddress([20]byte{0x55, 0xe5, 0xb3, 0x85})
	delegationCode := types.AddressToDelegation(target)
	delegHash := accounts.InternCodeHash(crypto.Keccak256Hash(delegationCode))

	reader := &fieldReader{addr: authority, account: nil}

	vm := NewVersionMap(nil)

	stale := accounts.NewAccount()
	stale.Nonce = 1
	stale.CodeHash = accounts.EmptyCodeHash
	v1 := Version{TxIndex: 1, Incarnation: 0}
	vm.WriteAddress(authority, v1, &stale, true)
	vm.WriteNonce(authority, v1, uint64(1), true)
	vm.WriteCodeHash(authority, v1, delegHash, true)
	vm.WriteCode(authority, v1, accounts.NewCode(delegationCode), true)
	vm.WriteCodeSize(authority, v1, len(delegationCode), true)

	ibs := NewWithVersionMap(reader, vm)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(100, 2)
	ibs.SetVersion(0)

	got, ok, err := ibs.GetDelegatedDesignation(authority)
	require.NoError(t, err)
	require.True(t, ok, "delegation must be visible via the CodePath cell, not the stale AddressPath codehash")
	assert.Equal(t, target, got)
	assert.Empty(t, ibs.stateObjects, "GetDelegatedDesignation must not materialize a stateObject")
}

// An account self-destructed by a prior tx must read empty code: the self-destruct gate must cover CodePath, not only CodeHashPath.
func TestCrossTxSelfDestruct_CodeReadsEmpty(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x4d, 0x95, 0xfb, 0xaf})
	code := []byte{0x60, 0x60, 0x60, 0x40, 0x52, 0x00}
	committed := accounts.NewAccount()
	committed.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	reader := &fieldReader{addr: addr, account: &committed, code: code}

	vm := NewVersionMap(nil)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 1, Incarnation: 0}, true, true)

	ibs := NewWithVersionMap(reader, vm)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(100, 2)
	ibs.SetVersion(0)

	gotCode, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Empty(t, gotCode, "code of a prior-tx self-destructed account must read empty")

	ch, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	assert.True(t, ch == accounts.EmptyCodeHash || ch.IsZero(),
		"code hash must read empty, consistent with the empty code")

	sz, err := ibs.GetCodeSize(addr)
	require.NoError(t, err)
	assert.Zero(t, sz, "code size must be 0")
}

// An incarnation bump that clears CodeHash but writes no CodePath cell must still read empty code, not the stale committed bytes.
func TestCrossTxIncarnationBump_CodeReadsEmpty(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x4d, 0x95, 0xfb, 0xaf})
	code := []byte{0x60, 0x60, 0x60, 0x40, 0x52, 0x00}
	committed := accounts.NewAccount()
	committed.Incarnation = 1
	committed.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	reader := &fieldReader{addr: addr, account: &committed, code: code}

	vm := NewVersionMap(nil)
	v1 := Version{TxIndex: 1, Incarnation: 0}
	vm.WriteIncarnation(addr, v1, uint64(2), true)
	vm.WriteCodeHash(addr, v1, accounts.EmptyCodeHash, true)

	ibs := NewWithVersionMap(reader, vm)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(100, 2)
	ibs.SetVersion(0)

	gotCode, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Empty(t, gotCode, "code must read empty after a prior-tx incarnation bump, not the stale committed code")

	ch, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	assert.Equal(t, accounts.EmptyCodeHash, ch, "code hash must be empty, consistent with the code")
}
