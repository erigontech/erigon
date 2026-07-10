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

// fieldReader returns a fixed committed account (+ optional code) for one addr.
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

// TestTransientStale_FieldGettersPreferCells pins that, on the noMaterialize
// (parallel) path, every account-field getter reads the per-field versionMap
// cell — not the AddressPath account record, which a prior tx can publish stale
// (its Nonce/CodeHash lagging the field cells). A later tx must see the cells.
func TestTransientStale_FieldGettersPreferCells(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x35, 0x85, 0x97, 0xa2})
	delegationCode := types.AddressToDelegation(accounts.InternAddress([20]byte{0x55, 0xe5, 0xb3, 0x85}))
	delegHash := accounts.InternCodeHash(crypto.Keccak256Hash(delegationCode))

	committed := accounts.NewAccount() // committed EOA: nonce 0, no code
	reader := &fieldReader{addr: addr, account: &committed}

	vm := NewVersionMap(nil)

	// tx1 (TxIndex=1) publishes a STALE AddressPath account record (nonce 0,
	// empty codehash, zero balance) alongside NEWER per-field cells.
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

// TestTransientStale_GetDelegatedDesignation is the warm-callcode consensus
// regression. A prior tx published an AddressPath account record with an EMPTY
// code hash (stale) alongside CodePath/CodeHashPath cells carrying a delegation.
// GetDelegatedDesignation goes through getStateObject, which on the noMaterialize
// path rebuilds a transient from the AddressPath record. It must reconcile the
// transient's code with the CodePath cell — otherwise it sees empty code,
// reports "not delegated", the EIP-7702 authorization is skipped, the delegation
// persists, and a later CALL runs the delegated code and runs out of gas
// (execution over-counts gas, producing a wrong receipt/trie root).
func TestTransientStale_GetDelegatedDesignation(t *testing.T) {
	authority := accounts.InternAddress([20]byte{0x35, 0x85, 0x97, 0xa2})
	target := accounts.InternAddress([20]byte{0x55, 0xe5, 0xb3, 0x85})
	delegationCode := types.AddressToDelegation(target)
	delegHash := accounts.InternCodeHash(crypto.Keccak256Hash(delegationCode))

	reader := &fieldReader{addr: authority, account: nil} // committed: absent

	vm := NewVersionMap(nil)

	stale := accounts.NewAccount()
	stale.Nonce = 1
	stale.CodeHash = accounts.EmptyCodeHash // stale — lags the CodeHashPath cell
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
