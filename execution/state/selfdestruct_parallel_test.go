package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
