package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestCodeReadParallel_NoMaterialize verifies that cold GetCode / GetCodeSize
// reads on the parallel (versionMap) path return the committed code without
// materializing/caching a stateObject.
func TestCodeReadParallel_NoMaterialize(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	code := []byte{0x60, 0x01, 0x60, 0x02, 0x01} // PUSH1 1 PUSH1 2 ADD

	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1
	acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))

	reader := &codeReader{addr: addr, account: &acc, code: code}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	got, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Equal(t, code, got, "cold GetCode must return the committed code")
	assert.Empty(t, ibs.stateObjects, "cold parallel GetCode must not materialize a stateObject")

	sz, err := ibs.GetCodeSize(addr)
	require.NoError(t, err)
	assert.Equal(t, len(code), sz, "cold GetCodeSize must return the committed size")
	assert.Empty(t, ibs.stateObjects, "cold parallel GetCodeSize must not materialize a stateObject")

	// Repeat reads hit the recorded ReadSet, still no materialization.
	got2, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Equal(t, code, got2)
	assert.Empty(t, ibs.stateObjects)
}

// TestCodeReadParallel_EmptyCodeHashIgnoresStaleCode pins the codeHash gate on
// the cold parallel code read. When a 7702 delegation is cleared the account's
// codeHash becomes empty, but the address-keyed CodeDomain keeps the stale
// delegation bytes (code is never deleted, only the codeHash pointer). The read
// must honour the empty codeHash and report no code — otherwise GetDelegatedDesignation
// sees the stale delegation and a plain transfer to the EOA is charged as a call
// to delegated code, running out of gas and flipping the receipt status.
func TestCodeReadParallel_EmptyCodeHashIgnoresStaleCode(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x30, 0x75, 0x76, 0xDd})
	staleDelegation := types.AddressToDelegation(accounts.InternAddress([20]byte{0xfb, 0x77, 0x02}))

	acc := accounts.NewAccount()
	acc.Nonce = 594735
	acc.CodeHash = accounts.EmptyCodeHash // delegation cleared: empty codehash...

	reader := &codeReader{addr: addr, account: &acc, code: staleDelegation} // ...but CodeDomain still holds the stale bytes
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	got, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Empty(t, got, "empty codehash must yield no code, not the stale CodeDomain bytes")

	sz, err := ibs.GetCodeSize(addr)
	require.NoError(t, err)
	assert.Zero(t, sz, "empty codehash must yield zero code size")

	_, ok, err := ibs.GetDelegatedDesignation(addr)
	require.NoError(t, err)
	assert.False(t, ok, "an empty-codehash account must not read as 7702-delegated")
}
