package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
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
