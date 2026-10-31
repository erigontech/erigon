package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
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

// The account record is the authority for the hash of committed code: the
// CodeDomain is keyed by address, so its bytes can disagree with the record.
func TestCommittedCodeHashComesFromAccountRecord(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	code := []byte{0x60, 0x01, 0x60, 0x00, 0x55, 0x00}
	recordHash := accounts.InternCodeHash(crypto.Keccak256Hash([]byte{0xDE, 0xAD}))
	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1
	acc.Balance.SetUint64(1000)
	acc.CodeHash = recordHash

	ibs := NewWithVersionMap(&codeReader{addr: addr, account: &acc, code: code}, NewVersionMap(nil))
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)
	got, err := ibs.GetCode(addr)
	require.NoError(t, err)
	require.Equal(t, code, got)

	so, err := ibs.getStateObject(addr, false)
	require.NoError(t, err)
	require.NotNil(t, so)
	require.Equal(t, recordHash, so.data.CodeHash, "account record must stay authoritative")
	require.Equal(t, recordHash, so.code.Hash)
	require.Equal(t, recordHash, so.original.CodeHash)
}

// TestPriorTxCodeWriteHashComesFromTheCell pins the cell as the authority for a
// prior-tx code write, so its hash is used rather than derived from its bytes.
func TestPriorTxCodeWriteHashComesFromTheCell(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	priorCode := []byte{0xef, 0x01, 0x00, 0x11, 0x22, 0x33}
	cellHash := accounts.InternCodeHash(crypto.Keccak256Hash([]byte{0xBE, 0xEF}))

	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1
	acc.CodeHash = accounts.EmptyCodeHash

	vm := NewVersionMap(nil)
	vm.WriteCode(addr, Version{TxIndex: 2, Incarnation: 0}, accounts.Code{Hash: cellHash, Bytes: priorCode}, true)

	ibs := NewWithVersionMap(&codeReader{addr: addr, account: &acc, code: nil}, vm)
	ibs.SetTxContext(100, 7)
	ibs.SetVersion(0)

	so, err := ibs.getStateObject(addr, false)
	require.NoError(t, err)
	require.NotNil(t, so)

	require.Equal(t, priorCode, so.code.Bytes)
	require.Equal(t, cellHash, so.code.Hash, "the cell's hash must win, not keccak(bytes)")
	require.Equal(t, cellHash, so.data.CodeHash)
}

// TestPriorTxCodeWriteHashSurvivesReadSetHit pins the same authority once the
// read is already recorded. A dirty address bypasses the read-once gate, so the
// rebuild re-probes the version map and lands on the read-set hit instead.
func TestPriorTxCodeWriteHashSurvivesReadSetHit(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	priorCode := []byte{0xef, 0x01, 0x00, 0x11, 0x22, 0x33}
	cellHash := accounts.InternCodeHash(crypto.Keccak256Hash([]byte{0xBE, 0xEF}))

	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1
	acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash([]byte{0xFE, 0xED}))

	vm := NewVersionMap(nil)
	vm.WriteCode(addr, Version{TxIndex: 2, Incarnation: 0}, accounts.Code{Hash: cellHash, Bytes: priorCode}, true)

	ibs := NewWithVersionMap(&codeReader{addr: addr, account: &acc, code: nil}, vm)
	ibs.SetTxContext(100, 7)
	ibs.SetVersion(0)

	got, err := ibs.GetCode(addr)
	require.NoError(t, err)
	require.Equal(t, priorCode, got)

	tr, ok := ibs.versionedReads.GetCode(addr)
	require.True(t, ok, "the first read must be recorded")
	require.Equal(t, MapRead, tr.Source)

	ibs.journal.dirties[addr] = 1

	so, err := ibs.getStateObject(addr, false)
	require.NoError(t, err)
	require.NotNil(t, so)

	require.Equal(t, priorCode, so.code.Bytes)
	require.Equal(t, cellHash, so.code.Hash, "the cell's hash must win, not keccak(bytes)")
	require.Equal(t, cellHash, so.data.CodeHash)
}

// refreshCode no longer derives a missing hash from the bytes, so a producer
// storing code without its hash must trip the assertion instead of leaving a
// nil code hash in the account record.
func TestRefreshCodeAssertsHashPresent(t *testing.T) {
	defer func(prev bool) { dbg.AssertEnabled = prev }(dbg.AssertEnabled)
	dbg.AssertEnabled = true

	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1

	vm := NewVersionMap(nil)
	vm.WriteCode(addr, Version{TxIndex: 2, Incarnation: 0}, accounts.Code{Bytes: []byte{0x60, 0x00}}, true)

	ibs := NewWithVersionMap(&codeReader{addr: addr, account: &acc, code: nil}, vm)
	ibs.SetTxContext(100, 7)
	ibs.SetVersion(0)

	assert.Panics(t, func() { _, _ = refreshCode(ibs, addr) })
}
