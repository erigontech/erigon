package state

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestCodeReadParallel_NoMaterialize(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	code := []byte{0x60, 0x01, 0x60, 0x02, 0x01}

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

	got2, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Equal(t, code, got2)
	assert.Empty(t, ibs.stateObjects)
}

// Clearing a 7702 delegation empties the codeHash, but the address-keyed
// CodeDomain still holds the stale delegation bytes — the read must honor the empty codeHash and report no code.
func TestCodeReadParallel_EmptyCodeHashIgnoresStaleCode(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x30, 0x75, 0x76, 0xDd})
	staleDelegation := types.AddressToDelegation(accounts.InternAddress([20]byte{0xfb, 0x77, 0x02}))

	acc := accounts.NewAccount()
	acc.Nonce = 594735
	acc.CodeHash = accounts.EmptyCodeHash

	reader := &codeReader{addr: addr, account: &acc, code: staleDelegation}
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

// committedCodeIBS builds a noMaterialize IBS over one committed contract;
// accountHash, when set, makes the account record disagree with the CodeDomain bytes.
func committedCodeIBS(tb testing.TB, codeLen int, accountHash *accounts.CodeHash) (*IntraBlockState, accounts.Address) {
	tb.Helper()

	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	code := make([]byte, codeLen)
	for i := range code {
		code[i] = byte(i)
	}

	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1
	acc.Balance.SetUint64(1000)
	acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	if accountHash != nil {
		acc.CodeHash = *accountHash
	}

	ibs := NewWithVersionMap(&codeReader{addr: addr, account: &acc, code: code}, NewVersionMap(nil))
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	got, err := ibs.GetCode(addr)
	require.NoError(tb, err)
	require.Len(tb, got, codeLen)

	return ibs, addr
}

// Cost growing with code size means getStateObject's rebuild is hashing the code bytes.
func BenchmarkGetStateObjectAfterCodeRead(b *testing.B) {
	for _, codeLen := range []int{32, 1024, 24576} {
		b.Run(fmt.Sprintf("code=%dB", codeLen), func(b *testing.B) {
			ibs, addr := committedCodeIBS(b, codeLen, nil)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := ibs.getStateObject(addr, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// The account record is authoritative for committed code; CodeDomain (keyed
// by address) can hold bytes a cleared 7702 delegation no longer owns.
func TestCommittedCodeHashComesFromAccountRecord(t *testing.T) {
	t.Run("account hash agrees with the bytes", func(t *testing.T) {
		ibs, addr := committedCodeIBS(t, 4096, nil)

		so, err := ibs.getStateObject(addr, false)
		require.NoError(t, err)
		require.NotNil(t, so)

		expected := accounts.InternCodeHash(crypto.Keccak256Hash(so.code.Bytes))
		require.Equal(t, expected, so.data.CodeHash)
		require.Equal(t, expected, so.code.Hash)
	})

	t.Run("stale CodeDomain bytes do not override the account hash", func(t *testing.T) {
		stale := accounts.InternCodeHash(crypto.Keccak256Hash([]byte{0xDE, 0xAD}))
		ibs, addr := committedCodeIBS(t, 4096, &stale)

		so, err := ibs.getStateObject(addr, false)
		require.NoError(t, err)
		require.NotNil(t, so)

		require.Equal(t, stale, so.data.CodeHash, "account record must stay authoritative")
		require.Equal(t, stale, so.code.Hash)
		require.Equal(t, stale, so.original.CodeHash)
	})
}

// A prior-tx code write's cell hash is authoritative — not keccak(bytes).
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
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(100, 7)
	ibs.SetVersion(0)

	so, err := ibs.getStateObject(addr, false)
	require.NoError(t, err)
	require.NotNil(t, so)

	require.Equal(t, priorCode, so.code.Bytes)
	require.Equal(t, cellHash, so.code.Hash, "the cell's hash must win, not keccak(bytes)")
	require.Equal(t, cellHash, so.data.CodeHash)
}

// The cell's hash stays authoritative even when a dirty address forces the
// rebuild to re-probe and land on the cached read-set hit instead.
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
	ibs.SetNoMaterialize(true)
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
