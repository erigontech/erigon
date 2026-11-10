package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/tracing"
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

type lendingReader struct {
	codeReader
	lends int
}

func (r *lendingReader) LendAccountCode(addr accounts.Address) ([]byte, bool, error) {
	if addr != r.addr {
		return nil, false, nil
	}
	r.lends++
	return r.code, true, nil
}

func newLendingRead(t *testing.T, vm *VersionMap) (*IntraBlockState, *lendingReader, accounts.Address, []byte) {
	t.Helper()
	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	code := []byte{0x60, 0x01, 0x60, 0x02, 0x01}
	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1
	acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	reader := &lendingReader{codeReader: codeReader{addr: addr, account: &acc, code: code}}
	ibs := NewWithVersionMap(reader, vm)
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)
	return ibs, reader, addr, code
}

func TestCodeLent_RecordsTheSameHeaderWithoutBytes(t *testing.T) {
	normal, _, addr, code := newLendingRead(t, NewVersionMap(nil))
	got, err := normal.GetCode(addr)
	require.NoError(t, err)
	require.Equal(t, code, got)
	wantRead, ok := normal.versionedReads.GetCode(addr)
	require.True(t, ok)

	lending, reader, addr, code := newLendingRead(t, NewVersionMap(nil))
	got, err = lending.GetCodeLent(addr)
	require.NoError(t, err)
	require.Equal(t, code, got)
	require.Equal(t, 1, reader.lends)
	require.Empty(t, lending.stateObjects)

	haveRead, ok := lending.versionedReads.GetCode(addr)
	require.True(t, ok)
	require.Equal(t, wantRead.ReadHeader, haveRead.ReadHeader)
	require.Nil(t, haveRead.Val.Bytes, "the lent bytes must not be retained")
	require.Equal(t, wantRead.Val.Hash, haveRead.Val.Hash)
}

func TestCodeLent_LaterReadSeesFullCode(t *testing.T) {
	for name, read := range map[string]func(*IntraBlockState, accounts.Address) ([]byte, error){
		"GetCode": (*IntraBlockState).GetCode,
		"ResolveCode": func(ibs *IntraBlockState, addr accounts.Address) ([]byte, error) {
			code, err := ibs.ResolveCode(addr)
			return code.Bytes, err
		},
		"GetCodeLent": (*IntraBlockState).GetCodeLent,
	} {
		t.Run(name, func(t *testing.T) {
			ibs, _, addr, code := newLendingRead(t, NewVersionMap(nil))
			_, err := ibs.GetCodeLent(addr)
			require.NoError(t, err)

			got, err := read(ibs, addr)
			require.NoError(t, err)
			require.Equal(t, code, got)
		})
	}
}

func TestCodeLent_LowerTxCodeWriteInvalidatesTheRead(t *testing.T) {
	vm := NewVersionMap(nil)
	ibs, _, addr, _ := newLendingRead(t, vm)
	_, err := ibs.GetCodeLent(addr)
	require.NoError(t, err)

	ws := &WriteSet{}
	lowerCode := accounts.NewCode([]byte{0x60, 0x00})
	ws.SetCode(addr, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr, Path: CodePath, Version: Version{TxIndex: 1}, valStatus: ValueChanged}, Val: lowerCode})
	vm.FlushVersionedWrites(ws, true)

	io := NewVersionedIO(6)
	io.RecordReads(Version{TxIndex: 5}, ibs.VersionedReads())
	require.Equal(t, VersionInvalid, vm.ValidateVersion(5, io, validateEqualVersion, false, ""))
}

func TestCodeLent_LowerTxSelfDestructInvalidatesTheRead(t *testing.T) {
	vm := NewVersionMap(nil)
	ibs, _, addr, _ := newLendingRead(t, vm)
	_, err := ibs.GetCodeLent(addr)
	require.NoError(t, err)

	ws := &WriteSet{}
	ws.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: Version{TxIndex: 1}}, Val: true})
	vm.FlushVersionedWrites(ws, true)

	io := NewVersionedIO(6)
	io.RecordReads(Version{TxIndex: 5}, ibs.VersionedReads())
	require.Equal(t, VersionInvalid, vm.ValidateVersion(5, io, validateEqualVersion, false, ""))
}

func TestCodeLent_UncommittedCodeStaysOnTheNormalPath(t *testing.T) {
	t.Run("version map", func(t *testing.T) {
		vm := NewVersionMap(nil)
		ibs, reader, addr, _ := newLendingRead(t, vm)
		mapped := accounts.NewCode([]byte{0x60, 0x00})
		ws := &WriteSet{}
		ws.SetCode(addr, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr, Path: CodePath, Version: Version{TxIndex: 1}, valStatus: ValueChanged}, Val: mapped})
		vm.FlushVersionedWrites(ws, true)

		got, err := ibs.GetCodeLent(addr)
		require.NoError(t, err)
		require.Equal(t, mapped.Bytes, got)
		require.Zero(t, reader.lends)
	})
	t.Run("own write", func(t *testing.T) {
		ibs, reader, addr, _ := newLendingRead(t, NewVersionMap(nil))
		own := []byte{0x60, 0x00}
		require.NoError(t, ibs.SetCode(addr, own, tracing.CodeChangeUnspecified))

		got, err := ibs.GetCodeLent(addr)
		require.NoError(t, err)
		require.Equal(t, own, got)
		require.Zero(t, reader.lends)
	})
}

func TestCodeLent_RepeatedReadKeepsTheBytes(t *testing.T) {
	ibs, reader, addr, code := newLendingRead(t, NewVersionMap(nil))
	for range 3 {
		got, err := ibs.GetCodeLent(addr)
		require.NoError(t, err)
		require.Equal(t, code, got)
	}
	require.Equal(t, 1, reader.lends, "a repeated read must not decode again")
	read, ok := ibs.versionedReads.GetCode(addr)
	require.True(t, ok)
	require.Equal(t, code, read.Val.Bytes)
}
