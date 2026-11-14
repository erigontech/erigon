package state

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
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

type segmentCodeReader struct {
	codeReader
	segmentReads, fullReads int
}

func (r *segmentCodeReader) ReadAccountCode(addr accounts.Address) ([]byte, error) {
	r.fullReads++
	return r.codeReader.ReadAccountCode(addr)
}

func (r *segmentCodeReader) ReadAccountCodeSegmentTo(addr accounts.Address, from uint64, dst []byte) (bool, error) {
	if addr != r.addr {
		return false, nil
	}
	r.segmentReads++
	common.CopySegment(dst, r.code, from)
	return true, nil
}

// decliningCodeReader serves no segments, like a reader over history or the previous block.
type decliningCodeReader struct {
	codeReader
}

func (decliningCodeReader) ReadAccountCodeSegmentTo(accounts.Address, uint64, []byte) (bool, error) {
	return false, nil
}

// A declined segment read is not counted: the full read that replaces it counts once.
func TestCodeSegment_DeclinedReadCountsOnce(t *testing.T) {
	prev := dbg.KVReadLevelledMetrics
	dbg.KVReadLevelledMetrics = true
	t.Cleanup(func() { dbg.KVReadLevelledMetrics = prev })
	_, reader, addr, code := newSegmentRead(t, nil)
	ibs := NewWithVersionMap(&decliningCodeReader{codeReader: reader.codeReader}, NewVersionMap(nil))
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	got, err := segmentOf(ibs, addr, len(code))
	require.NoError(t, err)
	require.Equal(t, code, got)
	require.Equal(t, int64(1), ibs.CodeReadCount())
}

func segmentOf(ibs *IntraBlockState, addr accounts.Address, n int) ([]byte, error) {
	dst := make([]byte, n)
	err := ibs.ReadCodeSegmentTo(addr, 0, dst)
	return dst, err
}

func newSegmentRead(t *testing.T, vm *VersionMap) (*IntraBlockState, *segmentCodeReader, accounts.Address, []byte) {
	t.Helper()
	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	code := []byte{0x60, 0x01, 0x60, 0x02, 0x01}
	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1
	acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	reader := &segmentCodeReader{codeReader: codeReader{addr: addr, account: &acc, code: code}}
	ibs := NewWithVersionMap(reader, vm)
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)
	return ibs, reader, addr, code
}

func TestCodeSegment_RecordsTheSameHeaderWithoutBytes(t *testing.T) {
	normal, _, addr, code := newSegmentRead(t, NewVersionMap(nil))
	got, err := normal.GetCode(addr)
	require.NoError(t, err)
	require.Equal(t, code, got)
	wantRead, ok := normal.versionedReads.GetCode(addr)
	require.True(t, ok)

	segmented, reader, addr, code := newSegmentRead(t, NewVersionMap(nil))
	got, err = segmentOf(segmented, addr, len(code))
	require.NoError(t, err)
	require.Equal(t, code, got)
	require.Equal(t, 1, reader.segmentReads)
	require.Empty(t, segmented.stateObjects)

	haveRead, ok := segmented.versionedReads.GetCode(addr)
	require.True(t, ok)
	require.Equal(t, wantRead.ReadHeader, haveRead.ReadHeader)
	require.Nil(t, haveRead.Val.Bytes, "the decoded bytes must not be retained")
	require.Equal(t, wantRead.Val.Hash, haveRead.Val.Hash)
}

func TestCodeSegment_LaterReadSeesFullCode(t *testing.T) {
	for name, read := range map[string]func(*IntraBlockState, accounts.Address) ([]byte, error){
		"GetCode": (*IntraBlockState).GetCode,
		"ResolveCode": func(ibs *IntraBlockState, addr accounts.Address) ([]byte, error) {
			code, err := ibs.ResolveCode(addr)
			return code.Bytes, err
		},
		"ReadCodeSegmentTo": func(ibs *IntraBlockState, addr accounts.Address) ([]byte, error) {
			return segmentOf(ibs, addr, 5)
		},
	} {
		t.Run(name, func(t *testing.T) {
			ibs, _, addr, code := newSegmentRead(t, NewVersionMap(nil))
			_, err := segmentOf(ibs, addr, 5)
			require.NoError(t, err)

			got, err := read(ibs, addr)
			require.NoError(t, err)
			require.Equal(t, code, got)
		})
	}
}

func TestCodeSegment_LowerTxCodeWriteInvalidatesTheRead(t *testing.T) {
	vm := NewVersionMap(nil)
	ibs, _, addr, _ := newSegmentRead(t, vm)
	_, err := segmentOf(ibs, addr, 5)
	require.NoError(t, err)

	ws := &WriteSet{}
	lowerCode := accounts.NewCode([]byte{0x60, 0x00})
	ws.SetCode(addr, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr, Path: CodePath, Version: Version{TxIndex: 1}, valStatus: ValueChanged}, Val: lowerCode})
	vm.FlushVersionedWrites(ws, true)

	io := NewVersionedIO(6)
	io.RecordReads(Version{TxIndex: 5}, ibs.VersionedReads())
	require.Equal(t, VersionInvalid, vm.ValidateVersion(5, io, validateEqualVersion, false, ""))
}

func TestCodeSegment_LowerTxSelfDestructInvalidatesTheRead(t *testing.T) {
	vm := NewVersionMap(nil)
	ibs, _, addr, _ := newSegmentRead(t, vm)
	_, err := segmentOf(ibs, addr, 5)
	require.NoError(t, err)

	ws := &WriteSet{}
	ws.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: Version{TxIndex: 1}}, Val: true})
	vm.FlushVersionedWrites(ws, true)

	io := NewVersionedIO(6)
	io.RecordReads(Version{TxIndex: 5}, ibs.VersionedReads())
	require.Equal(t, VersionInvalid, vm.ValidateVersion(5, io, validateEqualVersion, false, ""))
}

func TestCodeSegment_UncommittedCodeStaysOnTheNormalPath(t *testing.T) {
	t.Run("version map", func(t *testing.T) {
		vm := NewVersionMap(nil)
		ibs, reader, addr, _ := newSegmentRead(t, vm)
		mapped := accounts.NewCode([]byte{0x60, 0x00})
		ws := &WriteSet{}
		ws.SetCode(addr, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr, Path: CodePath, Version: Version{TxIndex: 1}, valStatus: ValueChanged}, Val: mapped})
		vm.FlushVersionedWrites(ws, true)

		got, err := segmentOf(ibs, addr, len(mapped.Bytes))
		require.NoError(t, err)
		require.Equal(t, mapped.Bytes, got)
		require.Zero(t, reader.segmentReads)
	})
	t.Run("own write", func(t *testing.T) {
		ibs, reader, addr, _ := newSegmentRead(t, NewVersionMap(nil))
		own := []byte{0x60, 0x00}
		require.NoError(t, ibs.SetCode(addr, own, tracing.CodeChangeUnspecified))

		got, err := segmentOf(ibs, addr, len(own))
		require.NoError(t, err)
		require.Equal(t, own, got)
		require.Zero(t, reader.segmentReads)
	})
}

// A repeated segment read copies again, from the worker's last decode, instead of reading the full code.
func TestCodeSegment_RepeatedReadCopiesAgain(t *testing.T) {
	ibs, reader, addr, code := newSegmentRead(t, NewVersionMap(nil))
	for range 3 {
		got, err := segmentOf(ibs, addr, len(code))
		require.NoError(t, err)
		require.Equal(t, code, got)
	}
	require.Zero(t, reader.fullReads, "a repeated segment read must not read the full code")
	require.Equal(t, 3, reader.segmentReads)
	read, ok := ibs.versionedReads.GetCode(addr)
	require.True(t, ok)
	require.True(t, codeBytesDropped(read.Val), "the read stays recorded without bytes")
}

func TestCodeSegment_CopiesFromTheOffsetAndZeroPads(t *testing.T) {
	for name, vm := range map[string]*VersionMap{"version map": NewVersionMap(nil), "no version map": nil} {
		t.Run(name, func(t *testing.T) {
			ibs, _, addr, code := newSegmentRead(t, vm)
			dst := []byte{0xff, 0xff, 0xff, 0xff}
			require.NoError(t, ibs.ReadCodeSegmentTo(addr, 3, dst))
			require.Equal(t, []byte{code[3], code[4], 0, 0}, dst)
		})
	}
}

func segmentReadThenLowerCodeWrite(t *testing.T) (*IntraBlockState, *VersionMap, accounts.Address, []byte) {
	t.Helper()
	vm := NewVersionMap(nil)
	ibs, _, addr, code := newSegmentRead(t, vm)
	_, err := segmentOf(ibs, addr, len(code))
	require.NoError(t, err)
	ws := &WriteSet{}
	ws.SetCode(addr, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr, Path: CodePath, Version: Version{TxIndex: 1}, valStatus: ValueChanged}, Val: accounts.NewCode([]byte{0x60, 0x00})})
	vm.FlushVersionedWrites(ws, true)
	return ibs, vm, addr, code
}

// Code an earlier tx of the block wrote (a CREATE2 deploy, a 7702 delegation) is what a later tx's EXTCODECOPY copies,
// not the committed code, and the copy does not go through the segment reader.
func TestCodeSegment_CopiesCodeAnEarlierTxWrote(t *testing.T) {
	for name, written := range map[string][]byte{
		"create2 deploy":  {0x60, 0x2a, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3},
		"7702 delegation": append([]byte{0xef, 0x01, 0x00}, bytes.Repeat([]byte{0x44}, 20)...),
	} {
		t.Run(name, func(t *testing.T) {
			vm := NewVersionMap(nil)
			ibs, reader, addr, _ := newSegmentRead(t, vm)
			ws := &WriteSet{}
			ws.SetCode(addr, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr, Path: CodePath, Version: Version{TxIndex: 1}, valStatus: ValueChanged}, Val: accounts.NewCode(written)})
			vm.FlushVersionedWrites(ws, true)

			got, err := segmentOf(ibs, addr, len(written)+2)
			require.NoError(t, err)
			require.Equal(t, append(bytes.Clone(written), 0, 0), got)
			require.Zero(t, reader.segmentReads, "code from the version map must not be read from the committed state")
		})
	}
}

// A read without bytes stands for the code the transaction first read, not a later write of a lower transaction.
func TestCodeSegment_RefreshKeepsTheFirstReadCode(t *testing.T) {
	ibs, _, addr, code := segmentReadThenLowerCodeWrite(t)
	got, err := refreshCode(ibs, addr)
	require.NoError(t, err)
	require.Equal(t, code, got.Bytes)
}

func TestCodeSegment_VersionedReaderKeepsTheFirstReadCode(t *testing.T) {
	ibs, vm, addr, code := segmentReadThenLowerCodeWrite(t)
	vr := NewVersionedStateReader(5, ibs.VersionedReads(), vm, ibs.stateReader)
	got, err := vr.ReadAccountCode(addr)
	require.NoError(t, err)
	require.Equal(t, code, got)
	size, err := vr.ReadAccountCodeSize(addr)
	require.NoError(t, err)
	require.Equal(t, len(code), size)
}
