package state

import (
	"testing"

	"github.com/erigontech/erigon/execution/tracing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type codeReader struct {
	emptyReader
	addr    accounts.Address
	account *accounts.Account
	code    []byte
}

func (r *codeReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if addr == r.addr {
		a := &accounts.Account{}
		a.Copy(r.account)
		return a, nil
	}
	return nil, nil
}

func (r *codeReader) ReadAccountCode(addr accounts.Address) ([]byte, error) {
	if addr == r.addr {
		return r.code, nil
	}
	return nil, nil
}

func (r *codeReader) ReadAccountCodeSize(addr accounts.Address) (int, error) {
	if addr == r.addr {
		return len(r.code), nil
	}
	return 0, nil
}

// TestSetCodeParallel_RevertToOriginalBug pins that revert-to-original must
// diff codeHash against the versionMap's value, not the domain's, or it drops a CodePath write after an earlier same-block tx clears the code.
func TestSetCodeParallel_RevertToOriginalBug(t *testing.T) {
	delegationCode := []byte{0xef, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
		0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
	}
	codeHashA := accounts.InternCodeHash(crypto.Keccak256Hash(delegationCode))

	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})

	domainAccount := accounts.NewAccount()
	domainAccount.CodeHash = codeHashA
	domainAccount.Nonce = 1

	reader := &codeReader{
		addr:    addr,
		account: &domainAccount,
		code:    delegationCode,
	}

	vm := NewVersionMap(nil)

	ibs88 := NewWithVersionMap(reader, vm)
	defer ibs88.Close()
	ibs88.SetTxContext(100, 88)
	ibs88.SetVersion(0)

	err := ibs88.SetCode(addr, nil, tracing.CodeChangeUnspecified)
	require.NoError(t, err)

	writes88 := ibs88.VersionedWrites()
	vm.FlushVersionedWrites(writes88, true, "")

	ch, rr, ok := vm.ReadCodeHash(addr, 89)
	require.Equal(t, MVReadResultDone, rr.Status(), "TX 88 should have written CodeHashPath")
	require.True(t, ok)
	assert.Equal(t, accounts.EmptyCodeHash, ch, "TX 88 should have written EmptyCodeHash")

	ibs90 := NewWithVersionMap(reader, vm)
	defer ibs90.Close()
	ibs90.SetTxContext(100, 90)
	ibs90.SetVersion(0)

	err = ibs90.SetCode(addr, delegationCode, tracing.CodeChangeUnspecified)
	require.NoError(t, err)

	code, err := ibs90.GetCode(addr)
	require.NoError(t, err)
	assert.Equal(t, delegationCode, code,
		"GetCode after SetCode(delegationCode) should return the delegation code, not empty")

	writes90 := ibs90.VersionedWrites()
	_, hasCodeWrite := writes90.GetCode(addr)
	assert.True(t, hasCodeWrite,
		"TX 90 should have a CodePath write in versionedWrites (the revert-to-original optimisation should NOT have fired)")
}

// TestSetCodeParallel_NoMaterialize_DelegateThenRevoke pins that a delegate-then-revoke within one tx folds to no code write on the noMaterialize path:
// each SetCode must seed from the tx's own prior write, not the tx-start value.
func TestSetCodeParallel_NoMaterialize_DelegateThenRevoke(t *testing.T) {
	delegationCode := []byte{0xef, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
		0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
	}
	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})

	acc := accounts.NewAccount()
	acc.Nonce = 1
	reader := &codeReader{addr: addr, account: &acc}

	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	require.NoError(t, ibs.SetCode(addr, delegationCode, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(addr, nil, tracing.CodeChangeUnspecified))

	code, err := ibs.GetCode(addr)
	require.NoError(t, err)
	assert.Empty(t, code, "delegate-then-revoke must leave the code empty")

	writes := ibs.VersionedWrites()
	_, hasCodeWrite := writes.GetCode(addr)
	assert.False(t, hasCodeWrite, "net-zero delegate-then-revoke must fold away the CodePath write")
	assert.Empty(t, ibs.stateObjects, "noMaterialize SetCode must not cache a stateObject")
}

// TestGetDelegatedDesignationParallel_NoMaterialize_OwnWrite pins that a delegation set earlier in the same tx is visible to GetDelegatedDesignation
// on the noMaterialize path, where Code() otherwise resolves only prior-tx state.
func TestGetDelegatedDesignationParallel_NoMaterialize_OwnWrite(t *testing.T) {
	target := accounts.InternAddress([20]byte{0xBB, 0xBB})
	delegationCode := types.AddressToDelegation(target)
	addr := accounts.InternAddress([20]byte{0xAA, 0xAA})

	acc := accounts.NewAccount()
	acc.Nonce = 1
	reader := &codeReader{addr: addr, account: &acc}

	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	require.NoError(t, ibs.SetCode(addr, delegationCode, tracing.CodeChangeAuthorization))

	got, ok, err := ibs.GetDelegatedDesignation(addr)
	require.NoError(t, err)
	assert.True(t, ok, "own-tx delegation must be visible to GetDelegatedDesignation")
	assert.Equal(t, target, got)
	assert.Empty(t, ibs.stateObjects, "GetDelegatedDesignation must not materialize a stateObject")
}

// TestGetDelegatedDesignation_TracksSplitCodePublish pins an OCC window where a prior tx's CodeHashPath is visible before its CodePath:
// the speculative read must record the missing CodePath so validation rejects it once bytecode publishes.
func TestGetDelegatedDesignation_TracksSplitCodePublish(t *testing.T) {
	t.Parallel()
	authority := accounts.InternAddress([20]byte{0xaa})
	delegate := accounts.InternAddress([20]byte{0xbb})
	delegation := accounts.NewCode(types.AddressToDelegation(delegate))
	domainAccount := accounts.NewAccount()
	domainAccount.Nonce = 1
	domainAccount.CodeHash = accounts.EmptyCodeHash
	reader := &codeReader{addr: authority, account: &domainAccount}
	vm := NewVersionMap(nil)
	priorVersion := Version{TxIndex: 0, Incarnation: 0}
	vm.WriteCodeHash(authority, priorVersion, delegation.Hash, true)
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 1)
	ibs.SetVersion(0)
	hash, err := ibs.GetCodeHash(authority)
	require.NoError(t, err)
	require.Equal(t, delegation.Hash, hash)
	_, delegated, err := ibs.GetDelegatedDesignation(authority)
	require.NoError(t, err)
	require.False(t, delegated)
	reads := ibs.VersionedReads()
	_, tracked := reads.GetCode(authority)
	require.True(t, tracked)
	io := NewVersionedIO(1)
	io.RecordReads(Version{TxIndex: 1, Incarnation: 0}, reads)
	vm.WriteCode(authority, priorVersion, delegation, true)
	validity := vm.ValidateVersion(1, io, func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}, true, false, false, "")
	require.Equal(t, VersionInvalid, validity)
}
