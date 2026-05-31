package state

import (
	"github.com/erigontech/erigon/execution/tracing"
	"testing"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// codeReader is a stub StateReader that returns a pre-configured account with
// code for one address.  All other addresses return nil.
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

// TestSetCodeParallel_RevertToOriginalBug verifies that the "revert-to-original"
// optimisation in SetCode does not incorrectly delete CodePath writes when a
// prior transaction within the same block cleared the code.
//
// Scenario (EIP-7702 delegation flip):
//   - Domain has account with delegation code (hash A).
//   - TX 88 clears the code → writes EmptyCodeHash to versionMap.
//   - TX 90 re-sets the same delegation code.
//
// Before the fix, SetCode compared codeHash against stateObject.original.CodeHash
// which holds the *domain* value (hash A), not the versionMap value (empty).
// Because codeHash == original.CodeHash, the optimisation deleted the CodePath
// write, causing subsequent GetCode reads to return empty code via the versionMap.
func TestSetCodeParallel_RevertToOriginalBug(t *testing.T) {
	delegationCode := []byte{0xef, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
		0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
	}
	codeHashA := accounts.InternCodeHash(crypto.Keccak256Hash(delegationCode))

	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})

	// Domain state: account exists with delegation code hash A.
	domainAccount := accounts.NewAccount()
	domainAccount.CodeHash = codeHashA
	domainAccount.Nonce = 1

	reader := &codeReader{
		addr:    addr,
		account: &domainAccount,
		code:    delegationCode,
	}

	vm := NewVersionMap(nil)

	// -----------------------------------------------------------
	// TX 88: clear the code (write EmptyCodeHash + nil code)
	// -----------------------------------------------------------
	ibs88 := NewWithVersionMap(reader, vm)
	ibs88.SetTxContext(100, 88)
	ibs88.SetVersion(0)

	err := ibs88.SetCode(addr, nil, tracing.CodeChangeUnspecified) // clear code
	require.NoError(t, err)

	// Flush TX 88 writes to versionMap
	writes88 := ibs88.VersionedWrites(false)
	vm.FlushVersionedWrites(writes88, true, "")

	// Verify TX 88 wrote empty code hash to versionMap
	rr := vm.Read(addr, CodeHashPath, accounts.NilKey, 89)
	require.Equal(t, MVReadResultDone, rr.Status(), "TX 88 should have written CodeHashPath")
	ch, ok := rr.Value().(accounts.CodeHash)
	require.True(t, ok)
	assert.Equal(t, accounts.EmptyCodeHash, ch, "TX 88 should have written EmptyCodeHash")

	// -----------------------------------------------------------
	// TX 90: re-set the same delegation code (hash A)
	// -----------------------------------------------------------
	ibs90 := NewWithVersionMap(reader, vm)
	ibs90.SetTxContext(100, 90)
	ibs90.SetVersion(0)

	err = ibs90.SetCode(addr, delegationCode, tracing.CodeChangeUnspecified)
	require.NoError(t, err)

	// -----------------------------------------------------------
	// Verify: GetCode on the TX 90 IBS should return the delegation code,
	// NOT empty/nil.
	// -----------------------------------------------------------
	code, err := ibs90.GetCode(addr)
	require.NoError(t, err)
	assert.Equal(t, delegationCode, code,
		"GetCode after SetCode(delegationCode) should return the delegation code, not empty")

	// Also verify that the versionedWrites contain the CodePath entry.
	writes90 := ibs90.VersionedWrites(false)
	hasCodeWrite := false
	for _, w := range writes90 {
		if w.Header().Address == addr && w.Header().Path == CodePath {
			hasCodeWrite = true
			break
		}
	}
	assert.True(t, hasCodeWrite,
		"TX 90 should have a CodePath write in versionedWrites (the revert-to-original optimisation should NOT have fired)")
}
