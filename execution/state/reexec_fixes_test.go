package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// fixedAccountReader returns a fixed account from the DB/state reader.
type fixedAccountReader struct {
	minimalStateReader
	acc *accounts.Account
}

func (r *fixedAccountReader) ReadAccountData(accounts.Address) (*accounts.Account, error) {
	return r.acc, nil
}

// eqAccount is existence-only: two non-nil records match; each sub-field is
// recorded and validated as its own read.
func TestEqAccount_ExistenceOnly(t *testing.T) {
	a := &accounts.Account{Balance: *uint256.NewInt(1)}
	b := &accounts.Account{Balance: *uint256.NewInt(2)}
	assert.True(t, eqAccount(a, b), "two non-nil records must satisfy the record-level tiebreaker")
	assert.False(t, eqAccount(nil, b), "a destroyed (nil) record must fail")
	assert.False(t, eqAccount(a, nil))
}

// A MapRead whose writer-version churned but whose value is unchanged stays
// valid — a version-only churn is not a real dependency.
func TestValidateRead_MapReadValueTiebreaker(t *testing.T) {
	vm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xab, 0x01})
	vm.WriteBalance(addr, Version{TxIndex: 5}, *uint256.NewInt(100), true)
	checkVersionEq := func(rv, wv Version) VersionValidity {
		if rv == wv {
			return VersionValid
		}
		return VersionInvalid
	}
	valid := validateRead(vm, 10, addr, BalancePath, accounts.NilKey, MapRead, Version{TxIndex: 3},
		*uint256.NewInt(100), liveBalance, eqUint256, checkVersionEq, false, "")
	assert.Equal(t, VersionValid, valid, "version churn with unchanged value must stay valid")

	valid2 := validateRead(vm, 10, addr, BalancePath, accounts.NilKey, MapRead, Version{TxIndex: 3},
		*uint256.NewInt(999), liveBalance, eqUint256, checkVersionEq, false, "")
	assert.Equal(t, VersionInvalid, valid2, "a genuine value change must still invalidate")
}

// readValueUnchanged is true when the recorded read's value equals the value
// just read (a version-only churn), false on a real value change.
func TestReadValueUnchanged(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xcd, 0x02})
	ibs := NewWithVersionMap(&minimalStateReader{}, mvhm)
	ibs.versionedReads = ReadSet{}
	ibs.versionedReads.SetBalance(addr, VersionedRead[uint256.Int]{Val: *uint256.NewInt(100)})
	r := &readPathResult{mapBalanceVal: *uint256.NewInt(100)}
	assert.True(t, ibs.readValueUnchanged(addr, BalancePath, accounts.NilKey, r), "value-equal → unchanged")
	r.mapBalanceVal = *uint256.NewInt(200)
	assert.False(t, ibs.readValueUnchanged(addr, BalancePath, accounts.NilKey, r), "value-changed → not unchanged")
}

// An account absent from both the versionMap AddressPath and the DB, but with
// a BAL-prepopulated balance cell proving an earlier tx created it, is
// synthesized at read time instead of read as non-existent — a read that would
// otherwise race the creator's flush and re-execute.
func TestGetVersionedAccount_SynthesizesCreatedFromBAL(t *testing.T) {
	mvhm := NewVersionMap(nil)
	mvhm.HasBAL = true
	addr := accounts.InternAddress([20]byte{0xba, 0x01})
	mvhm.WriteBalance(addr, Version{TxIndex: 2}, *uint256.NewInt(500), true)
	ibs := NewWithVersionMap(&minimalStateReader{}, mvhm)
	ibs.txIndex = 5
	acc, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	require.NotNil(t, acc, "BAL-proven created account must be synthesized, not read as absent")
	assert.Equal(t, *uint256.NewInt(500), acc.Balance)
	rd, ok := ibs.versionedReads.GetAddress(addr)
	require.True(t, ok)
	require.NotNil(t, rd.Val, "the recorded AddressPath read must be reconciled to non-nil")
	require.NotNil(t, rd.Val.Account())
}

// A DB-present account resolved after an AddressPath map-miss must reconcile
// the recorded nil map-read marker with the loaded record: leaving the nil in
// place spuriously invalidates the reader once a later record cell (e.g. the
// calcFees coinbase record) is flushed at validation time.
func TestGetVersionedAccount_ReconcilesDBLoadedRecordRead(t *testing.T) {
	mvhm := NewVersionMap(nil)
	mvhm.HasBAL = true
	addr := accounts.InternAddress([20]byte{0xdb, 0x01})
	mvhm.WriteBalance(addr, Version{TxIndex: 2}, *uint256.NewInt(700), true)
	dbAcc := &accounts.Account{Balance: *uint256.NewInt(100), CodeHash: accounts.EmptyCodeHash}
	ibs := NewWithVersionMap(&fixedAccountReader{acc: dbAcc}, mvhm)
	ibs.txIndex = 5
	acc, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	require.NotNil(t, acc)
	assert.Equal(t, *uint256.NewInt(700), acc.Balance, "BAL floor overlays the DB balance")
	rd, ok := ibs.versionedReads.GetAddress(addr)
	require.True(t, ok)
	require.NotNil(t, rd.Val, "recorded AddressPath read must be reconciled to the loaded record")
	require.NotNil(t, rd.Val.Account())
}

// A recordRead=false record read (delegation resolution, journal reverts)
// still leaves refreshAccount's nil map-read marker in the read set; once the
// DB resolves the account the marker must be reconciled all the same —
// recordRead only means "don't add a read", not "leave a wrong one".
func TestGetStateObject_NoRecordReadStillReconciles(t *testing.T) {
	mvhm := NewVersionMap(nil)
	mvhm.HasBAL = true
	addr := accounts.InternAddress([20]byte{0xdb, 0x02})
	mvhm.WriteBalance(addr, Version{TxIndex: 2}, *uint256.NewInt(900), true)
	dbAcc := &accounts.Account{Balance: *uint256.NewInt(100), CodeHash: accounts.EmptyCodeHash}
	ibs := NewWithVersionMap(&fixedAccountReader{acc: dbAcc}, mvhm)
	ibs.txIndex = 5
	so, err := ibs.getStateObject(addr, false)
	require.NoError(t, err)
	require.NotNil(t, so)
	rd, ok := ibs.versionedReads.GetAddress(addr)
	require.True(t, ok)
	require.NotNil(t, rd.Val, "nil marker must be reconciled even with recordRead=false")
	require.NotNil(t, rd.Val.Account())
}

// Without a BAL the sub-field cells are racing worker flushes, not a
// deterministic pre-population — no synthesis.
func TestGetVersionedAccount_NoSynthesisWithoutBAL(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xba, 0x02})
	mvhm.WriteBalance(addr, Version{TxIndex: 2}, *uint256.NewInt(500), true)
	ibs := NewWithVersionMap(&minimalStateReader{}, mvhm)
	ibs.txIndex = 5
	acc, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	assert.Nil(t, acc)
}

// Cells proving only an EIP-161-empty state must not synthesize: an
// existing-empty account is not gas-equivalent to a non-existent one.
func TestGetVersionedAccount_NoSynthesisForEmpty(t *testing.T) {
	mvhm := NewVersionMap(nil)
	mvhm.HasBAL = true
	addr := accounts.InternAddress([20]byte{0xba, 0x03})
	mvhm.WriteBalance(addr, Version{TxIndex: 2}, uint256.Int{}, true)
	ibs := NewWithVersionMap(&minimalStateReader{}, mvhm)
	ibs.txIndex = 5
	acc, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	assert.Nil(t, acc, "EIP-161-empty synthesis must be suppressed")
}

// An estimate (non-Done) cell is a racing speculative write — no synthesis.
func TestGetVersionedAccount_NoSynthesisFromEstimate(t *testing.T) {
	mvhm := NewVersionMap(nil)
	mvhm.HasBAL = true
	addr := accounts.InternAddress([20]byte{0xba, 0x04})
	mvhm.WriteBalance(addr, Version{TxIndex: 2}, *uint256.NewInt(500), false)
	ibs := NewWithVersionMap(&minimalStateReader{}, mvhm)
	ibs.txIndex = 5
	acc, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	assert.Nil(t, acc)
}

// A destroyed account (SelfDestruct floor true) must not be synthesized.
func TestGetVersionedAccount_NoSynthesisAfterSelfDestruct(t *testing.T) {
	mvhm := NewVersionMap(nil)
	mvhm.HasBAL = true
	addr := accounts.InternAddress([20]byte{0xba, 0x05})
	mvhm.WriteBalance(addr, Version{TxIndex: 2}, *uint256.NewInt(500), true)
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 3}, true, true)
	ibs := NewWithVersionMap(&minimalStateReader{}, mvhm)
	ibs.txIndex = 5
	acc, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	assert.Nil(t, acc)
}

// A created contract synthesized from its BAL code cell carries the code's
// hash and a fresh incarnation, and getStateObject loads the code bytes.
func TestGetStateObject_SynthesizedContractFromBAL(t *testing.T) {
	mvhm := NewVersionMap(nil)
	mvhm.HasBAL = true
	addr := accounts.InternAddress([20]byte{0xba, 0x06})
	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	mvhm.WriteCode(addr, Version{TxIndex: 2}, accounts.NewCode(code), true)
	mvhm.WriteNonce(addr, Version{TxIndex: 2}, 1, true)
	ibs := NewWithVersionMap(&minimalStateReader{}, mvhm)
	ibs.txIndex = 5
	so, err := ibs.getStateObject(addr, true)
	require.NoError(t, err)
	require.NotNil(t, so, "BAL-proven created contract must be synthesized")
	assert.Equal(t, accounts.NewCode(code).Hash, so.data.CodeHash)
	assert.Equal(t, uint64(1), so.data.Nonce)
	assert.Equal(t, uint64(1), so.data.Incarnation, "fresh contract incarnation")
	loaded, err := so.Code()
	require.NoError(t, err)
	assert.Equal(t, code, loaded)
}

// A BAL-funded account with a stale SelfDestructPath flag must not be dropped;
// only a genuinely empty destroyed account is deleted.
func TestGetStateObject_SelfDestructedButBALFunded_StaysAlive(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x5d, 0x03})
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 2}, true, true)
	mvhm.WriteBalance(addr, Version{TxIndex: 3}, *uint256.NewInt(1000), true)
	dbAcc := &accounts.Account{CodeHash: accounts.EmptyCodeHash}
	ibs := NewWithVersionMap(&fixedAccountReader{acc: dbAcc}, mvhm)
	ibs.txIndex = 5
	so, err := ibs.getStateObject(addr, true)
	require.NoError(t, err)
	require.NotNil(t, so, "a BAL-funded account must not be dropped by a stale SD flag")
	assert.Equal(t, *uint256.NewInt(1000), so.data.Balance)
}
