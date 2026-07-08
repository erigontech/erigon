package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// emptyReader is a stub StateReader that returns empty/nil for all reads.
type emptyReader struct{}

func (r *emptyReader) ReadAccountData(accounts.Address) (*accounts.Account, error) { return nil, nil }
func (r *emptyReader) ReadAccountDataForDebug(accounts.Address) (*accounts.Account, error) {
	return nil, nil
}
func (r *emptyReader) ReadAccountStorage(accounts.Address, accounts.StorageKey) (uint256.Int, bool, error) {
	return uint256.Int{}, false, nil
}
func (r *emptyReader) HasStorage(accounts.Address) (bool, error)               { return false, nil }
func (r *emptyReader) ReadAccountCode(accounts.Address) ([]byte, error)        { return nil, nil }
func (r *emptyReader) ReadAccountCodeSize(accounts.Address) (int, error)       { return 0, nil }
func (r *emptyReader) ReadAccountIncarnation(accounts.Address) (uint64, error) { return 0, nil }
func (r *emptyReader) SetTrace(bool, string)                                   {}
func (r *emptyReader) Trace() bool                                             { return false }
func (r *emptyReader) TracePrefix() string                                     { return "" }

// TestValueTiebreaker_BalancePath verifies that validation does not
// invalidate a StorageRead when the versionMap Done value matches the
// read value. This prevents unnecessary re-executions that cause
// cascading state errors.
func TestValueTiebreaker_BalancePath(t *testing.T) {
	vm := NewVersionMap(nil)

	addr := accounts.InternAddress([20]byte{0x01})
	balance := uint256.NewInt(1000)

	// Write a balance to the versionMap at txIndex=5
	vm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 5, Incarnation: 0}, *balance, true)

	// Validate a read from txIndex=10 that read the SAME value from storage
	readVal := *balance // Same value

	valid := vm.validateRead(10, addr, BalancePath, accounts.NilKey, StorageRead, Version{TxIndex: UnknownDep},
		readVal, // value tiebreaker
		func(rv, wv Version) VersionValidity { return VersionValid },
		false, "")

	assert.Equal(t, VersionValid, valid, "Should be valid when StorageRead value matches versionMap Done value")
}

// TestValueTiebreaker_DifferentBalance verifies that validation DOES
// invalidate when the StorageRead value differs from the versionMap value.
func TestValueTiebreaker_DifferentBalance(t *testing.T) {
	vm := NewVersionMap(nil)

	addr := accounts.InternAddress([20]byte{0x01})

	// Write balance=1000 to versionMap
	vm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 5, Incarnation: 0}, *uint256.NewInt(1000), true)

	// Validate a read that got balance=500 from storage (stale)
	readVal := *uint256.NewInt(500)

	valid := vm.validateRead(10, addr, BalancePath, accounts.NilKey, StorageRead, Version{TxIndex: UnknownDep},
		readVal,
		func(rv, wv Version) VersionValidity { return VersionValid },
		false, "")

	assert.Equal(t, VersionInvalid, valid, "Should be invalid when StorageRead value differs from versionMap Done value")
}

// TestValueTiebreaker_NoncePath verifies nonce comparison works.
func TestValueTiebreaker_NoncePath(t *testing.T) {
	vm := NewVersionMap(nil)

	addr := accounts.InternAddress([20]byte{0x02})

	// Write nonce=42 to versionMap
	vm.Write(addr, NoncePath, accounts.NilKey, Version{TxIndex: 5, Incarnation: 0}, uint64(42), true)

	// Same nonce from storage → valid
	valid := vm.validateRead(10, addr, NoncePath, accounts.NilKey, StorageRead, Version{TxIndex: UnknownDep},
		uint64(42),
		func(rv, wv Version) VersionValidity { return VersionValid },
		false, "")
	assert.Equal(t, VersionValid, valid, "Same nonce should be valid")

	// Different nonce → invalid
	valid = vm.validateRead(10, addr, NoncePath, accounts.NilKey, StorageRead, Version{TxIndex: UnknownDep},
		uint64(41),
		func(rv, wv Version) VersionValidity { return VersionValid },
		false, "")
	assert.Equal(t, VersionInvalid, valid, "Different nonce should be invalid")
}

// TestValuesEqual verifies the valuesEqual helper for all path types.
func TestValuesEqual(t *testing.T) {
	// BalancePath
	b1 := uint256.NewInt(100)
	b2 := uint256.NewInt(100)
	b3 := uint256.NewInt(200)
	assert.True(t, valuesEqual(BalancePath, *b1, *b2), "Same balance should be equal")
	assert.False(t, valuesEqual(BalancePath, *b1, *b3), "Different balance should not be equal")

	// NoncePath
	assert.True(t, valuesEqual(NoncePath, uint64(5), uint64(5)), "Same nonce")
	assert.False(t, valuesEqual(NoncePath, uint64(5), uint64(6)), "Different nonce")

	// IncarnationPath
	assert.True(t, valuesEqual(IncarnationPath, uint64(1), uint64(1)), "Same incarnation")
	assert.False(t, valuesEqual(IncarnationPath, uint64(1), uint64(2)), "Different incarnation")

	// Nil values
	assert.True(t, valuesEqual(BalancePath, nil, nil), "Both nil should be equal")
	assert.False(t, valuesEqual(BalancePath, *b1, nil), "One nil should not be equal")
	assert.False(t, valuesEqual(BalancePath, nil, *b1), "One nil should not be equal")

	// CodePath (review R2: was dead — default:false — so code readers never relaxed)
	code := []byte{0x60, 0x00, 0x56}
	assert.True(t, valuesEqual(CodePath, code, []byte{0x60, 0x00, 0x56}), "Same code should be equal")
	assert.False(t, valuesEqual(CodePath, code, []byte{0x60, 0x01}), "Different code should not be equal")
}

// TestValuesEqual_AddressPathExistenceOnly: the AddressPath record comparison is
// existence-only. createObject stamps the record with initial fields (balance 0,
// nonce 0, empty code); the real fields are set afterwards and land in their own
// Balance/Nonce/Incarnation/CodeHash cells that refreshVersionedAccount overlays
// AND records as separate reads. So the record comparison must ignore ALL fields
// (each validated via its own read) and only confirm existence — else a created
// funded EOA or a contract (nonce 1 + code) re-executes against the stale initial
// stamp. nil (creation/deletion) is still caught.
func TestValuesEqual_AddressPathExistenceOnly(t *testing.T) {
	ch := accounts.EmptyCodeHash
	initial := &accounts.Account{CodeHash: ch}
	funded := &accounts.Account{Balance: *uint256.NewInt(61522), CodeHash: ch}
	contract := &accounts.Account{Nonce: 1, Incarnation: 1, CodeHash: accounts.InternCodeHash(crypto.HashData([]byte{0x60, 0x00}))}
	assert.True(t, valuesEqual(AddressPath, funded, initial), "funded EOA vs initial stamp: existence-only → equal")
	assert.True(t, valuesEqual(AddressPath, contract, initial), "contract vs initial stamp: existence-only → equal")
	assert.False(t, valuesEqual(AddressPath, funded, nil), "nil (deletion) must still be caught")
	assert.False(t, valuesEqual(AddressPath, nil, contract), "nil (creation) must still be caught")
}

// TestFoldedSubfieldRead_RelaxesOnUnchangedField (review R1): a sub-field read
// with no dedicated cell folds onto AddressPath; when calcFees re-stamps the
// AddressPath cell (new incarnation/balance, same code hash), the folded read
// must relax on the unchanged field instead of version-only re-invalidating —
// e.g. EXTCODEHASH(block.coinbase) on every fee-paying tx.
func TestFoldedSubfieldRead_RelaxesOnUnchangedField(t *testing.T) {
	vm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xf9, 0x7e, 0x18, 0x0c})
	ch := accounts.EmptyCodeHash
	acc5 := &accounts.Account{Balance: *uint256.NewInt(100), CodeHash: ch}
	acc10 := &accounts.Account{Balance: *uint256.NewInt(200), CodeHash: ch} // fee re-stamp, same code hash
	vm.Write(addr, AddressPath, accounts.NilKey, Version{TxIndex: 5}, acc5, true)
	vm.Write(addr, AddressPath, accounts.NilKey, Version{TxIndex: 10}, acc10, true)
	// No CodeHashPath cell → a code-hash read at tx 11 folds onto AddressPath at
	// its recorded version (5), which floors to the re-stamped tx-10 cell.
	ck := func(rv, wv Version) VersionValidity {
		if rv == wv {
			return VersionValid
		}
		return VersionInvalid
	}
	valid := vm.validateRead(11, addr, CodeHashPath, accounts.NilKey, MapRead, Version{TxIndex: 5}, ch, ck, false, "")
	assert.Equal(t, VersionValid, valid, "folded code-hash read must relax when the record's code hash is unchanged")
}

// TestAddressRecordReadReconciledWithSupersedingBalance reproduces the
// coinbase over-invalidation storm. A tx reads an account record (AddressPath)
// that floors to an early flushed version, while a later pre-populated
// BalancePath (the BAL fee credit) supersedes the balance.
// refreshVersionedAccount overlays the superseding balance onto the account
// the tx uses — but the read recorded in the readSet kept the stale snapshot,
// so a later account-record flush churned the AddressPath version and
// validation spuriously invalidated the read.
func TestAddressRecordReadReconciledWithSupersedingBalance(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xf9, 0x7e, 0x18, 0x0c})

	// Account record flushed at tx 1 (balance 100).
	acc := &accounts.Account{Balance: *uint256.NewInt(100)}
	mvhm.Write(addr, AddressPath, accounts.NilKey, Version{TxIndex: 1}, acc, true)
	// Later pre-populated balance (BAL fee credit) at tx 2 (balance 200).
	mvhm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 2}, *uint256.NewInt(200), true)

	ibs := NewWithVersionMap(&emptyReader{}, mvhm)
	ibs.txIndex = 4

	// Account-record read (e.g. a STATICCALL / existence probe).
	so, err := ibs.getStateObject(addr, true)
	require.NoError(t, err)
	require.NotNil(t, so)
	assert.Equal(t, *uint256.NewInt(200), so.data.Balance, "tx must see the superseding balance")

	rd, ok := ibs.versionedReads[addr][AccountKey{Path: AddressPath}]
	require.True(t, ok, "AddressPath read must be recorded")
	recordedAcc, ok := rd.Val.(*accounts.Account)
	require.True(t, ok)
	assert.Equal(t, *uint256.NewInt(200), recordedAcc.Balance,
		"recorded AddressPath read should reflect the superseding BalancePath, not the stale snapshot")

	// Apply-flush writes the up-to-date record at tx 3 (balance 200).
	acc2 := &accounts.Account{Balance: *uint256.NewInt(200)}
	mvhm.Write(addr, AddressPath, accounts.NilKey, Version{TxIndex: 3}, acc2, true)

	// Validation must stay valid (no re-execution) — via ValidateVersion so the
	// read-set iteration is exercised too (review F).
	io := NewVersionedIO(5)
	io.RecordReads(Version{TxIndex: 4, Incarnation: 0}, ibs.VersionedReads())
	checkVersionEqual := func(rv, wv Version) VersionValidity {
		if rv == wv {
			return VersionValid
		}
		return VersionInvalid
	}
	valid := mvhm.ValidateVersion(4, io, checkVersionEqual, false, "")
	assert.Equal(t, VersionValid, valid, "record read must stay valid after the later record flush")
}

// TestCachedRefreshDoesNotReconcileRecordedRead (review A): the recorded-read
// reconciliation must fire only from the clean getVersionedAccount load path,
// not from getStateObject's cached-object refresh — which passes the tx's own
// (possibly later-reverted) data. Otherwise the recorded read stops meaning
// "what was read".
func TestCachedRefreshDoesNotReconcileRecordedRead(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x0c, 0x0b})
	acc := &accounts.Account{Balance: *uint256.NewInt(100)}
	mvhm.Write(addr, AddressPath, accounts.NilKey, Version{TxIndex: 1}, acc, true)
	mvhm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 2}, *uint256.NewInt(200), true) // superseding → triggers overlay copy

	ibs := NewWithVersionMap(&emptyReader{}, mvhm)
	ibs.txIndex = 5
	original := &accounts.Account{Balance: *uint256.NewInt(100)}
	ibs.versionedReads = ReadSet{}
	ibs.versionedReads.Set(VersionedRead{Address: addr, Path: AddressPath, Key: accounts.NilKey, Source: MapRead, Version: Version{TxIndex: 1}, Val: original})

	// Simulate getStateObject's cached-object refresh: live &so.data, UnknownVersion.
	live := &accounts.Account{Balance: *uint256.NewInt(100)}
	_, _, _, err := ibs.refreshVersionedAccount(addr, live, StorageRead, UnknownVersion)
	require.NoError(t, err)

	rd := ibs.versionedReads[addr][AccountKey{Path: AddressPath}]
	got, ok := rd.Val.(*accounts.Account)
	require.True(t, ok)
	assert.Equal(t, *uint256.NewInt(100), got.Balance,
		"cached-refresh must not overwrite the recorded read with the overlaid value")
}

// TestCreatedAccountResolvedFromBAL: an account absent from the DB but carrying
// a BAL-populated sub-field cell from an earlier tx must resolve as created —
// read from the versionMap rather than returning "absent" and re-executing when
// the creating tx later flushes its AddressPath record. This is the dominant
// residual re-exec source (the storage(-2.-1)!=(N.0) pattern in the trace).
func TestCreatedAccountResolvedFromBAL(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x08, 0x16})

	// A prior tx funds X at tx 4; the BAL pre-populates its balance. No
	// AddressPath cell (the record is never in the BAL), no DB entry, no code
	// (EOA funding — the common created-account case).
	mvhm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 4}, *uint256.NewInt(100), true)

	ibs := NewWithVersionMap(&emptyReader{}, mvhm)
	ibs.txIndex = 10

	so, err := ibs.getStateObject(addr, true)
	require.NoError(t, err)
	require.NotNil(t, so, "account with a BAL sub-field cell from an earlier tx must resolve as created, not absent")
	assert.Equal(t, *uint256.NewInt(100), so.data.Balance, "must see the BAL-populated balance")

	// The creating tx flushes X's AddressPath record at tx 4 — a real funded
	// EOA carries EmptyCodeHash, which the synthesized record must match.
	created := &accounts.Account{Balance: *uint256.NewInt(100)}
	created.CodeHash = accounts.EmptyCodeHash
	mvhm.Write(addr, AddressPath, accounts.NilKey, Version{TxIndex: 4}, created, true)

	// Validation must stay valid — the read resolved from the BAL, so no re-exec.
	io := NewVersionedIO(11)
	io.RecordReads(Version{TxIndex: 10, Incarnation: 0}, ibs.VersionedReads())
	checkVersionEqual := func(rv, wv Version) VersionValidity {
		if rv == wv {
			return VersionValid
		}
		return VersionInvalid
	}
	valid := mvhm.ValidateVersion(10, io, checkVersionEqual, false, "")
	assert.Equal(t, VersionValid, valid, "created-account read must not re-execute")
}

// TestCreatedContractResolvedFromBAL: post-EIP-6780 a contract created by a
// prior tx always has incarnation 1 (cross-tx SD+recreate is impossible), so a
// created contract absent from the DB is fully synthesizable from the BAL —
// incarnation 1, codehash from the CodePath cell — and must not re-execute.
func TestCreatedContractResolvedFromBAL(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x08, 0x88})
	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	codeHash := accounts.InternCodeHash(crypto.HashData(code))

	// A prior tx deploys X as a contract at tx 4; the BAL carries its balance,
	// nonce and code. No AddressPath cell, no DB entry, no selfdestruct.
	mvhm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 4}, *uint256.NewInt(50), true)
	mvhm.Write(addr, NoncePath, accounts.NilKey, Version{TxIndex: 4}, uint64(1), true)
	mvhm.Write(addr, CodePath, accounts.NilKey, Version{TxIndex: 4}, code, true)

	ibs := NewWithVersionMap(&emptyReader{}, mvhm)
	ibs.txIndex = 10

	so, err := ibs.getStateObject(addr, true)
	require.NoError(t, err)
	require.NotNil(t, so, "created contract must resolve as created, not absent")
	assert.Equal(t, *uint256.NewInt(50), so.data.Balance)
	assert.Equal(t, uint64(1), so.data.Nonce)
	assert.Equal(t, uint64(1), so.data.Incarnation, "fresh contract incarnation = 1")
	assert.Equal(t, codeHash, so.data.CodeHash, "codehash = keccak(code)")

	// The creating tx flushes X's record at tx 4.
	created := &accounts.Account{Balance: *uint256.NewInt(50), Nonce: 1, Incarnation: 1, CodeHash: codeHash}
	mvhm.Write(addr, AddressPath, accounts.NilKey, Version{TxIndex: 4}, created, true)

	io := NewVersionedIO(11)
	io.RecordReads(Version{TxIndex: 10, Incarnation: 0}, ibs.VersionedReads())
	checkVersionEqual := func(rv, wv Version) VersionValidity {
		if rv == wv {
			return VersionValid
		}
		return VersionInvalid
	}
	valid := mvhm.ValidateVersion(10, io, checkVersionEqual, false, "")
	assert.Equal(t, VersionValid, valid, "created-contract read must not re-execute")
}

// fixedReader returns a preset account for ReadAccountData (a pre-block DB
// record), delegating everything else to emptyReader.
type fixedReader struct {
	emptyReader
	acc *accounts.Account
}

func (r *fixedReader) ReadAccountData(accounts.Address) (*accounts.Account, error) { return r.acc, nil }

// TestDBRecordOverlaysBALBalance: a record loaded from the DB (pre-block) must
// overlay a newer in-block BAL BalancePath value, not keep the stale pre-block
// balance. The bug was that the fresh DB-load stamped the record with the tx's
// own version, so refreshVersionedAccount's bversion>readVersion overlay guard
// skipped the (lower-versioned) BAL cell — leaving a stale balance that
// re-executes at validation.
func TestDBRecordOverlaysBALBalance(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xab, 0xcd})
	// BAL: a prior tx changed X's balance to 200 at tx 5.
	mvhm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 5}, *uint256.NewInt(200), true)
	// DB: X exists pre-block with balance 100.
	dbAcc := &accounts.Account{Balance: *uint256.NewInt(100)}
	dbAcc.CodeHash = accounts.EmptyCodeHash

	ibs := NewWithVersionMap(&fixedReader{acc: dbAcc}, mvhm)
	ibs.txIndex = 10

	so, err := ibs.getStateObject(addr, true)
	require.NoError(t, err)
	require.NotNil(t, so)
	assert.Equal(t, *uint256.NewInt(200), so.data.Balance,
		"DB-read record must overlay the BAL's in-block balance, not keep the stale pre-block value")

	// The prior tx flushes X's record (balance 200) at tx 5.
	rec := &accounts.Account{Balance: *uint256.NewInt(200)}
	rec.CodeHash = accounts.EmptyCodeHash
	mvhm.Write(addr, AddressPath, accounts.NilKey, Version{TxIndex: 5}, rec, true)

	io := NewVersionedIO(11)
	io.RecordReads(Version{TxIndex: 10, Incarnation: 0}, ibs.VersionedReads())
	checkVersionEqual := func(rv, wv Version) VersionValidity {
		if rv == wv {
			return VersionValid
		}
		return VersionInvalid
	}
	valid := mvhm.ValidateVersion(10, io, checkVersionEqual, false, "")
	assert.Equal(t, VersionValid, valid, "overlaid DB record read must not re-execute")
}

// TestStaleRecordCellOverlaidByBAL: a mid-flight worker can leave a stale
// AddressPath record cell at a higher version than the BAL's BalancePath cell.
// The record read must still resolve the BAL's authoritative balance (the BAL
// is the source of truth for floor(txIndex)), not let the stale record shadow
// it via the bversion>readVersion overlay guard. A read that doesn't resolve
// from the BAL is exactly the non-determinism that causes re-execution.
func TestStaleRecordCellOverlaidByBAL(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xf9, 0x7e})
	// BAL: authoritative balance 200 for floor(10) (changed at tx 5).
	mvhm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 5}, *uint256.NewInt(200), true)
	// A mid-flight worker left a stale AddressPath record at tx 7 (balance 100),
	// at a higher version than the BalancePath cell.
	stale := &accounts.Account{Balance: *uint256.NewInt(100)}
	stale.CodeHash = accounts.EmptyCodeHash
	mvhm.Write(addr, AddressPath, accounts.NilKey, Version{TxIndex: 7}, stale, true)

	ibs := NewWithVersionMap(&emptyReader{}, mvhm)
	ibs.txIndex = 10

	so, err := ibs.getStateObject(addr, true)
	require.NoError(t, err)
	require.NotNil(t, so)
	assert.Equal(t, *uint256.NewInt(200), so.data.Balance,
		"the BAL's balance must override a stale higher-versioned record cell")
}

// TestVersionedWriteVersion verifies that VersionedWrite entries at
// txIndex=0 are still reachable. The bug was that finalizeTx appended
// writes without Version (zero value = txIndex=0), making them only
// visible via floor(0) but invisible to floor(N-1) for N > 1.
func TestVersionedWriteVersion(t *testing.T) {
	vm := NewVersionMap(nil)

	addr := accounts.InternAddress([20]byte{0x03})

	// Write at txIndex=10 with correct Version
	vm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 10, Incarnation: 1}, *uint256.NewInt(500), true)

	// Read at txIndex=11 should find txIndex=10
	rr := vm.Read(addr, BalancePath, accounts.NilKey, 11)
	assert.Equal(t, MVReadResultDone, rr.Status(), "Should find entry at floor(10)")
	assert.Equal(t, 10, rr.DepIdx(), "Should be from txIndex 10")

	// Now also write at txIndex=0 (simulates the zero-Version bug)
	vm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, *uint256.NewInt(999), true)

	// Read at txIndex=11 should STILL find txIndex=10 (not 0)
	rr = vm.Read(addr, BalancePath, accounts.NilKey, 11)
	assert.Equal(t, MVReadResultDone, rr.Status())
	assert.Equal(t, 10, rr.DepIdx(), "Should find txIndex=10, not txIndex=0")

	// But read at txIndex=1 should find txIndex=0
	rr = vm.Read(addr, BalancePath, accounts.NilKey, 1)
	assert.Equal(t, MVReadResultDone, rr.Status())
	assert.Equal(t, 0, rr.DepIdx(), "Should find txIndex=0 for floor(0)")
}

// TestAccessListResetInIBSReset verifies that IBS.Reset() clears the
// access list, preventing stale warm addresses from leaking between
// TX executions on the same worker.
func TestAccessListResetInIBSReset(t *testing.T) {
	ibs := New(nil)

	// Add an address to the access list
	testAddr := accounts.InternAddress([20]byte{0x42})
	ibs.AddAddressToAccessList(testAddr)
	assert.True(t, ibs.AddressInAccessList(testAddr), "Address should be warm")

	// Reset
	ibs.Reset()

	// Address should be cold after reset
	assert.False(t, ibs.AddressInAccessList(testAddr), "Address should be cold after Reset")
}

// TestAddressAccessResetInIBSReset verifies that IBS.Reset() clears BAL
// address-access recording. An aborted incarnation never harvests
// AccessedAddresses, and only regular txs call Prepare (which re-inits
// recording) — a worker next assigned a system block-start/block-end
// transaction never calls Prepare, so it would harvest the leftovers into
// its own block's access list as phantom entries.
func TestAddressAccessResetInIBSReset(t *testing.T) {
	ibs := New(nil)
	sender := accounts.InternAddress([20]byte{0x01})
	coinbase := accounts.InternAddress([20]byte{0x02})
	leaked := accounts.InternAddress([20]byte{0x42})
	// Prepare enables access recording at tx start.
	require.NoError(t, ibs.Prepare(&chain.Rules{}, sender, coinbase, accounts.NilAddress, nil, nil, nil))
	ibs.MarkAddressAccess(leaked, false)
	// Tx aborts: AccessedAddresses is never harvested. The worker resets
	// the shared IBS before the next task.
	ibs.Reset()
	assert.Empty(t, ibs.AccessedAddresses(), "no recorded accesses should survive Reset")
}

// TestTransientStorageResetInIBSReset verifies that IBS.Reset() clears
// transient storage (EIP-1153).
func TestTransientStorageResetInIBSReset(t *testing.T) {
	ibs := New(nil)

	testAddr := accounts.InternAddress([20]byte{0x42})
	testKey := accounts.InternKey([32]byte{0x01})

	// Set transient storage
	ibs.SetTransientState(testAddr, testKey, *uint256.NewInt(42))
	val := ibs.GetTransientState(testAddr, testKey)
	assert.False(t, val.IsZero(), "Transient storage should be set")

	// Reset
	ibs.Reset()

	// Transient storage should be cleared
	val = ibs.GetTransientState(testAddr, testKey)
	assert.True(t, val.IsZero(), "Transient storage should be zero after Reset")
}

// TestCodeReadFromVersionMap verifies that the versionMap CodePath
// entries are accessible. This ensures EIP-7702 synthetic code
// (delegation prefix) written by a prior TX is visible to subsequent
// TXs via the versionMap.
func TestCodeReadFromVersionMap(t *testing.T) {
	vm := NewVersionMap(nil)

	addr := accounts.InternAddress([20]byte{0x55})

	// Write EIP-7702 delegation code to versionMap at txIndex=5
	delegationCode := []byte{0xef, 0x01, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05,
		0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
		0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14}
	vm.Write(addr, CodePath, accounts.NilKey, Version{TxIndex: 5, Incarnation: 0}, delegationCode, true)

	// Read at txIndex=10 should find the code
	rr := vm.Read(addr, CodePath, accounts.NilKey, 10)
	require.Equal(t, MVReadResultDone, rr.Status(), "Should find CodePath entry")

	code, ok := rr.Value().([]byte)
	require.True(t, ok, "Value should be []byte")
	assert.Equal(t, delegationCode, code, "Code should match")
	assert.Equal(t, byte(0xef), code[0], "Should have EIP-7702 prefix")

	// Read at txIndex=3 should NOT find it (before the write)
	rr = vm.Read(addr, CodePath, accounts.NilKey, 3)
	assert.NotEqual(t, MVReadResultDone, rr.Status(), "Should not find code before write txIndex")
}

// TestTouchUpdates_Account verifies that TouchUpdates feeds account field
// writes directly to commitment.Updates via TouchPlainKeyDirect, producing
// merged Updates with correct key count.
func TestTouchUpdates_Account(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x42})

	writes := VersionedWrites{
		{Address: addr, Path: BalancePath, Val: *uint256.NewInt(1000)},
		{Address: addr, Path: NoncePath, Val: uint64(5)},
		{Address: addr, Path: IncarnationPath, Val: uint64(1)},
		{Address: addr, Path: CodeHashPath, Val: accounts.InternCodeHash([32]byte{0xaa, 0xbb})},
	}

	updates := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), func(k []byte) []byte { return k })
	writes.TouchUpdates(updates)

	// All 4 fields merge into 1 key (same address)
	assert.Equal(t, uint64(1), updates.Size(), "Should have 1 merged key for same address")
}

// TestTouchUpdates_AccountModeParallel: ModeParallel must merge per-field
// touches of one address additively, like ModeUpdate.
func TestTouchUpdates_AccountModeParallel(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x42})

	writes := VersionedWrites{
		{Address: addr, Path: BalancePath, Val: *uint256.NewInt(1000)},
		{Address: addr, Path: NoncePath, Val: uint64(5)},
		{Address: addr, Path: CodeHashPath, Val: accounts.InternCodeHash([32]byte{0xaa, 0xbb})},
	}

	updates := commitment.NewUpdates(commitment.ModeParallel, t.TempDir(), func(k []byte) []byte { return k })
	defer updates.Close()
	writes.TouchUpdates(updates)

	assert.Equal(t, uint64(1), updates.Size(), "Should have 1 merged key for same address")
}

// TestToTouchKeys_Storage verifies storage entries use correct composite keys.
func TestToTouchKeys_Storage(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x55})
	slot1 := accounts.InternKey([32]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04})
	slot2 := accounts.InternKey([32]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05})
	val1 := *uint256.NewInt(42)
	val2 := *uint256.NewInt(0) // zero = delete

	writes := VersionedWrites{
		{Address: addr, Path: StoragePath, Key: slot1, Val: val1},
		{Address: addr, Path: StoragePath, Key: slot2, Val: val2},
	}

	updates := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), func(k []byte) []byte { return k })
	writes.TouchUpdates(updates)

	// Should have 2 unique keys (different slots)
	assert.Equal(t, uint64(2), updates.Size(), "Should have 2 storage keys")
}

// TestTouchUpdates_Code verifies code writes feed through TouchUpdates.
func TestTouchUpdates_Code(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xd2})
	code := []byte{0xef, 0x01, 0x00, 0x01, 0x02, 0x03}

	writes := VersionedWrites{
		{Address: addr, Path: CodePath, Val: code},
	}

	updates := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), func(k []byte) []byte { return k })
	writes.TouchUpdates(updates)

	assert.Equal(t, uint64(1), updates.Size(), "Should have 1 code key")
}

// TestTouchUpdates_MixedBatch verifies that a mixed batch of writes
// (accounts + storage + code) feeds correctly through TouchUpdates.
func TestTouchUpdates_MixedBatch(t *testing.T) {
	addr1 := accounts.InternAddress([20]byte{0x01})
	addr2 := accounts.InternAddress([20]byte{0x02})
	slot := accounts.InternKey([32]byte{0x04})

	writes := VersionedWrites{
		// Account 1: balance + nonce + code
		{Address: addr1, Path: BalancePath, Val: *uint256.NewInt(100)},
		{Address: addr1, Path: NoncePath, Val: uint64(1)},
		{Address: addr1, Path: IncarnationPath, Val: uint64(0)},
		{Address: addr1, Path: CodeHashPath, Val: accounts.InternCodeHash([32]byte{})},
		{Address: addr1, Path: CodePath, Val: []byte{0x60, 0x00}},
		// Account 2: storage write
		{Address: addr2, Path: StoragePath, Key: slot, Val: *uint256.NewInt(999)},
	}

	updates := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), func(k []byte) []byte { return k })
	writes.TouchUpdates(updates)

	// addr1: account fields + code all merge into 1 key
	// addr2+slot: 1 storage key
	// Total: 2 unique keys
	assert.Equal(t, uint64(2), updates.Size(), "2 unique keys (addr1 merged, addr2 storage)")
}

// TestBlockStateCacheWriteAccount_NilCommitted verifies that WriteAccount
// doesn't panic when the committed cache has a nil account entry.
// This can happen when PutCommittedAccount stores nil (account doesn't exist).
func TestBlockStateCacheWriteAccount_NilCommitted(t *testing.T) {
	cache := NewBlockStateCache()

	addr := accounts.InternAddress([20]byte{0x42})

	// Put a nil committed account (account doesn't exist in pre-block state)
	cache.PutCommittedAccount(addr, nil)

	// Write a new account — should not panic
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(1000)
	acc.Nonce = 1
	enc := accounts.SerialiseV3(&acc)

	assert.NotPanics(t, func() {
		cache.WriteAccount(addr, enc, 1)
	}, "WriteAccount should not panic with nil committed account")

	// Verify the write is recorded.
	current, ok := cache.GetCurrentAccount(addr)
	assert.True(t, ok, "Should have current account")
	assert.Equal(t, enc, current, "Current account should match written value")
}

// TestBlockStateCacheWriteAccountUpdatesCurrent verifies that successive
// writes update the current view to the latest value (last write wins
// for read access via GetCurrentAccount). The full per-tx history is
// preserved in writeLog for Flush.
func TestBlockStateCacheWriteAccountUpdatesCurrent(t *testing.T) {
	cache := NewBlockStateCache()

	addr := accounts.InternAddress([20]byte{0x55})

	// Set up committed account
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(500)
	acc.Nonce = 3
	cache.PutCommittedAccount(addr, &acc)

	enc := accounts.SerialiseV3(&acc)
	cache.WriteAccount(addr, enc, 3)

	acc2 := accounts.NewAccount()
	acc2.Balance = *uint256.NewInt(600)
	acc2.Nonce = 3
	enc2 := accounts.SerialiseV3(&acc2)
	cache.WriteAccount(addr, enc2, 5)

	current, ok := cache.GetCurrentAccount(addr)
	assert.True(t, ok)
	assert.Equal(t, enc2, current, "GetCurrentAccount should return the latest write")
}

// Pins that a second DeleteAccount in the same block is a writeLog no-op —
// Flush must emit exactly one DomainDel per address (matching serial's IBS
// short-circuit). Without this dedup the redundant nil-history entry feeds
// into commitment-cache step keys and produces a non-deterministic state
// root between parallel-exec nodes validating each other's blocks.
func TestBlockStateCacheDeleteAccount_IdempotentInBlock(t *testing.T) {
	cache := NewBlockStateCache()
	addr := accounts.InternAddress([20]byte{0x77})

	cache.DeleteAccount(addr, 1)
	cache.DeleteAccount(addr, 2)

	deletes := 0
	for i := range cache.writeLog {
		if cache.writeLog[i].kind == bcOpDeleteAccount && cache.writeLog[i].addr == addr {
			deletes++
		}
	}
	assert.Equal(t, 1, deletes, "second DeleteAccount in the same block must not append a second writeLog entry")

	enc, present := cache.GetCurrentAccount(addr)
	assert.True(t, present, "current view must still report addr as present-and-empty")
	assert.Nil(t, enc, "current view value must remain nil")
}

// Pins the recreate-then-redelete pattern: an intervening WriteAccount
// resets the dedup so the next DeleteAccount IS recorded — only the
// "no write in between" duplicate is collapsed.
func TestBlockStateCacheDeleteAccount_RecreateThenDeleteRecords(t *testing.T) {
	cache := NewBlockStateCache()
	addr := accounts.InternAddress([20]byte{0x88})

	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(1)
	enc := accounts.SerialiseV3(&acc)

	cache.DeleteAccount(addr, 1)
	cache.WriteAccount(addr, enc, 2)
	cache.DeleteAccount(addr, 3)

	deletes := 0
	for i := range cache.writeLog {
		if cache.writeLog[i].kind == bcOpDeleteAccount && cache.writeLog[i].addr == addr {
			deletes++
		}
	}
	assert.Equal(t, 2, deletes, "delete after recreate must be recorded; only no-op duplicates are collapsed")
}

// TestSelfDestructKeepsDirtyStorageReadableSameTx verifies that after an
// account self-destructs (versionMap active), a subsequent same-tx GetState
// still returns the dirty value written before the SELFDESTRUCT. Pre-Cancun
// (and for CALL-based SELFDESTRUCT generally) the account stays alive until
// end-of-tx, so re-entered code must see the real storage — not zero.
//
// Regression: IBS.Selfdestruct used to versionWritten(StoragePath, key, 0)
// for every dirty slot to feed the parallel commitment calculator. Because
// versionedRead consults versionedWrites before the stateObject, those
// spurious zero writes made same-tx re-reads return 0 — wrong gas
// (SSTORE_SET vs dirty-update, +19900) and a wrong written value
// (EEST cancun/eip6780_selfdestruct/* under EXEC3_PARALLEL). The calc now
// gets per-slot DELETEs from normalizeWriteSet's SD cascade instead.
func TestSelfDestructKeepsDirtyStorageReadableSameTx(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xAA})
	slot0 := accounts.InternKey([32]byte{0x00})
	slot1 := accounts.InternKey([32]byte{0x01})

	vm := NewVersionMap(nil)

	ibs := New(&emptyReader{})
	ibs.SetVersionMap(vm)
	ibs.SetTxContext(100, 0)
	ibs.SetVersion(0)

	ibs.CreateAccount(addr, true)
	require.NoError(t, ibs.SetState(addr, slot0, *uint256.NewInt(42)))
	require.NoError(t, ibs.SetState(addr, slot1, *uint256.NewInt(99)))

	_, err := ibs.Selfdestruct(addr)
	require.NoError(t, err)

	// After SELFDESTRUCT, same-tx reads must still see the dirty values.
	got0, err := ibs.GetState(addr, slot0)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), got0.Uint64(), "slot0 must read back as 42, not 0, after SELFDESTRUCT")
	got1, err := ibs.GetState(addr, slot1)
	require.NoError(t, err)
	assert.Equal(t, uint64(99), got1.Uint64(), "slot1 must read back as 99, not 0, after SELFDESTRUCT")

	// SelfDestructPath is still recorded.
	destructed, err := ibs.HasSelfdestructed(addr)
	require.NoError(t, err)
	assert.True(t, destructed)

	// And it must NOT have emitted spurious StoragePath=0 writes.
	for _, w := range ibs.VersionedWrites(false) {
		if w.Address == addr && w.Path == StoragePath {
			v := w.Val.(uint256.Int)
			assert.False(t, v.IsZero(), "Selfdestruct must not emit StoragePath=0 for slot %x", w.Key.Value())
		}
	}
}

// TestReadTimeDependency_RelaxesOnUnchangedValue: a read-time version mismatch
// whose VALUE is unchanged (a lower tx re-stamped the same value at a new
// version — coinbase churn) must not abort with ErrDependency. The value-aware
// check treats it as a spurious version-only dependency; validation stays the
// backstop for real changes.
func TestReadTimeDependency_RelaxesOnUnchangedValue(t *testing.T) {
	vm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xf9, 0x7e, 0x18, 0x0c})
	vm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 5}, *uint256.NewInt(100), true)
	ibs := NewWithVersionMap(&emptyReader{}, vm)
	ibs.txIndex = 10
	ibs.versionedReads = ReadSet{}
	ibs.versionedReads.Set(VersionedRead{Address: addr, Path: BalancePath, Key: accounts.NilKey, Source: MapRead, Version: Version{TxIndex: 3}, Val: *uint256.NewInt(100)})
	assert.NotPanics(t, func() {
		v, _, _, err := versionedRead[uint256.Int](ibs, addr, BalancePath, accounts.NilKey, false, uint256.Int{}, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, *uint256.NewInt(100), v)
	}, "unchanged-value version churn must not abort")
}

// TestReadTimeDependency_RelaxesOnUnchangedRecord is the coinbase case: an
// AddressPath record read whose version churned but whose existence is unchanged
// (fields resolved separately from the BAL) must not abort.
func TestReadTimeDependency_RelaxesOnUnchangedRecord(t *testing.T) {
	vm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xf9, 0x7e, 0x18, 0x0c})
	vm.Write(addr, AddressPath, accounts.NilKey, Version{TxIndex: 5}, &accounts.Account{Balance: *uint256.NewInt(200), CodeHash: accounts.EmptyCodeHash}, true)
	ibs := NewWithVersionMap(&emptyReader{}, vm)
	ibs.txIndex = 10
	ibs.versionedReads = ReadSet{}
	ibs.versionedReads.Set(VersionedRead{Address: addr, Path: AddressPath, Key: accounts.NilKey, Source: MapRead, Version: Version{TxIndex: 3}, Val: &accounts.Account{Balance: *uint256.NewInt(100), CodeHash: accounts.EmptyCodeHash}})
	assert.NotPanics(t, func() {
		_, _, _, err := versionedRead[*accounts.Account](ibs, addr, AddressPath, accounts.NilKey, false, nil, nil, nil)
		require.NoError(t, err)
	}, "record read must not abort on version-only churn when existence is unchanged")
}

// TestReadTimeDependency_AbortsOnChangedValue guards the relaxation: a genuinely
// changed value must still abort (real data dependency).
func TestReadTimeDependency_AbortsOnChangedValue(t *testing.T) {
	vm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x0c, 0x0b})
	vm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 5}, *uint256.NewInt(200), true)
	ibs := NewWithVersionMap(&emptyReader{}, vm)
	ibs.txIndex = 10
	ibs.versionedReads = ReadSet{}
	ibs.versionedReads.Set(VersionedRead{Address: addr, Path: BalancePath, Key: accounts.NilKey, Source: MapRead, Version: Version{TxIndex: 3}, Val: *uint256.NewInt(100)})
	assert.Panics(t, func() {
		_, _, _, _ = versionedRead[uint256.Int](ibs, addr, BalancePath, accounts.NilKey, false, uint256.Int{}, nil, nil)
	}, "changed balance value must still abort (real dependency)")
}

// TestReadTimeDependency_RelaxationRecordsValue: a value-aware relaxation must
// record the value it returned (not leave Val nil). Otherwise a subsequent
// same-version read type-asserts a nil interface at the cached-read return and
// panics — surfacing as a caught speculative-defer re-execution.
func TestReadTimeDependency_RelaxationRecordsValue(t *testing.T) {
	vm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xab, 0xcd})
	vm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 5}, *uint256.NewInt(100), true)
	ibs := NewWithVersionMap(&emptyReader{}, vm)
	ibs.txIndex = 10
	ibs.versionedReads = ReadSet{}
	ibs.versionedReads.Set(VersionedRead{Address: addr, Path: BalancePath, Key: accounts.NilKey, Source: MapRead, Version: Version{TxIndex: 3}, Val: *uint256.NewInt(100)})
	_, _, _, err := versionedRead[uint256.Int](ibs, addr, BalancePath, accounts.NilKey, false, uint256.Int{}, nil, nil)
	require.NoError(t, err)
	rd, ok := ibs.versionedReads[addr][AccountKey{Path: BalancePath}]
	require.True(t, ok)
	require.NotNil(t, rd.Val, "relaxed read must record its value, not nil")
	assert.NotPanics(t, func() {
		_, _, _, _ = versionedRead[uint256.Int](ibs, addr, BalancePath, accounts.NilKey, false, uint256.Int{}, nil, nil)
	}, "subsequent same-version read of the relaxed record must not nil-panic")
}

// TestWriteTimeDependency_RelaxesOnUnchangedValue: a tx that WROTE a path and
// re-reads it must not abort when a version-only churn left the read value
// unchanged (a value-forwarder whose net balance is restored). Only a genuine
// value change is a real dependency (WR-DEP relaxation).
func TestWriteTimeDependency_RelaxesOnUnchangedValue(t *testing.T) {
	vm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x41, 0x95})
	vm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 5}, *uint256.NewInt(100), true)
	ibs := NewWithVersionMap(&emptyReader{}, vm)
	ibs.txIndex = 10
	ibs.versionedReads = ReadSet{}
	ibs.versionedReads.Set(VersionedRead{Address: addr, Path: BalancePath, Key: accounts.NilKey, Source: MapRead, Version: Version{TxIndex: 3}, Val: *uint256.NewInt(100)})
	ibs.versionedWrites = WriteSet{}
	ibs.versionedWrites.Set(VersionedWrite{Address: addr, Path: BalancePath, Key: accounts.NilKey, Version: Version{TxIndex: 10}, Val: *uint256.NewInt(50)})
	ibs.journal.dirties[addr] = 1 // versionedWrite is only consulted for a dirtied address
	assert.NotPanics(t, func() {
		v, src, _, err := versionedRead[uint256.Int](ibs, addr, BalancePath, accounts.NilKey, false, uint256.Int{}, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, WriteSetRead, src, "must return the tx's own write")
		assert.Equal(t, *uint256.NewInt(50), v)
	}, "write-time version churn with unchanged read value must not abort")
}

// TestWriteTimeDependency_AbortsOnChangedValue guards the WR-DEP relaxation: a
// genuine value change under a written path must still abort.
func TestWriteTimeDependency_AbortsOnChangedValue(t *testing.T) {
	vm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x41, 0x96})
	vm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 5}, *uint256.NewInt(999), true)
	ibs := NewWithVersionMap(&emptyReader{}, vm)
	ibs.txIndex = 10
	ibs.versionedReads = ReadSet{}
	ibs.versionedReads.Set(VersionedRead{Address: addr, Path: BalancePath, Key: accounts.NilKey, Source: MapRead, Version: Version{TxIndex: 3}, Val: *uint256.NewInt(100)})
	ibs.versionedWrites = WriteSet{}
	ibs.versionedWrites.Set(VersionedWrite{Address: addr, Path: BalancePath, Key: accounts.NilKey, Version: Version{TxIndex: 10}, Val: *uint256.NewInt(50)})
	ibs.journal.dirties[addr] = 1 // versionedWrite is only consulted for a dirtied address
	assert.Panics(t, func() {
		_, _, _, _ = versionedRead[uint256.Int](ibs, addr, BalancePath, accounts.NilKey, false, uint256.Int{}, nil, nil)
	}, "write-time genuine value change must still abort")
}

// TestGetStateObject_SelfDestructedButBALFunded_StaysAlive: the account.Empty()
// gate — a BAL-funded account (balance set at floor(txIndex)) must NOT be dropped
// by a stale SelfDestructPath flag. Without the gate the funded account is
// wrongly deleted.
func TestGetStateObject_SelfDestructedButBALFunded_StaysAlive(t *testing.T) {
	vm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x5d, 0x02})
	vm.Write(addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, true, true)
	vm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 3}, *uint256.NewInt(1000), true)
	dbAcc := &accounts.Account{CodeHash: accounts.EmptyCodeHash}
	ibs := NewWithVersionMap(&fixedReader{acc: dbAcc}, vm)
	ibs.txIndex = 5
	so, err := ibs.getStateObject(addr, true)
	require.NoError(t, err)
	require.NotNil(t, so, "a BAL-funded account must not be dropped by a stale SD flag")
	assert.Equal(t, *uint256.NewInt(1000), so.data.Balance)
}
