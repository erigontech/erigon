package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func addrN(b byte) accounts.Address {
	return accounts.InternAddress([20]byte{0xd0, b})
}

func emptyRecord() *accounts.Account {
	return &accounts.Account{CodeHash: accounts.EmptyCodeHash}
}

func fundedRecord(v uint64) *accounts.Account {
	return &accounts.Account{Balance: *uint256.NewInt(v), CodeHash: accounts.EmptyCodeHash}
}

// A created account that nets EIP-161-empty leaves a phantom record in the
// versionMap that spuriously invalidates readers which correctly saw it as
// non-existent; every write for it must be dropped from the flush.
func TestDropCreatedNonExistentWrites_RemovesEmptyCreated(t *testing.T) {
	empty := addrN(1)
	funded := addrN(2)
	ver := state.Version{TxIndex: 3}
	ws := newWS().
		addr(empty, ver, emptyRecord()).
		codeHash(empty, ver, accounts.EmptyCodeHash).
		bal(empty, ver, uint256.Int{}).
		addr(funded, ver, fundedRecord(100)).
		codeHash(funded, ver, accounts.EmptyCodeHash).
		bal(funded, ver, *uint256.NewInt(100)).
		build()
	out := dropCreatedNonExistentWrites(ws, true)
	_, hasEmptyAddr := out.GetAddress(empty)
	_, hasEmptyBal := out.GetBalance(empty)
	_, hasEmptyCH := out.GetCodeHash(empty)
	assert.False(t, hasEmptyAddr, "empty created account record must be dropped")
	assert.False(t, hasEmptyBal)
	assert.False(t, hasEmptyCH)
	_, hasFundedAddr := out.GetAddress(funded)
	_, hasFundedBal := out.GetBalance(funded)
	assert.True(t, hasFundedAddr, "funded account must keep its writes")
	assert.True(t, hasFundedBal)
}

// Under EIP-6780 (any BAL block) a final SelfDestruct=true means
// created-and-destroyed in the same tx: net non-existent, DB-absent. Readers
// must fall through to the DB instead of racing the SD signal cell.
func TestDropCreatedNonExistentWrites_DropsDestroyed(t *testing.T) {
	destroyed := addrN(3)
	ver := state.Version{TxIndex: 5}
	key := accounts.InternKey([32]byte{0x11})
	ws := newWS().
		selfDestruct(destroyed, ver, true).
		bal(destroyed, ver, uint256.Int{}).
		inc(destroyed, ver, 1).
		stor(destroyed, key, ver, *uint256.NewInt(7)).
		build()
	out := dropCreatedNonExistentWrites(ws, true)
	assert.Zero(t, out.Count(), "all writes of a destroyed (created+SD) account must be dropped")
}

// A same-tx destroy-then-recreate ends with SelfDestruct=false: the account
// survives, so its writes must be kept.
func TestDropCreatedNonExistentWrites_KeepsResurrected(t *testing.T) {
	resurrected := addrN(4)
	ver := state.Version{TxIndex: 2}
	ws := newWS().
		selfDestruct(resurrected, ver, false).
		addr(resurrected, ver, fundedRecord(50)).
		bal(resurrected, ver, *uint256.NewInt(50)).
		build()
	out := dropCreatedNonExistentWrites(ws, true)
	_, hasAddr := out.GetAddress(resurrected)
	_, hasBal := out.GetBalance(resurrected)
	_, hasSD := out.GetSelfDestruct(resurrected)
	assert.True(t, hasAddr)
	assert.True(t, hasBal)
	assert.True(t, hasSD)
}

// A created contract starts at nonce 1 (EIP-161) — never empty, always kept.
func TestDropCreatedNonExistentWrites_KeepsCreatedContract(t *testing.T) {
	contract := addrN(5)
	ver := state.Version{TxIndex: 1}
	rec := &accounts.Account{Nonce: 1, CodeHash: accounts.EmptyCodeHash, Incarnation: 1}
	ws := newWS().
		addr(contract, ver, rec).
		nonce(contract, ver, 1).
		codeHash(contract, ver, accounts.EmptyCodeHash).
		inc(contract, ver, 1).
		build()
	out := dropCreatedNonExistentWrites(ws, true)
	_, hasAddr := out.GetAddress(contract)
	assert.True(t, hasAddr, "nonce-1 contract is not EIP-161-empty")
}

// An otherwise-empty created account with storage writes is kept (conservative).
func TestDropCreatedNonExistentWrites_KeepsStorageWriter(t *testing.T) {
	a := addrN(6)
	ver := state.Version{TxIndex: 1}
	key := accounts.InternKey([32]byte{0x22})
	ws := newWS().
		addr(a, ver, emptyRecord()).
		stor(a, key, ver, *uint256.NewInt(9)).
		build()
	out := dropCreatedNonExistentWrites(ws, true)
	_, hasAddr := out.GetAddress(a)
	assert.True(t, hasAddr)
}

// Without a BAL the no-BAL Block-STM semantics must stay byte-identical:
// the input set is returned unchanged.
func TestDropCreatedNonExistentWrites_NoBALPassthrough(t *testing.T) {
	a := addrN(7)
	ver := state.Version{TxIndex: 1}
	ws := newWS().
		addr(a, ver, emptyRecord()).
		bal(a, ver, uint256.Int{}).
		selfDestruct(addrN(8), ver, true).
		build()
	out := dropCreatedNonExistentWrites(ws, false)
	assert.Same(t, ws, out, "no-BAL path must be a passthrough")
}
