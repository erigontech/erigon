package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// EIP-8246 removes the SELFDESTRUCT balance burn, leaving a destroyed account
// alive as a balance-only account. These tests cover that behavior inside
// IntraBlockState: recording the cleared code/nonce so concurrent readers see
// them, reconstructing a preserved account on the versioned-read path, and
// resetting the in-memory object at end-of-tx so the block assembler's shared
// IBS carries the preserved balance into a later tx.

// A balance-preserving SELFDESTRUCT must record the cleared CodeHash and
// Nonce into the tx's versioned writes, otherwise the version map keeps the
// CREATE's deployed codehash and a concurrent tx's EXTCODEHASH reads the
// destroyed contract's hash instead of empty.
func TestEIP8246_Selfdestruct_RecordsClearedCodeHashAndNonce(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246B"))
	realCodeHash := accounts.InternCodeHash(common.HexToHash("0x31537ad3f3619e1f93aac0ddfdb0d8a0013bd170b427d81dd5abbee4f3f5248e"))
	reader := newAccountStateReader(addr)
	reader.accounts[addr].Nonce = 7
	reader.accounts[addr].CodeHash = realCodeHash
	reader.accounts[addr].Balance = *uint256.NewInt(1)

	ibs := New(reader)
	ibs.SetTxContext(0, 3)
	ibs.SetVersion(0)
	ibs.SetVersionMap(NewVersionMap(nil))
	ibs.eip8246 = true

	_, err := ibs.Selfdestruct(addr, true /* preserveBalance */)
	require.NoError(t, err)
	require.NoError(t, ibs.MakeWriteSet(&chain.Rules{IsAmsterdam: true}, NewNoopWriter()))

	chVW, ok := ibs.versionedWrites.GetCodeHash(addr)
	require.True(t, ok, "EIP-8246 SELFDESTRUCT must record a CodeHash write so concurrent readers stop seeing the deployed hash")
	require.Equal(t, accounts.EmptyCodeHash, chVW.Val, "EIP-8246 SD clears code → CodeHashPath must be EmptyCodeHash")

	nVW, ok := ibs.versionedWrites.GetNonce(addr)
	require.True(t, ok, "EIP-8246 SELFDESTRUCT must record a Nonce write")
	require.Equal(t, uint64(0), nVW.Val, "EIP-8246 SD clears nonce → NoncePath must be 0")
}

// The block assembler runs every tx on one shared IBS (no per-tx Reset).
// After a balance-preserving SELFDESTRUCT is finalized, a later tx's CREATE2 at
// the same address must carry the preserved balance — i.e. FinalizeTx must not
// leave a stale selfdestructed marker on the in-memory object.
func TestEIP8246_FinalizeTx_PreservedBalanceCarriesToLaterTxCreate(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246C"))
	realCodeHash := accounts.InternCodeHash(common.HexToHash("0x31537ad3f3619e1f93aac0ddfdb0d8a0013bd170b427d81dd5abbee4f3f5248e"))
	reader := newAccountStateReader(addr)
	reader.accounts[addr].Nonce = 3
	reader.accounts[addr].CodeHash = realCodeHash
	reader.accounts[addr].Balance = *uint256.NewInt(100)

	rules := &chain.Rules{IsAmsterdam: true}
	ibs := New(reader)

	ibs.SetTxContext(1, 0)
	_, err := ibs.Selfdestruct(addr, true /* preserveBalance */)
	require.NoError(t, err)
	require.NoError(t, ibs.FinalizeTx(rules, NewNoopWriter()))

	ibs.SetTxContext(1, 1)
	require.NoError(t, ibs.CreateAccount(addr, true))
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(100), bal, "EIP-8246: preserved balance must carry into a later-tx CREATE2 on the assembler's shared IBS")
}

// getVersionedAccount must not treat a later Balance/Nonce/CodeHash write
// as a revival. An in-block-created account that self-destructs (preserving
// balance) and is then funded by a later tx must still be reconstructed — with
// the later funding overlaid — so a concurrent reader sees it exist, matching
// serial. Only a genuine re-creation (a later AddressPath) skips reconstruction.
func TestEIP8246_VersionedAccount_LaterBalanceWriteDoesNotSkipReconstruction(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246D"))
	reader := newAccountStateReader() // in-block-created: no pre-block record

	vm := NewVersionMap(nil)
	sdVer := Version{TxIndex: 0}
	vm.WriteSelfDestruct(addr, sdVer, true, true)
	vm.WriteBalance(addr, sdVer, *uint256.NewInt(1), true)
	vm.WriteIncarnation(addr, sdVer, 1, true)
	// A later tx funds the preserved account — a balance write only, no re-creation.
	vm.WriteBalance(addr, Version{TxIndex: 1}, *uint256.NewInt(2), true)

	ibs := New(reader)
	ibs.SetTxContext(0, 2)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)
	ibs.eip8246 = true

	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists, "EIP-8246: a preserved account funded by a later tx must still exist for a concurrent reader")
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(2), bal, "reader must see the latest funded balance of the preserved account")
}
