package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type sdAccountReader struct {
	emptyReader
	addr    accounts.Address
	account *accounts.Account
}

func (r *sdAccountReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if addr == r.addr && r.account != nil {
		a := &accounts.Account{}
		a.Copy(r.account)
		return a, nil
	}
	return nil, nil
}

// TestSelfdestructParallel_NoMaterialize verifies that on the parallel
// (versionMap) path Selfdestruct records the self-destruct through
// versioned-write cells without materializing/caching a stateObject.
func TestSelfdestructParallel_NoMaterialize(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(100)
	acc.Nonce = 1
	acc.Incarnation = 1

	reader := &sdAccountReader{addr: addr, account: &acc}
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(reader, vm)
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	destroyed, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	assert.True(t, destroyed, "existing account should be destroyed")

	writes := ibs.VersionedWrites()
	sd, ok := writes.GetSelfDestruct(addr)
	require.True(t, ok, "SelfDestructPath write expected")
	assert.True(t, sd.Val, "self-destruct flag should be set")

	bal, ok := writes.GetBalance(addr)
	require.True(t, ok, "BalancePath write expected")
	assert.True(t, bal.Val.IsZero(), "balance should be cleared")

	assert.Empty(t, ibs.stateObjects, "parallel Selfdestruct must not materialize a stateObject")
}

// TestSelfdestructParallel_RepeatedSameTx verifies that a second SELFDESTRUCT of
// the same account within a tx still proceeds (returns true and re-clears a
// balance credited in between) — matching the serial object, whose deleted flag
// stays false until finalize.
func TestSelfdestructParallel_RepeatedSameTx(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(100)
	acc.Nonce = 1

	reader := &sdAccountReader{addr: addr, account: &acc}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	destroyed, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	require.True(t, destroyed)

	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(30), tracing.BalanceChangeUnspecified))

	destroyed, err = ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	assert.True(t, destroyed, "repeat same-tx SELFDESTRUCT should still report destroyed")

	writes := ibs.VersionedWrites()
	bal, ok := writes.GetBalance(addr)
	require.True(t, ok)
	assert.True(t, bal.Val.IsZero(), "balance credited between the two SELFDESTRUCTs must be re-cleared")
}

// A second SELFDESTRUCT with nothing credited in between changes no cell, so it
// leaves the journal as it was.
func TestSelfdestructParallel_RepeatWithNothingChangedJournalsNothing(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(100)

	reader := &sdAccountReader{addr: addr, account: &acc}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetTxContext(100, 5)

	_, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	entries := ibs.journal.length()

	destroyed, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	assert.True(t, destroyed)
	assert.Equal(t, entries, ibs.journal.length())
}

// TestSelfdestructParallel_AbsentAccount verifies that self-destructing an
// account that does not exist returns false and records nothing.
func TestSelfdestructParallel_AbsentAccount(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xBE, 0xEF})

	reader := &sdAccountReader{addr: addr, account: nil}
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(reader, vm)
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	destroyed, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	assert.False(t, destroyed, "absent account cannot be destroyed")
	assert.Empty(t, ibs.stateObjects, "absent-account Selfdestruct must not materialize a stateObject")
}

// An earlier transaction's SELFDESTRUCT leaves a deleted account, which a later
// transaction of the block has not self-destructed.
func TestHasSelfdestructed_EarlierTxDestruct(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(100)
	acc.Nonce = 1
	reader := &sdAccountReader{addr: addr, account: &acc}
	rules := &chain.Rules{IsSpuriousDragon: true}
	vm := NewVersionMap(nil)

	destroyer := NewWithVersionMap(reader, vm)
	destroyer.SetTxContext(1, 0)
	destroyed, err := destroyer.Selfdestruct(addr, false)
	require.NoError(t, err)
	require.True(t, destroyed)
	vm.FlushVersionedWrites(destroyer.FinalizedWrites(rules), true)

	ibs := NewWithVersionMap(reader, vm)
	ibs.SetTxContext(1, 1)
	// The first call records the destruct; the second is served from that record.
	for range 2 {
		destructed, err := ibs.HasSelfdestructed(addr)
		require.NoError(t, err)
		require.False(t, destructed)
	}
}

// HasSelfdestructed reports the transaction's own SELFDESTRUCT whatever earlier
// transactions of the block left in the version map, so before London a second
// SELFDESTRUCT of the contract in that transaction earns no refund.
func TestHasSelfdestructed_OwnDestructAfterEarlierTx(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(100)
	acc.Nonce = 1
	reader := &sdAccountReader{addr: addr, account: &acc}
	rules := &chain.Rules{IsSpuriousDragon: true}

	type step func(t *testing.T, ibs *IntraBlockState)
	destroy := func(t *testing.T, ibs *IntraBlockState) {
		destroyed, err := ibs.Selfdestruct(addr, false)
		require.NoError(t, err)
		require.True(t, destroyed)
	}
	credit := func(t *testing.T, ibs *IntraBlockState) {
		require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(3), tracing.BalanceChangeUnspecified))
	}
	// recreate follows the EVM's CREATE2: collision check, then the new contract.
	recreate := func(t *testing.T, ibs *IntraBlockState) {
		_, err := ibs.GetCodeHash(addr)
		require.NoError(t, err)
		_, err = ibs.GetNonce(addr)
		require.NoError(t, err)
		require.NoError(t, ibs.CreateAccount(addr, true))
		require.NoError(t, ibs.SetNonce(addr, 1, tracing.NonceChangeNewContract))
		require.NoError(t, ibs.SetCode(addr, []byte{0x33, 0xff}, tracing.CodeChangeContractCreation))
	}
	for _, tc := range []struct {
		name    string
		earlier []step // one per earlier transaction
		before  step   // this transaction, before its first SELFDESTRUCT
	}{
		{name: "no earlier write"},
		{name: "destroyed earlier, re-created in this tx", earlier: []step{destroy}, before: recreate},
		{name: "destroyed and re-created earlier", earlier: []step{destroy, recreate}},
		{name: "destroyed, credited and re-created earlier", earlier: []step{destroy, credit, recreate}},
		{name: "destroyed, credited and re-created in this tx", earlier: []step{destroy, credit}, before: recreate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			vm := NewVersionMap(nil)
			for i, run := range tc.earlier {
				ibs := NewWithVersionMap(reader, vm)
				ibs.SetTxContext(1, i)
				run(t, ibs)
				vm.FlushVersionedWrites(ibs.FinalizedWrites(rules), true)
			}

			ibs := NewWithVersionMap(reader, vm)
			ibs.SetTxContext(1, len(tc.earlier))
			if tc.before != nil {
				tc.before(t, ibs)
			}
			// The SELFDESTRUCT gas function asks before the opcode runs.
			hasSelfdestructed := func() bool {
				t.Helper()
				destructed, err := ibs.HasSelfdestructed(addr)
				require.NoError(t, err)
				return destructed
			}
			require.False(t, hasSelfdestructed(), "the first SELFDESTRUCT earns the refund")
			snapshot := ibs.PushSnapshot()
			destroy(t, ibs)
			ibs.RevertToSnapshot(snapshot, nil)
			require.False(t, hasSelfdestructed(), "a reverted SELFDESTRUCT leaves the refund to the next")
			destroy(t, ibs)
			require.True(t, hasSelfdestructed(), "a second SELFDESTRUCT in the tx earns no refund")
		})
	}
}
