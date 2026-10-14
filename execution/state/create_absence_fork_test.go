package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestCreateOverAbsenceConsumedBeforeDestructFlush pins the OCC invariant that
// an execution which consumed an account's absence (e.g. for the EIP-8037
// new-account gas charge) must not silently adopt a destruct flushed since: it
// has to record a dependency or fail commit-time validation, else its gas view
// forks from its state view. Here the read-set inconsistency is caught by
// commit-time ValidateVersion, so the tx re-executes rather than publishing.
func TestCreateOverAbsenceConsumedBeforeDestructFlush(t *testing.T) {
	t.Parallel()
	for _, collisionCheck := range []bool{false, true} {
		name := "createOnly"
		if collisionCheck {
			name = "collisionCheckFirst"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			vm := NewVersionMap(nil)
			ibs := NewWithVersionMap(&emptyReader{}, vm)
			ibs.SetNoMaterialize(true)
			ibs.SetTxContext(0, 1)
			ibs.SetVersion(0)
			ibs.eip8246 = true

			addr := getAddress(8246)

			// tx1's gas probe consumes the account's absence before tx0 flushes.
			empty, err := ibs.Empty(addr)
			require.NoError(t, err)
			require.True(t, empty)

			// tx0's writes flush: CREATE2 whose constructor self-destructed to
			// itself. EIP-8246 preserves the balance; the worker flush carries no
			// AddressPath cell, so only the destruct probe can reveal the account.
			writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, *uint256.NewInt(1_000_000), true)
			writeFor(vm, addr, NoncePath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, uint64(1), true)
			writeFor(vm, addr, IncarnationPath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, uint64(0), true)
			writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, true, true)
			writeFor(vm, addr, CreateContractPath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, true, true)

			// The CREATE2 flow re-reads the account it is about to create over.
			// Our read model records a dependency (sdb.dep) rather than panicking, so
			// the consensus invariant is only that the fork is CAUGHT — HadInvalidRead
			// or a failing commit-time validation — not that intermediate reads stay
			// absent (a doomed run may still observe reconstructed values before it
			// re-executes).
			if collisionCheck {
				_, err := ibs.GetCodeHash(addr)
				require.NoError(t, err)
				_, err = ibs.GetNonce(addr)
				require.NoError(t, err)
			}
			require.NoError(t, ibs.CreateAccount(addr, true))

			diverged := ibs.HadInvalidRead()
			if !diverged {
				io := NewVersionedIO(2)
				io.RecordReads(Version{TxIndex: 1, Incarnation: 0}, ibs.VersionedReads())
				diverged = vm.ValidateVersion(1, io, validateEqualVersion, false, "") != VersionValid
			}
			require.True(t, diverged,
				"consuming the account's absence and then adopting the flushed destruct forks the gas view from the state view")
		})
	}
}
