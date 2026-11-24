package state

import (
	"fmt"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// EIP-8246 removes the SELFDESTRUCT balance burn, leaving a destroyed account
// alive as a balance-only account. These tests cover that behavior inside
// IntraBlockState: reconstructing a preserved account on the versioned-read
// path, resetting the in-memory object at end-of-tx so the block assembler's
// shared IBS carries the preserved balance into a later tx, and keeping the
// re-created incarnation aligned across execution modes.

// A destroyed-preserved contract's deployed code hash and nonce must not reach
// a later tx. Extraction drops nonce/code/codeHash for self-destructed
// accounts, so nothing in the version map carries them: GetCodeHash serves
// EmptyCodeHash through the preserved-account reconstruction fallback.
func TestEIP8246_PreservedSD_ReadsAsEmptyCodeAccountInLaterTx(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246B"))
	preserved := *uint256.NewInt(1)
	vm := NewVersionMap(nil)
	reader := newAccountStateReader()
	tx0 := New(reader)
	defer tx0.Close()
	tx0.SetTxContext(0, 0)
	tx0.SetVersion(0)
	tx0.SetVersionMap(vm)
	require.NoError(t, tx0.CreateAccount(addr, true))
	require.NoError(t, tx0.SetBalance(addr, preserved, tracing.BalanceChangeUnspecified))
	require.NoError(t, tx0.SetCode(addr, []byte("deployed runtime code"), tracing.CodeChangeUnspecified))
	_, err := tx0.Selfdestruct(addr, true)
	require.NoError(t, err)
	writes, err := tx0.FinalizedWrites(&chain.Rules{IsAmsterdam: true})
	require.NoError(t, err)
	tx0.FlushWritesToVersionMap(writes)
	tx1 := New(reader)
	defer tx1.Close()
	tx1.SetTxContext(0, 1)
	tx1.SetVersion(0)
	tx1.SetVersionMap(vm)
	ch, err := tx1.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, accounts.EmptyCodeHash, ch, "EXTCODEHASH of a preserved account must read empty, not the pre-destruct deployed hash")
	nonce, err := tx1.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(0), nonce, "the preserved account's nonce reads as cleared")
	exists, err := tx1.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists, "the preserved account still exists")
}

func TestEIP8246_PreservedSD_WithEarlierAccountRecord(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246B1"))
	account := accounts.NewAccount()
	account.Balance = *uint256.NewInt(7)
	vm := NewVersionMap(nil)
	vm.WriteAddress(addr, Version{TxIndex: 0}, &account, true)
	vm.WriteBalance(addr, Version{TxIndex: 0}, account.Balance, true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true)
	vm.WriteBalance(addr, Version{TxIndex: 1}, account.Balance, true)

	ibs := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 2)

	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists)
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, account.Balance, bal)
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
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()

	ibs.SetTxContext(1, 0)
	_, err := ibs.Selfdestruct(addr, true /* preserveBalance */)
	require.NoError(t, err)
	ibs.EndTransaction()
	vm.FlushVersionedWrites(finalizedWrites(t, ibs, rules), true)
	ibs.ResetVersionedIO()

	ibs.SetTxContext(1, 1)
	require.NoError(t, ibs.CreateAccount(addr, true))
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(100), bal, "EIP-8246: preserved balance must carry into a later-tx CREATE2 on the assembler's shared IBS")
}

// On a versioned IBS that caches state objects, the balance written before an
// EIP-8246 SELFDESTRUCT must survive FinalizeTx.
func TestEIP8246_FinalizeTx_PreservesSameTxCredit(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246C1"))
	rules := &chain.Rules{IsAmsterdam: true}
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(newAccountStateReader(), vm)
	defer ibs.Close()

	ibs.SetTxContext(1, 0)
	require.NoError(t, ibs.CreateAccount(addr, true))
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(7), tracing.BalanceChangeTransfer))
	_, err := ibs.Selfdestruct(addr, true /* preserveBalance */)
	require.NoError(t, err)
	ibs.EndTransaction()
	vm.FlushVersionedWrites(finalizedWrites(t, ibs, rules), true)
	ibs.ResetVersionedIO()

	ibs.SetTxContext(1, 1)
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(7), bal)
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
	defer ibs.Close()
	ibs.SetTxContext(0, 2)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists, "EIP-8246: a preserved account funded by a later tx must still exist for a concurrent reader")
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(2), bal, "reader must see the latest funded balance of the preserved account")
}

// EIP-8246: the whole-account versionedStateReader — used by calcFees to read the
// coinbase / burnt fee recipient on the state-root composition path — must not drop
// a self-destruct-preserved balance. IsNetAbsent is fork-agnostic and reports a
// preserve as absent; a fork-aware reader must reconstruct the preserved value from
// the versionMap cells, or calcFees credits the tip onto 0 and burns the preserved
// balance (a parallel-only wrong-state-root divergence vs serial).
func TestEIP8246_VersionedStateReader_PreserveNotBurned(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246CB"))
	preserved := *uint256.NewInt(500)

	vm := NewVersionMap(nil)
	sdVer := Version{TxIndex: 5}
	vm.WriteSelfDestruct(addr, sdVer, true, true)
	vm.WriteBalance(addr, sdVer, preserved, true)
	vm.WriteIncarnation(addr, sdVer, 0, true)

	// The versionedStateReader (the calcFees path) must surface the preserve as a
	// balance-only EOA. Branch-agnostic: it keys on the balance's non-deleted stamp
	// (a burn would stamp it ValueDeleted), not a fork flag.
	vsr := NewVersionedStateReader(6, ReadSet{}, vm, nil)
	acc, err := vsr.ReadAccountData(addr)
	require.NoError(t, err)
	require.NotNil(t, acc, "a balance-preserving self-destruct must not read as absent (calcFees would burn the balance)")
	require.Equal(t, preserved, acc.Balance)
}

// The burn counterpart to TestEIP8246_VersionedStateReader_PreserveNotBurned, and
// the case Alexey's revert flagged: a non-preserving SELFDESTRUCT must read as
// absent even though it had a non-zero balance before destruct. The burn stamps
// its balance write ValueDeleted, so the whole-account reader keys on the stamp
// — not the cell value — and cannot mistake the burn for a preserve.
func TestEIP8246_VersionedStateReader_BurnReadsAbsent(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246CC"))

	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(newAccountStateReader(), vm)
	defer ibs.Close()
	ibs.SetTxContext(0, 5)
	require.NoError(t, ibs.CreateAccount(addr, true))
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(500), tracing.BalanceChangeTransfer))
	_, err := ibs.Selfdestruct(addr, false /* burn */)
	require.NoError(t, err)
	vm.FlushVersionedWrites(finalizedWrites(t, ibs, &chain.Rules{IsAmsterdam: true}), true)

	vsr := NewVersionedStateReader(6, ReadSet{}, vm, nil)
	acc, err := vsr.ReadAccountData(addr)
	require.NoError(t, err)
	require.Nil(t, acc, "a burn stamps its balance ValueDeleted; the account must read absent, not as a preserved EOA")
}

// The persisted balance-only record has incarnation 0, so a later CREATE2 over
// a preserved account must compute incarnation 1 in every execution mode.
// Selfdestruct publishes the pre-destruct IncarnationPath and extraction keeps
// it even for self-destructed accounts; without a superseding write the
// parallel worker's version map serves the stale value and the recreated
// contract's domain record diverges from serial and from the next block.
func TestEIP8246_CreateAfterPreservedSD_IncarnationAndBalanceAcrossModes(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246E1"))
	preserved := *uint256.NewInt(100)
	rules := &chain.Rules{IsAmsterdam: true}
	t.Run("worker-map-shaped", func(t *testing.T) {
		t.Parallel()
		reader := newAccountStateReader()
		vm := NewVersionMap(nil)
		tx0 := New(reader)
		defer tx0.Close()
		tx0.SetTxContext(0, 0)
		tx0.SetVersion(0)
		tx0.SetVersionMap(vm)
		require.NoError(t, tx0.CreateAccount(addr, true))
		require.NoError(t, tx0.SetBalance(addr, preserved, tracing.BalanceChangeUnspecified))
		_, err := tx0.Selfdestruct(addr, true)
		require.NoError(t, err)
		writes, err := tx0.FinalizedWrites(rules)
		require.NoError(t, err)
		tx0.FlushWritesToVersionMap(writes)
		tx1 := New(reader)
		defer tx1.Close()
		tx1.SetTxContext(0, 1)
		tx1.SetVersion(0)
		tx1.SetVersionMap(vm)
		bal, err := tx1.GetBalance(addr)
		require.NoError(t, err)
		require.Equal(t, preserved, bal, "concurrent reader must see the preserved balance before re-creation")
		require.NoError(t, tx1.CreateAccount(addr, true))
		inc, err := tx1.GetIncarnation(addr)
		require.NoError(t, err)
		require.Equal(t, uint64(1), inc, "map-shaped re-creation must match serial and next-block incarnation")
		balAfter, err := tx1.GetBalance(addr)
		require.NoError(t, err)
		require.Equal(t, preserved, balAfter, "re-creation must carry the preserved balance")
	})
	t.Run("assembler-shared-ibs", func(t *testing.T) {
		t.Parallel()
		reader := newAccountStateReader()
		vm := NewVersionMap(nil)
		ibs := New(reader)
		defer ibs.Close()
		ibs.SetTxContext(1, 0)
		ibs.SetVersion(0)
		ibs.SetVersionMap(vm)
		require.NoError(t, ibs.CreateAccount(addr, true))
		require.NoError(t, ibs.SetBalance(addr, preserved, tracing.BalanceChangeUnspecified))
		_, err := ibs.Selfdestruct(addr, true)
		require.NoError(t, err)
		writes, err := ibs.FinalizedWrites(rules)
		require.NoError(t, err)
		ibs.FlushWritesToVersionMap(writes)
		ibs.EndTransaction()
		ibs.ResetVersionedIO()
		ibs.SetTxContext(1, 1)
		require.NoError(t, ibs.CreateAccount(addr, true))
		inc, err := ibs.GetIncarnation(addr)
		require.NoError(t, err)
		require.Equal(t, uint64(1), inc, "assembler re-creation incarnation")
		bal, err := ibs.GetBalance(addr)
		require.NoError(t, err)
		require.Equal(t, preserved, bal, "assembler re-creation must carry the preserved balance")
	})
}

// Account-level reconstruction of a preserved account must agree with the
// field-level reads: a later tx's Nonce/CodeHash map entries overlay the
// reconstructed account exactly like a later balance write does, so the
// account record sees the same fields a per-path read returns.
func TestEIP8246_PreservedAccount_OverlaysLaterFieldWrites(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246F"))
	laterCodeHash := accounts.InternCodeHash(common.HexToHash("0x31537ad3f3619e1f93aac0ddfdb0d8a0013bd170b427d81dd5abbee4f3f5248e"))
	reader := newAccountStateReader()
	vm := NewVersionMap(nil)
	sdVer := Version{TxIndex: 0}
	vm.WriteSelfDestruct(addr, sdVer, true, true)
	vm.WriteBalance(addr, sdVer, *uint256.NewInt(1), true)
	vm.WriteIncarnation(addr, sdVer, 0, true)
	laterVer := Version{TxIndex: 1}
	vm.WriteNonce(addr, laterVer, 7, true)
	vm.WriteCodeHash(addr, laterVer, laterCodeHash, true)
	ibs := New(reader)
	defer ibs.Close()
	ibs.SetTxContext(0, 2)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)
	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists, "the preserved account must exist")
	fieldNonce, err := ibs.GetNonce(addr)
	require.NoError(t, err)
	fieldCodeHash, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(7), fieldNonce, "the later-tx nonce write must overlay")
	require.Equal(t, laterCodeHash, fieldCodeHash, "the later-tx code hash write must overlay")
	acc, _, _, err := ibs.getVersionedAccount(addr, false)
	require.NoError(t, err)
	require.NotNil(t, acc)
	require.Equal(t, uint64(7), acc.Nonce, "the account record must carry the later-tx nonce")
	require.Equal(t, laterCodeHash, acc.CodeHash, "the account record must carry the later-tx code hash")
}

// Under EIP-8246 a contract created at an absent address and destroyed in one tx leaves only its
// kept balance, so the tx publishes for the address what a plain transfer of that balance publishes.
func TestEIP8246_CreateDestructPublishesAPlainBalance(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0xc0de"))
	slot := accounts.InternKey(common.Hash{0x01})
	rules := &chain.Rules{IsSpuriousDragon: true, IsCancun: true, IsAmsterdam: true}
	for _, tc := range []struct {
		name           string
		removedEarlier bool
		kept           uint64
	}{
		{"absent, nothing kept", false, 0},
		{"absent, balance kept", false, 5},
		{"removed earlier in the block, nothing kept", true, 0},
		{"removed earlier in the block, balance kept", true, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			publish := func(run func(ibs *IntraBlockState)) ([]string, types.AccountChanges) {
				var reader StateReader = &emptyReader{}
				vm := NewVersionMap(nil)
				if tc.removedEarlier {
					acc := accounts.NewAccount()
					reader = &sdAccountReader{addr: addr, account: &acc}
					removal := &WriteSet{}
					removal.SetEmptyRemoval(addr, Version{TxIndex: 1})
					vm.FlushVersionedWrites(removal, true)
				}
				ibs := NewWithVersionMap(reader, vm)
				defer ibs.Close()
				ibs.SetTxContext(1, 2)
				run(ibs)
				ws, err := ibs.FinalizedWrites(rules)
				require.NoError(t, err)
				io := NewVersionedIO(4)
				ibs.MergeTxIOInto(io, ws)
				var changes types.AccountChanges
				for _, ac := range io.AsBlockAccessList() {
					if ac.Address == addr.Value() {
						changes = ac
					}
				}
				var out []string
				for h := range ws.AllHeaders() {
					if h.Address != addr {
						continue
					}
					w := fmt.Sprintf("%s %x deleted=%v", h.Path, h.Key.Value(), h.Deletes())
					if b, ok := ws.GetBalance(addr); ok && h.Path == BalancePath {
						w += " " + b.Val.String()
					}
					if a, ok := ws.GetAddress(addr); ok && h.Path == AddressPath && a.Val != nil {
						w += fmt.Sprintf(" %+v", *a.Val)
					}
					out = append(out, w)
				}
				slices.Sort(out)
				return out, changes
			}
			plain, _ := publish(func(ibs *IntraBlockState) {
				require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(tc.kept), tracing.BalanceChangeUnspecified))
			})
			createDestruct, changes := publish(func(ibs *IntraBlockState) {
				require.NoError(t, ibs.CreateAccount(addr, true))
				require.NoError(t, ibs.SetNonce(addr, 1, tracing.NonceChangeUnspecified))
				require.NoError(t, ibs.SetCode(addr, []byte{0x60, 0x00}, tracing.CodeChangeUnspecified))
				require.NoError(t, ibs.SetState(addr, slot, *uint256.NewInt(7)))
				destroyed, err := ibs.Selfdestruct(addr, true)
				require.NoError(t, err)
				require.True(t, destroyed)
				if tc.kept > 0 {
					require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(tc.kept), tracing.BalanceChangeUnspecified))
				}
			})
			require.Equal(t, plain, createDestruct)
			require.Equal(t, []common.Hash{slot.Value()}, changes.StorageReads, "the dead slot stays in the access list as a read")
			require.Empty(t, changes.StorageChanges)
		})
	}
}

// CREATE may target an existing account that holds storage. Under EIP-8246 its create and destroy
// must still wipe that storage when the tx's writes are applied.
func TestEIP8246_CreateDestructWipesTheStorageOfAnExistingAccount(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	addr := accounts.InternAddress(common.HexToAddress("0x5dead"))
	av := addr.Value()
	pre := accounts.NewAccount()
	pre.Incarnation = 1
	pre.Balance.SetUint64(10)
	slot := common.HexToHash("0x02")
	storageKey := slices.Concat(av[:], slot[:])
	domains.SetTxNum(1)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, av[:], accounts.SerialiseV3(&pre), 1, nil))
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, storageKey, []byte{0x42}, 1, nil))

	rules := &chain.Rules{IsSpuriousDragon: true, IsCancun: true, IsAmsterdam: true}
	ibs := NewWithVersionMap(NewReaderV3(domains.AsGetter(tx)), NewVersionMap(nil))
	defer ibs.Close()
	ibs.SetTxContext(1, 0)
	require.NoError(t, ibs.CreateAccount(addr, true))
	require.NoError(t, ibs.SetNonce(addr, 1, tracing.NonceChangeUnspecified))
	require.NoError(t, ibs.SubBalance(addr, *uint256.NewInt(10), tracing.BalanceDecreaseSelfdestruct))
	_, err := ibs.Selfdestruct(addr, true)
	require.NoError(t, err)
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(5), tracing.BalanceChangeUnspecified))
	ws, err := ibs.FinalizedWrites(rules)
	require.NoError(t, err)

	domains.SetTxNum(2)
	require.NoError(t, ApplyWrites(ws, domains, tx, 1, 2, false))
	enc, _, err := domains.GetLatest(kv.AccountsDomain, tx, av[:])
	require.NoError(t, err)
	var acc accounts.Account
	require.NoError(t, accounts.DeserialiseV3(&acc, enc))
	require.Equal(t, uint64(5), acc.Balance.Uint64())
	require.Zero(t, acc.Nonce)
	require.Zero(t, acc.Incarnation)
	require.Equal(t, accounts.EmptyCodeHash, acc.CodeHash)
	v, _, err := domains.GetLatest(kv.StorageDomain, tx, storageKey)
	require.NoError(t, err)
	require.Empty(t, v)
}

// Under EIP-8246 a contract that a tx creates over an existing account and destroys keeps the balance.
// The tx publishes it as a creation that leaves the balance: the creation replaces the old storage,
// so no destruct reaches a later tx, and the access list is the same. An account that ends empty is
// still removed.
func TestEIP8246_CreateDestructOverAnExistingAccountPublishesACreation(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0xc0de"))
	slot, old := accounts.InternKey(common.Hash{0x01}), accounts.InternKey(common.Hash{0x02})
	rules := &chain.Rules{IsSpuriousDragon: true, IsCancun: true, IsAmsterdam: true}
	for _, tc := range []struct {
		name     string
		balance  uint64
		destruct bool
	}{
		{"balance kept", 10, false},
		{"empty at the end", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pre := accounts.NewAccount()
			pre.Balance = *uint256.NewInt(tc.balance)
			pre.Incarnation = FirstContractIncarnation
			reader := &storageReader{addr: addr, account: &pre, storage: map[accounts.StorageKey]uint256.Int{old: *uint256.NewInt(0x42)}}
			vm := NewVersionMap(nil)
			ibs := NewWithVersionMap(reader, vm)
			defer ibs.Close()
			ibs.SetTxContext(1, 2)
			require.NoError(t, ibs.CreateAccount(addr, true))
			require.NoError(t, ibs.SetNonce(addr, 1, tracing.NonceChangeUnspecified))
			require.NoError(t, ibs.SetCode(addr, []byte{0x60, 0x00}, tracing.CodeChangeUnspecified))
			require.NoError(t, ibs.SetState(addr, slot, *uint256.NewInt(7)))
			destroyed, err := ibs.Selfdestruct(addr, true)
			require.NoError(t, err)
			require.True(t, destroyed)
			ws, err := ibs.FinalizedWrites(rules)
			require.NoError(t, err)

			sd, ok := ws.GetSelfDestruct(addr)
			require.Equal(t, tc.destruct, ok && sd.Val)
			io := NewVersionedIO(4)
			ibs.MergeTxIOInto(io, ws)
			found := false
			for _, ac := range io.AsBlockAccessList() {
				if ac.Address == addr.Value() {
					found = true
					require.Equal(t, []common.Hash{slot.Value()}, ac.StorageReads, "the dead slot stays in the access list as a read")
					require.Empty(t, ac.StorageChanges)
					require.Empty(t, ac.NonceChanges)
					require.Empty(t, ac.CodeChanges)
				}
			}
			require.True(t, found)
			if tc.destruct {
				return
			}
			cc, ok := ws.GetCreateContract(addr)
			require.True(t, ok && cc.Val, "the creation replaces the old storage")
			dead, ok := ws.GetStorage(addr, slot)
			require.True(t, ok && dead.Val.IsZero(), "the dead slot's zero write stays for readers of the state changes")

			vm.FlushVersionedWrites(ws, true)
			later := NewWithVersionMap(reader, vm)
			defer later.Close()
			later.SetTxContext(1, 3)
			exists, err := later.Exist(addr)
			require.NoError(t, err)
			require.True(t, exists)
			balance, err := later.GetBalance(addr)
			require.NoError(t, err)
			require.Equal(t, *uint256.NewInt(tc.balance), balance)
			value, err := later.GetState(addr, old)
			require.NoError(t, err)
			require.True(t, value.IsZero())
			require.Equal(t, VersionValid, validateReads(vm, later, 3))
		})
	}
}
