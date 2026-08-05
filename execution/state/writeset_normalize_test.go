package state

import (
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// errAccountReader fails ReadAccountData, to verify Normalize surfaces a
// state-read failure rather than swallowing it into a partial write set.
type errAccountReader struct{ minimalStateReader }

func (r *errAccountReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	return nil, errors.New("boom: state read failed")
}

// TestNormalize_PropagatesStateReadError pins that a ReadAccountData failure
// during account-field completion is returned, not discarded. A swallowed error
// yields a seemingly-valid partial write set (e.g. missing fields prevent the
// EIP-161 empty-account delete), which would corrupt the trie root.
func TestNormalize_PropagatesStateReadError(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x57"))
	kVM := accounts.InternKey(common.HexToHash("0x01"))
	vm := NewVersionMap(nil)
	vm.WriteStorage(addr, kVM, Version{TxIndex: 0}, *uint256.NewInt(7), true)
	// Storage-only dirty account: its account fields are all missing, forcing
	// the stateReader fallback that must propagate the read error.
	ws := &WriteSet{}
	ws.SetStorage(addr, kVM, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: kVM, Version: Version{TxIndex: 0}},
		Val:         *uint256.NewInt(7),
	})
	_, err := ws.Normalize(vm, 0, 0, &errAccountReader{}, nil, false /*emptyRemoval*/, false /*isAura*/, false /*eip8246*/)
	require.Error(t, err, "a stateReader ReadAccountData failure must be returned, not swallowed")
}

// Direct unit coverage for WriteSet.Normalize — the single commit oracle shared
// by the parallel executor and the block generator. These pin the edge cases
// that a generate-then-import differential check cannot (both sides run this
// same method), per review discussion.

// The incarnation arg is the validated-incarnation filter: writes whose
// Version.Incarnation != incarnation are dropped. This is exactly the arg that
// differs between block generation (sequential, incarnation 0) and parallel
// import (the OCC result incarnation) — so it must be pinned.
func TestNormalize_IncarnationFilter(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0xC0DE"))
	vm := NewVersionMap(nil)
	build := func() *WriteSet {
		ws := &WriteSet{}
		ws.SetCreateContract(addr, &VersionedWrite[bool]{
			WriteHeader: WriteHeader{Address: addr, Path: CreateContractPath, Version: Version{TxIndex: 0, Incarnation: 1}},
			Val:         true,
		})
		return ws
	}
	// Normalized at incarnation 0: the incarnation-1 write is filtered out.
	out0, _ := build().Normalize(vm, 0, 0, &minimalStateReader{}, nil, false, false, false)
	_, ok0 := out0.GetCreateContract(addr)
	require.False(t, ok0, "write from a non-matching incarnation must be dropped")
	// Normalized at incarnation 1: kept.
	out1, _ := build().Normalize(vm, 0, 1, &minimalStateReader{}, nil, false, false, false)
	_, ok1 := out1.GetCreateContract(addr)
	require.True(t, ok1, "write from the matching incarnation must be kept")
}

// On self-destruct, Normalize must re-emit a StoragePath delete for every slot
// the account holds — the union of slots written this batch (versionMap) and
// slots committed before the batch (domainStorageKeys) — and drop the account's
// own field writes so applyVersionedWrites reaches the pure-delete branch.
func TestNormalize_SelfDestructDeletesVmAndDomainStorageSlots(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x5D"))
	kVM := accounts.InternKey(common.HexToHash("0x01"))     // written this batch
	kDomain := accounts.InternKey(common.HexToHash("0x02")) // pre-block, in domain only
	vm := NewVersionMap(nil)
	vm.WriteStorage(addr, kVM, Version{TxIndex: 0}, *uint256.NewInt(9), true)
	domainKeys := func(a accounts.Address) []accounts.StorageKey {
		if a == addr {
			return []accounts.StorageKey{kDomain}
		}
		return nil
	}

	ws := &WriteSet{}
	ws.SetSelfDestruct(addr, &VersionedWrite[bool]{
		WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: Version{TxIndex: 1}},
		Val:         true,
	})
	ws.SetBalance(addr, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 1}},
		Val:         *uint256.NewInt(0),
	})

	out, _ := ws.Normalize(vm, 1, 0, &minimalStateReader{}, domainKeys, false /*emptyRemoval*/, false /*isAura*/, false /*eip8246*/)

	_, sdOK := out.GetSelfDestruct(addr)
	require.True(t, sdOK, "self-destruct must be retained")
	_, vmSlotOK := out.GetStorage(addr, kVM)
	require.True(t, vmSlotOK, "batch (versionMap) storage slot must be DELETE'd on SD")
	_, domainSlotOK := out.GetStorage(addr, kDomain)
	require.True(t, domainSlotOK, "pre-block (domain) storage slot must be DELETE'd on SD")
	_, balOK := out.GetBalance(addr)
	require.False(t, balOK, "pre-8246 self-destruct drops the account's balance write")
}

// EIP-8246 (no-burn SELFDESTRUCT) keeps the post-SD balance so the account can
// be preserved as balance-only rather than fully deleted; the pre-8246 path
// drops it. Same SD, only the eip8246 flag differs.
func TestNormalize_SelfDestructBalanceRetention_EIP8246(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x82"))
	vm := NewVersionMap(nil)
	build := func() *WriteSet {
		ws := &WriteSet{}
		ws.SetSelfDestruct(addr, &VersionedWrite[bool]{
			WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: Version{TxIndex: 1}},
			Val:         true,
		})
		ws.SetBalance(addr, &VersionedWrite[uint256.Int]{
			WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 1}},
			Val:         *uint256.NewInt(5),
		})
		return ws
	}
	pre, _ := build().Normalize(vm, 1, 0, &minimalStateReader{}, nil, false, false, false /*eip8246*/)
	_, preBal := pre.GetBalance(addr)
	require.False(t, preBal, "pre-8246 SD drops the balance write")

	post, _ := build().Normalize(vm, 1, 0, &minimalStateReader{}, nil, false, false, true /*eip8246*/)
	_, postBal := post.GetBalance(addr)
	require.True(t, postBal, "EIP-8246 SD retains the balance write")
}

// A self-destructed address must keep none of its account-field or raw storage
// writes: any survivor makes Apply see a non-empty account and take the
// cleanup-before-recreate branch instead of the pure delete, leaving a phantom
// account whose incarnation breaks a later CREATE2 at the same address. Only
// SelfDestructPath (and, under EIP-8246, the balance) may remain.
func TestNormalize_SelfDestructDropsAccountFieldAndStorageWrites(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x5DEAD"))
	kRaw := accounts.InternKey(common.HexToHash("0x03"))
	code := accounts.NewCode([]byte{0x60, 0x00})
	ver := Version{TxIndex: 1}

	ws := &WriteSet{}
	ws.SetSelfDestruct(addr, &VersionedWrite[bool]{
		WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: ver},
		Val:         true,
	})
	ws.SetNonce(addr, &VersionedWrite[uint64]{
		WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: ver},
		Val:         7,
	})
	ws.SetIncarnation(addr, &VersionedWrite[uint64]{
		WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath, Version: ver},
		Val:         3,
	})
	ws.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{
		WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath, Version: ver},
		Val:         code.Hash,
	})
	ws.SetCode(addr, &VersionedWrite[accounts.Code]{
		WriteHeader: WriteHeader{Address: addr, Path: CodePath, Version: ver},
		Val:         code,
	})
	// Not present in the versionMap or the domain, so the SD storage cascade
	// cannot re-emit it — if it shows up, the raw write survived the SD filter.
	ws.SetStorage(addr, kRaw, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: kRaw, Version: ver},
		Val:         *uint256.NewInt(42),
	})

	out, err := ws.Normalize(NewVersionMap(nil), 1, 0, &minimalStateReader{}, nil, false /*emptyRemoval*/, false /*isAura*/, false /*eip8246*/)
	require.NoError(t, err)

	_, sdOK := out.GetSelfDestruct(addr)
	require.True(t, sdOK, "the self-destruct itself must survive")
	_, nonceOK := out.GetNonce(addr)
	require.False(t, nonceOK, "nonce write of a self-destructed address must be dropped")
	_, incOK := out.GetIncarnation(addr)
	require.False(t, incOK, "incarnation write of a self-destructed address must be dropped")
	_, codeHashOK := out.GetCodeHash(addr)
	require.False(t, codeHashOK, "codeHash write of a self-destructed address must be dropped")
	_, codeOK := out.GetCode(addr)
	require.False(t, codeOK, "code write of a self-destructed address must be dropped")
	_, storageOK := out.GetStorage(addr, kRaw)
	require.False(t, storageOK, "raw storage write of a self-destructed address must be dropped")
}

// The account-field writes of a self-destructed address are what make Apply
// compute pureDelete=false and take the cleanup-before-recreate branch, leaving
// a phantom account. Normalize drops them; this asserts the guarantee at the
// consumer so a filter regression surfaces on the block that triggers it rather
// than as a wrong trie root later.
func TestAssertSelfDestructNormalized(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0xA55E27"))
	ver := Version{TxIndex: 1}
	sd := func(ws *WriteSet) *WriteSet {
		ws.SetSelfDestruct(addr, &VersionedWrite[bool]{
			WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: ver},
			Val:         true,
		})
		return ws
	}

	require.Panics(t, func() {
		ws := sd(&WriteSet{})
		ws.SetNonce(addr, &VersionedWrite[uint64]{
			WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: ver},
			Val:         1,
		})
		ws.assertSelfDestructNormalized()
	}, "a nonce write on a self-destructed address must trip the assert")

	require.Panics(t, func() {
		ws := sd(&WriteSet{})
		ws.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{
			WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath, Version: ver},
			Val:         accounts.NewCode([]byte{0x00}).Hash,
		})
		ws.assertSelfDestructNormalized()
	}, "a codeHash write on a self-destructed address must trip the assert")

	require.Panics(t, func() {
		ws := sd(&WriteSet{})
		ws.SetIncarnation(addr, &VersionedWrite[uint64]{
			WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath, Version: ver},
			Val:         2,
		})
		ws.assertSelfDestructNormalized()
	}, "an incarnation write on a self-destructed address must trip the assert")

	// Balance (kept under EIP-8246) and the storage-delete cascade are the two
	// things a normalized self-destruct legitimately carries.
	require.NotPanics(t, func() {
		ws := sd(&WriteSet{})
		ws.SetBalance(addr, &VersionedWrite[uint256.Int]{
			WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: ver},
			Val:         *uint256.NewInt(9),
		})
		k := accounts.InternKey(common.HexToHash("0x01"))
		ws.SetStorage(addr, k, &VersionedWrite[uint256.Int]{
			WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: k, Version: ver},
		})
		ws.assertSelfDestructNormalized()
	}, "balance and storage deletes are legal on a self-destructed address")
}

// A slot written before an in-block SELFDESTRUCT and re-written to the same value
// by a tx after the revival: the destruct wiped the slot, so the re-write is a
// real change, not a no-op. The pre-destruct write is not a valid baseline for
// it, and the latest SelfDestruct entry is the revival, which says nothing about
// the destruct that did the wiping.
func TestNormalize_KeepsRewriteOfPreDestructValueAfterRevival(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x7f"))
	key := accounts.InternKey(common.HexToHash("0x32"))
	val := *uint256.NewInt(0x593d)

	vm := NewVersionMap(nil)
	vm.WriteStorage(addr, key, Version{TxIndex: 16}, val, true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 41}, true, true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 42}, false, true)

	ws := &WriteSet{}
	ws.SetStorage(addr, key, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: key, Version: Version{TxIndex: 43}},
		Val:         val,
	})

	out, err := ws.Normalize(vm, 43, 0, &minimalStateReader{}, nil, false, false, false)
	require.NoError(t, err)
	got, ok := out.GetStorage(addr, key)
	require.True(t, ok, "the destruct wiped the slot, so re-writing the pre-destruct value is a real change")
	require.True(t, got.Val.Eq(&val))
}

// The no-op filter must still drop a genuine no-op: same value, same slot, with
// the destruct sitting below the baseline write rather than above it.
func TestNormalize_DropsNoOpWhenDestructPrecedesBaselineWrite(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x7e"))
	key := accounts.InternKey(common.HexToHash("0x32"))
	val := *uint256.NewInt(0x593d)

	vm := NewVersionMap(nil)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 10}, true, true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 11}, false, true)
	vm.WriteStorage(addr, key, Version{TxIndex: 16}, val, true)

	ws := &WriteSet{}
	ws.SetStorage(addr, key, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: key, Version: Version{TxIndex: 43}},
		Val:         val,
	})

	out, err := ws.Normalize(vm, 43, 0, &minimalStateReader{}, nil, false, false, false)
	require.NoError(t, err)
	_, ok := out.GetStorage(addr, key)
	require.False(t, ok, "no destruct after the baseline write, so writing the same value is a no-op")
}

// A tx that writes a slot and then self-destructs leaves both cells at its own
// TxIndex, and the destruct wins: the slot is gone. A later revival re-writing
// that value is a real change, so the baseline scan has to include the origin
// write's own TxIndex, not start above it.
func TestNormalize_KeepsRewriteWhenDestructSharesTheOriginTxIndex(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x7d"))
	key := accounts.InternKey(common.HexToHash("0x32"))
	val := *uint256.NewInt(5)

	vm := NewVersionMap(nil)
	vm.WriteStorage(addr, key, Version{TxIndex: 10}, val, true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 10}, true, true)

	ws := &WriteSet{}
	ws.SetStorage(addr, key, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: key, Version: Version{TxIndex: 20}},
		Val:         val,
	})

	out, err := ws.Normalize(vm, 20, 0, &minimalStateReader{}, nil, false, false, false)
	require.NoError(t, err)
	_, ok := out.GetStorage(addr, key)
	require.True(t, ok, "the destruct at the same TxIndex wiped the slot, so the re-write is a real change")
}
