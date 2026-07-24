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
