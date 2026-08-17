package state

import (
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// errAccountReader fails ReadAccountData so Normalize's error propagation can be tested.
type errAccountReader struct{ minimalStateReader }

func (r *errAccountReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	return nil, errors.New("boom: state read failed")
}

// A swallowed ReadAccountData error would silently yield a partial write set
// (e.g. skipping the EIP-161 delete).
func TestNormalize_PropagatesStateReadError(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x57"))
	kVM := accounts.InternKey(common.HexToHash("0x01"))
	vm := NewVersionMap(nil)
	vm.WriteStorage(addr, kVM, Version{TxIndex: 0}, *uint256.NewInt(7), true)
	ws := &WriteSet{}
	ws.SetStorage(addr, kVM, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: kVM, Version: Version{TxIndex: 0}},
		Val:         *uint256.NewInt(7),
	})
	_, err := ws.Normalize(vm, 0, 0, &errAccountReader{}, nil, false, false, false)
	require.Error(t, err, "a stateReader ReadAccountData failure must be returned, not swallowed")
}

// incarnation differs between sequential generation (0) and parallel import
// (the OCC result), so the validated-incarnation filter must be pinned here.
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
	out0, _ := build().Normalize(vm, 0, 0, &minimalStateReader{}, nil, false, false, false)
	_, ok0 := out0.GetCreateContract(addr)
	require.False(t, ok0, "write from a non-matching incarnation must be dropped")
	out1, _ := build().Normalize(vm, 0, 1, &minimalStateReader{}, nil, false, false, false)
	_, ok1 := out1.GetCreateContract(addr)
	require.True(t, ok1, "write from the matching incarnation must be kept")
}

func TestNormalize_SelfDestructDeletesVmAndDomainStorageSlots(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x5D"))
	kVM := accounts.InternKey(common.HexToHash("0x01"))
	kDomain := accounts.InternKey(common.HexToHash("0x02"))
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

	out, _ := ws.Normalize(vm, 1, 0, &minimalStateReader{}, domainKeys, false, false, false)

	_, sdOK := out.GetSelfDestruct(addr)
	require.True(t, sdOK, "self-destruct must be retained")
	_, vmSlotOK := out.GetStorage(addr, kVM)
	require.True(t, vmSlotOK, "batch (versionMap) storage slot must be DELETE'd on SD")
	_, domainSlotOK := out.GetStorage(addr, kDomain)
	require.True(t, domainSlotOK, "pre-block (domain) storage slot must be DELETE'd on SD")
	_, balOK := out.GetBalance(addr)
	require.False(t, balOK, "pre-8246 self-destruct drops the account's balance write")
}

// EIP-8246 (no-burn SELFDESTRUCT) keeps the post-SD balance instead of dropping it.
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
	pre, _ := build().Normalize(vm, 1, 0, &minimalStateReader{}, nil, false, false, false)
	_, preBal := pre.GetBalance(addr)
	require.False(t, preBal, "pre-8246 SD drops the balance write")

	post, _ := build().Normalize(vm, 1, 0, &minimalStateReader{}, nil, false, false, true)
	_, postBal := post.GetBalance(addr)
	require.True(t, postBal, "EIP-8246 SD retains the balance write")
}

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
	// kRaw is absent from vm/domain, isolating the SD filter from the delete cascade.
	ws.SetStorage(addr, kRaw, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: kRaw, Version: ver},
		Val:         *uint256.NewInt(42),
	})

	out, err := ws.Normalize(NewVersionMap(nil), 1, 0, &minimalStateReader{}, nil, false, false, false)
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
