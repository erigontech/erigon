package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// fakeBaseReader stands in for the raw sd read at the bottom of the prevBlock
// chain: a fixed account/storage map, no versioning.
type fakeBaseReader struct {
	accts   map[accounts.Address]*accounts.Account
	storage map[accounts.Address]map[accounts.StorageKey]uint256.Int
}

func (f *fakeBaseReader) ReadAccountByPlainAddr(address common.Address) (*accounts.Account, error) {
	return f.ReadAccountData(accounts.InternAddress(address))
}

func (f *fakeBaseReader) HasAccount(address common.Address) (bool, error) {
	acc, err := f.ReadAccountByPlainAddr(address)
	return acc != nil, err
}

func (f *fakeBaseReader) ReadAccountData(a accounts.Address) (*accounts.Account, error) {
	return f.accts[a], nil
}

func (f *fakeBaseReader) ReadAccountDataForDebug(a accounts.Address) (*accounts.Account, error) {
	return f.accts[a], nil
}

func (f *fakeBaseReader) ReadAccountStorage(a accounts.Address, k accounts.StorageKey) (uint256.Int, bool, error) {
	if m, ok := f.storage[a]; ok {
		if v, ok := m[k]; ok {
			return v, true, nil
		}
	}
	return uint256.Int{}, false, nil
}

func (f *fakeBaseReader) HasStorage(a accounts.Address) (bool, error) {
	return len(f.storage[a]) > 0, nil
}
func (f *fakeBaseReader) ReadAccountCode(a accounts.Address) ([]byte, error)  { return nil, nil }
func (f *fakeBaseReader) ReadAccountCodeSize(a accounts.Address) (int, error) { return 0, nil }
func (f *fakeBaseReader) ReadAccountIncarnation(a accounts.Address) (uint64, error) {
	return 0, nil
}
func (f *fakeBaseReader) SetTrace(bool, string) {}
func (f *fakeBaseReader) Trace() bool           { return false }
func (f *fakeBaseReader) TracePrefix() string   { return "" }

func acctBal(v uint64) *accounts.Account {
	a := accounts.NewAccount()
	a.Balance.SetUint64(v)
	return &a
}

func mapWithBalance(addr accounts.Address, v uint64) *VersionMap {
	m := NewVersionMap(nil)
	m.WriteBalance(addr, Version{TxIndex: 0}, *uint256.NewInt(v), true)
	m.SealUpTo(0)
	return m
}

func TestLayerVersionMaps_NewestLayerWins(t *testing.T) {
	t.Parallel()
	addr := getAddress(1)
	base := &fakeBaseReader{accts: map[accounts.Address]*accounts.Account{addr: acctBal(10)}}
	older := mapWithBalance(addr, 20)
	newer := mapWithBalance(addr, 30)

	r := layerVersionMaps(base, []*VersionMap{older, newer}) // oldest→newest
	acc, err := r.ReadAccountData(addr)
	require.NoError(t, err)
	require.NotNil(t, acc)
	assert.Equal(t, uint64(30), acc.Balance.Uint64(), "newest prevBlock layer wins")
}

func TestLayerVersionMaps_MissFallsThroughToBase(t *testing.T) {
	t.Parallel()
	addr := getAddress(2)
	other := getAddress(3)
	base := &fakeBaseReader{accts: map[accounts.Address]*accounts.Account{addr: acctBal(10)}}
	prevBlock := mapWithBalance(other, 99) // writes a different account

	r := layerVersionMaps(base, []*VersionMap{prevBlock})
	acc, err := r.ReadAccountData(addr)
	require.NoError(t, err)
	require.NotNil(t, acc)
	assert.Equal(t, uint64(10), acc.Balance.Uint64(), "no prevBlock entry → base value")
}

func TestLayerVersionMaps_EmptyIsBase(t *testing.T) {
	t.Parallel()
	addr := getAddress(4)
	base := &fakeBaseReader{accts: map[accounts.Address]*accounts.Account{addr: acctBal(7)}}
	r := layerVersionMaps(base, nil)
	acc, err := r.ReadAccountData(addr)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), acc.Balance.Uint64())
}

// preservedSelfdestructMap builds the map a block leaves behind when a
// balance-preserving SELFDESTRUCT (EIP-8246) keeps a non-zero balance: the
// account is net-absent (destroyed, not revived above the destruct) yet its
// balance cell survives non-deleted. The balance write shares the destruct tx,
// so it is not a strictly-higher revival and the account stays LifecycleAbsent.
func preservedSelfdestructMap(addr accounts.Address, bal uint64) *VersionMap {
	const sdTx = 5
	m := NewVersionMap(nil)
	m.WriteSelfDestruct(addr, Version{TxIndex: sdTx}, true, true)
	m.WriteBalance(addr, Version{TxIndex: sdTx}, *uint256.NewInt(bal), true)
	m.SealUpTo(sdTx)
	return m
}

func TestLayerVersionMaps_ReconstructsPreservedSelfdestruct(t *testing.T) {
	t.Parallel()
	addr := getAddress(6)
	base := &fakeBaseReader{} // the preserved account lives only in the prev block's map
	prevBlock := preservedSelfdestructMap(addr, 500)

	r := layerVersionMaps(base, []*VersionMap{prevBlock})
	acc, err := r.ReadAccountData(addr)
	require.NoError(t, err)
	require.NotNil(t, acc, "prev-block layer must reconstruct the balance-only EOA itself")
	assert.Equal(t, uint64(500), acc.Balance.Uint64())
}

// TestPrevBlockLayer_NextBlockSeesPreservedBalance is the two-block regression:
// block N preserves a self-destructed account's balance (EIP-8246) and seals,
// then block N+1 — still in the uncommitted pipeline window — reads it through
// the prev-block layer. The next block must see the account existing with the
// preserved balance, not absent.
func TestPrevBlockLayer_NextBlockSeesPreservedBalance(t *testing.T) {
	t.Parallel()
	addr := getAddress(7)
	prevBlock := preservedSelfdestructMap(addr, 500)
	base := layerVersionMaps(&fakeBaseReader{}, []*VersionMap{prevBlock})

	ibs := NewWithVersionMap(base, NewVersionMap(nil))
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 0)

	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, uint64(500), bal.Uint64(), "next block must see the preserved balance")
	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	assert.True(t, exists, "preserved account must exist for the next block")
}

func TestLayerVersionMaps_Storage(t *testing.T) {
	t.Parallel()
	addr := getAddress(5)
	key := accounts.InternKey(common.HexToHash("0x01"))
	base := &fakeBaseReader{storage: map[accounts.Address]map[accounts.StorageKey]uint256.Int{
		addr: {key: *uint256.NewInt(100)},
	}}
	prevBlock := NewVersionMap(nil)
	prevBlock.WriteStorage(addr, key, Version{TxIndex: 0}, *uint256.NewInt(200), true)
	prevBlock.SealUpTo(0)

	r := layerVersionMaps(base, []*VersionMap{prevBlock})
	v, ok, err := r.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(200), v.Uint64(), "prevBlock storage wins over base")
}
