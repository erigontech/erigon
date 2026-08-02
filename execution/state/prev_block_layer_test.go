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
