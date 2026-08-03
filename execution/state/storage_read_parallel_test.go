package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

type storageReader struct {
	emptyReader
	addr    accounts.Address
	account *accounts.Account
	storage map[accounts.StorageKey]uint256.Int
}

func (r *storageReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if addr == r.addr && r.account != nil {
		a := &accounts.Account{}
		a.Copy(r.account)
		return a, nil
	}
	return nil, nil
}

func (r *storageReader) ReadAccountStorage(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	if addr == r.addr {
		if v, ok := r.storage[key]; ok {
			return v, true, nil
		}
	}
	return uint256.Int{}, false, nil
}

// TestStorageReadParallel_NoMaterialize verifies that a cold storage read on the
// parallel (versionMap) path returns the committed value without
// materializing/caching a stateObject.
func TestStorageReadParallel_NoMaterialize(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})
	key := accounts.InternKey([32]byte{0x01})
	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1

	reader := &storageReader{
		addr:    addr,
		account: &acc,
		storage: map[accounts.StorageKey]uint256.Int{key: *uint256.NewInt(42)},
	}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	v, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	assert.Equal(t, uint256.NewInt(42), &v, "cold SLOAD must return the committed value")
	assert.Empty(t, ibs.stateObjects, "cold parallel SLOAD must not materialize a stateObject")

	// A repeat read hits the recorded ReadSet and must return the same value.
	v2, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	assert.Equal(t, v, v2)
	assert.Empty(t, ibs.stateObjects, "repeat SLOAD must still not materialize a stateObject")
}

// TestStorageReadParallel_CommittedRead verifies GetCommittedState on the
// parallel path also avoids materialization.
func TestStorageReadParallel_CommittedRead(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})
	key := accounts.InternKey([32]byte{0x02})
	acc := accounts.NewAccount()
	acc.Nonce = 1

	reader := &storageReader{
		addr:    addr,
		account: &acc,
		storage: map[accounts.StorageKey]uint256.Int{key: *uint256.NewInt(7)},
	}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	v, err := ibs.GetCommittedState(addr, key)
	require.NoError(t, err)
	assert.Equal(t, uint256.NewInt(7), &v)
	assert.Empty(t, ibs.stateObjects, "cold parallel committed read must not materialize a stateObject")
}
