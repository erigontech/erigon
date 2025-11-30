package state

import (
	"container/list"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
)

// MockStateReader implements StateReader interface for testing
type MockStateReader struct {
	accounts map[common.Address]*accounts.Account
}

func (m *MockStateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	if acc, ok := m.accounts[address]; ok {
		return acc, nil
	}
	return nil, nil
}

func (m *MockStateReader) ReadAccountDataForDebug(address common.Address) (*accounts.Account, error) {
	return m.ReadAccountData(address)
}

func (m *MockStateReader) ReadAccountStorage(address common.Address, key common.Hash) (uint256.Int, bool, error) {
	return uint256.Int{}, false, nil
}

func (m *MockStateReader) HasStorage(address common.Address) (bool, error) {
	return false, nil
}

func (m *MockStateReader) ReadAccountCode(address common.Address) ([]byte, error) {
	return nil, nil
}

func (m *MockStateReader) ReadAccountCodeSize(address common.Address) (int, error) {
	return 0, nil
}

func (m *MockStateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}

func (m *MockStateReader) SetTrace(trace bool, tracePrefix string) {}

func TestLRUCacheEviction(t *testing.T) {
	reader := &MockStateReader{
		accounts: make(map[common.Address]*accounts.Account),
	}

	// Create state DB
	sdb := New(reader)

	// 1. Fill cache up to limit
	// MaxStateObjectsCacheSize is 10000
	// We'll add 10001 items and expect the first one to be evicted if it's clean

	addresses := make([]common.Address, MaxStateObjectsCacheSize+1)
	for i := 0; i < MaxStateObjectsCacheSize+1; i++ {
		addresses[i] = common.BigToAddress(uint256.NewInt(uint64(i + 1)).ToBig())
		// Mock account in reader so getStateObject finds it
		reader.accounts[addresses[i]] = &accounts.Account{Nonce: uint64(i)}
	}

	// Load first 10000 objects
	for i := 0; i < MaxStateObjectsCacheSize; i++ {
		obj, err := sdb.getStateObject(addresses[i])
		assert.NoError(t, err)
		assert.NotNil(t, obj)
	}

	// Verify cache size
	assert.Equal(t, MaxStateObjectsCacheSize, sdb.lruList.Len())
	assert.Equal(t, MaxStateObjectsCacheSize, len(sdb.stateObjects))

	// Load the 10001th object - should trigger eviction of the 1st object (addresses[0])
	obj, err := sdb.getStateObject(addresses[MaxStateObjectsCacheSize])
	assert.NoError(t, err)
	assert.NotNil(t, obj)

	// Verify cache size remains at limit
	assert.Equal(t, MaxStateObjectsCacheSize, sdb.lruList.Len())
	assert.Equal(t, MaxStateObjectsCacheSize, len(sdb.stateObjects))

	// Verify addresses[0] is evicted
	_, exists := sdb.stateObjects[addresses[0]]
	assert.False(t, exists, "First object should have been evicted")

	// Verify addresses[1] is still there
	_, exists = sdb.stateObjects[addresses[1]]
	assert.True(t, exists, "Second object should still be in cache")

	// Verify addresses[MaxStateObjectsCacheSize] is in cache
	_, exists = sdb.stateObjects[addresses[MaxStateObjectsCacheSize]]
	assert.True(t, exists, "Newest object should be in cache")
}

func TestLRUCacheDirtyProtection(t *testing.T) {
	reader := &MockStateReader{
		accounts: make(map[common.Address]*accounts.Account),
	}
	sdb := New(reader)

	// Create addresses
	addr1 := common.BigToAddress(uint256.NewInt(1).ToBig())
	addr2 := common.BigToAddress(uint256.NewInt(2).ToBig())

	// Mock accounts
	reader.accounts[addr1] = &accounts.Account{Nonce: 1}
	reader.accounts[addr2] = &accounts.Account{Nonce: 2}

	// Load addr1
	_, err := sdb.getStateObject(addr1)
	assert.NoError(t, err)

	// Mark addr1 as dirty
	sdb.stateObjectsDirty[addr1] = struct{}{}

	// Simulate filling the cache to force eviction
	// We need to add enough items to push addr1 to the back and then add one more
	// Since we can't easily fill 10000 items in a quick test without loop, we'll assume
	// the logic holds. Instead, let's manually manipulate the LRU list for this test
	// to simulate addr1 being the LRU candidate.

	// Clear existing list and add addr1
	sdb.lruList = list.New()
	elem := sdb.lruList.PushFront(addr1)
	sdb.lruMap[addr1] = elem

	// Now pretend cache is full by setting a condition that would trigger eviction
	// But since we can't change the constant MaxStateObjectsCacheSize, we have to
	// actually fill it.

	// Fill // Fill up to limit-1 (since addr1 is already there)
	for i := 0; i < MaxStateObjectsCacheSize-1; i++ {
		addr := common.BigToAddress(uint256.NewInt(uint64(i + 100)).ToBig())
		reader.accounts[addr] = &accounts.Account{Nonce: uint64(i)}
		_, err := sdb.getStateObject(addr)
		assert.NoError(t, err)
	}

	// Now cache should be full (addr1 + 10000 items? No, wait.)
	// We added addr1 manually to list/map but didn't account for it in normal flow?
	// Actually, sdb.getStateObject adds to front.
	// Let's restart cleanly.

	sdb = New(reader)

	// 1. Load addr1
	_, err = sdb.getStateObject(addr1)
	assert.NoError(t, err)

	// 2. Mark addr1 dirty
	sdb.stateObjectsDirty[addr1] = struct{}{}

	// 3. Load 9999 other items (Max - 1) so that with addr1 we have exactly Max items
	for i := 0; i < MaxStateObjectsCacheSize-1; i++ {
		addr := common.BigToAddress(uint256.NewInt(uint64(i + 100)).ToBig())
		reader.accounts[addr] = &accounts.Account{Nonce: uint64(i)}
		_, err := sdb.getStateObject(addr)
		assert.NoError(t, err)
	}

	// Now addr1 should be at the back of the list
	assert.Equal(t, addr1, sdb.lruList.Back().Value.(common.Address))

	// 4. Load one more item. This should try to evict addr1.
	addrLast := common.BigToAddress(uint256.NewInt(999999).ToBig())
	reader.accounts[addrLast] = &accounts.Account{Nonce: 999}
	_, err = sdb.getStateObject(addrLast)
	assert.NoError(t, err)

	// 5. Verify addr1 is STILL in cache because it is dirty
	_, exists := sdb.stateObjects[addr1]
	assert.True(t, exists, "Dirty object should NOT be evicted")

	// Verify size allowed to exceed limit for dirty objects?
	// Our implementation:
	// if full: try evict back.
	// if back is dirty: do nothing (don't evict).
	// add new object.
	// So size should be Max + 1

	assert.Equal(t, MaxStateObjectsCacheSize+1, len(sdb.stateObjects))
}

func TestLRUHitRefresh(t *testing.T) {
	reader := &MockStateReader{
		accounts: make(map[common.Address]*accounts.Account),
	}
	sdb := New(reader)

	addr1 := common.BigToAddress(uint256.NewInt(1).ToBig())
	addr2 := common.BigToAddress(uint256.NewInt(2).ToBig())

	reader.accounts[addr1] = &accounts.Account{Nonce: 1}
	reader.accounts[addr2] = &accounts.Account{Nonce: 2}

	// Load addr1
	_, err := sdb.getStateObject(addr1)
	assert.NoError(t, err)

	// Load addr2 (addr2 at Front, addr1 at Back)
	_, err = sdb.getStateObject(addr2)
	assert.NoError(t, err)

	assert.Equal(t, addr1, sdb.lruList.Back().Value.(common.Address))
	assert.Equal(t, addr2, sdb.lruList.Front().Value.(common.Address))

	// Access addr1 again (Hit)
	_, err = sdb.getStateObject(addr1)
	assert.NoError(t, err)

	// addr1 should be at Front now
	assert.Equal(t, addr1, sdb.lruList.Front().Value.(common.Address))
	assert.Equal(t, addr2, sdb.lruList.Back().Value.(common.Address))
}
