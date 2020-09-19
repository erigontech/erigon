// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty off
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Pruning of the Merkle Patricia trees

package trie

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockAccountEvicter struct {
	keys [][]byte
}

func newMockAccountEvicter() *mockAccountEvicter {
	return &mockAccountEvicter{make([][]byte, 0)}
}

func (m *mockAccountEvicter) EvictNode(key []byte) {
	m.keys = append(m.keys, key)
}

func TestEvictionBasicOperations(t *testing.T) {
	eviction := NewEviction()
	eviction.SetBlockNumber(1)

	key := []byte{0x01, 0x01, 0x01, 0x01}
	hex := keybytesToHex(key)
	eviction.CodeNodeCreated(hex, 1024)

	assert.Equal(t, 1024, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 1, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 1024, int(eviction.generations.blockNumToGeneration[1].totalSize), "should register size of gen")

	// grow
	eviction.CodeNodeSizeChanged(hex, 2048)

	assert.Equal(t, 2048, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 1, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 2048, int(eviction.generations.blockNumToGeneration[1].totalSize), "should register size of gen")

	// shrink
	eviction.CodeNodeSizeChanged(hex, 100)

	assert.Equal(t, 100, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 1, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 100, int(eviction.generations.blockNumToGeneration[1].totalSize), "should register size of gen")

	// shrink
	eviction.CodeNodeDeleted(hex)

	assert.Equal(t, 0, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 1, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 0, int(eviction.generations.blockNumToGeneration[1].totalSize), "should register size of gen")
}

func TestEvictionPartialSingleGen(t *testing.T) {
	eviction := NewEviction()
	eviction.SetBlockNumber(1)

	// create 100kb or accounts
	for i := 0; i < 100; i++ {
		key := []byte{0x01, 0x01, 0x01, byte(i)}
		eviction.BranchNodeCreated(keybytesToHex(key))
	}

	assert.Equal(t, 100, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 1, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 0, int(eviction.generations.oldestBlockNum), "should register block num")
	assert.Equal(t, 100, int(eviction.generations.blockNumToGeneration[1].totalSize), "should register size of gen")

	mock := newMockAccountEvicter()

	eviction.EvictToFitSize(mock, 99)

	assert.Equal(t, 0, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 0, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 2, int(eviction.generations.oldestBlockNum), "should register block num")
	assert.Equal(t, 100, len(mock.keys), "should evict all 100 accounts")
}

func TestEvictionFullSingleGen(t *testing.T) {
	eviction := NewEviction()
	eviction.SetBlockNumber(1)

	// create 100kb or accounts
	for i := 0; i < 100; i++ {
		key := []byte{0x01, 0x01, 0x01, byte(i)}
		eviction.BranchNodeCreated(keybytesToHex(key))
	}

	assert.Equal(t, 100, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 1, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 0, int(eviction.generations.oldestBlockNum), "should register block num")
	assert.Equal(t, 100, int(eviction.generations.blockNumToGeneration[1].totalSize), "should register size of gen")

	mock := newMockAccountEvicter()

	eviction.EvictToFitSize(mock, 0)

	assert.Equal(t, 0, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 0, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 2, int(eviction.generations.oldestBlockNum), "should register block num")
	assert.Equal(t, 100, len(mock.keys), "should evict all 100 accounts")
}

func TestEvictionNoNeedSingleGen(t *testing.T) {
	eviction := NewEviction()
	eviction.SetBlockNumber(1)

	// create 100kb or accounts
	for i := 0; i < 100; i++ {
		key := []byte{0x01, 0x01, 0x01, byte(i)}
		eviction.BranchNodeCreated(keybytesToHex(key))
	}

	assert.Equal(t, 100, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 1, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 0, int(eviction.generations.oldestBlockNum), "should register block num")
	assert.Equal(t, 100, int(eviction.generations.blockNumToGeneration[1].totalSize), "should register size of gen")

	mock := newMockAccountEvicter()

	eviction.EvictToFitSize(mock, 100)

	assert.Equal(t, 100, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 1, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 0, int(eviction.generations.oldestBlockNum), "should register block num")
	assert.Equal(t, 100, int(eviction.generations.blockNumToGeneration[1].totalSize), "should register size of gen")

	assert.Equal(t, 0, len(mock.keys), "should evict all 100 accounts")
}

func TestEvictionNoNeedMultipleGen(t *testing.T) {
	eviction := NewEviction()
	eviction.SetBlockNumber(1)

	// create 10kb or accounts
	for i := 0; i < 10; i++ {
		key := []byte{0x01, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 1024)
	}

	eviction.SetBlockNumber(2)

	// create 10kb or accounts
	for i := 0; i < 10; i++ {
		key := []byte{0x02, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 1024)
	}

	eviction.SetBlockNumber(4)

	// create 10kb or accounts
	for i := 0; i < 10; i++ {
		key := []byte{0x03, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 1024)
	}

	eviction.SetBlockNumber(5)

	// create 10kb or accounts
	for i := 0; i < 10; i++ {
		key := []byte{0x04, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 1024)
	}

	eviction.SetBlockNumber(7)

	// create 10kb or accounts
	for i := 0; i < 10; i++ {
		key := []byte{0x05, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 1024)
	}

	// 50 kb total

	assert.Equal(t, 50*1024, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 5, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 0, int(eviction.generations.oldestBlockNum), "should register block num")
	for _, i := range []uint64{1, 2, 4, 5, 7} {
		assert.Equal(t, 10*1024, int(eviction.generations.blockNumToGeneration[i].totalSize), "should register size of gen")
	}

	mock := newMockAccountEvicter()

	eviction.EvictToFitSize(mock, 50*1024)

	assert.Equal(t, 50*1024, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 5, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 0, int(eviction.generations.oldestBlockNum), "should register block num")

	assert.Equal(t, 0, len(mock.keys), "should not evict anything")
}

func TestEvictionPartialMultipleGen(t *testing.T) {
	eviction := NewEviction()
	eviction.SetBlockNumber(1)

	// create 10kb or accounts
	for i := 0; i < 10; i++ {
		key := []byte{0x01, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 1024)
	}

	eviction.SetBlockNumber(2)

	// create 10kb or accounts
	for i := 0; i < 10; i++ {
		key := []byte{0x02, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 1024)
	}

	eviction.SetBlockNumber(4)

	// create 10kb or accounts
	for i := 0; i < 10; i++ {
		key := []byte{0x03, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 1024)
	}

	eviction.SetBlockNumber(5)

	// create 10kb or accounts
	for i := 0; i < 10; i++ {
		key := []byte{0x04, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 1024)
	}

	eviction.SetBlockNumber(7)

	// create 10kb or accounts
	for i := 0; i < 10; i++ {
		key := []byte{0x05, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 1024)
	}

	// 50 kb total

	assert.Equal(t, 50*1024, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 5, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 0, int(eviction.generations.oldestBlockNum), "should register block num")
	for _, i := range []uint64{1, 2, 4, 5, 7} {
		assert.Equal(t, 10*1024, int(eviction.generations.blockNumToGeneration[i].totalSize), "should register size of gen")
	}

	mock := newMockAccountEvicter()

	eviction.EvictToFitSize(mock, 20*1024)

	assert.Equal(t, 20*1024, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 2, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 5, int(eviction.generations.oldestBlockNum), "should register block num")
	for _, i := range []uint64{5, 7} {
		assert.Equal(t, 10*1024, int(eviction.generations.blockNumToGeneration[i].totalSize), "should register size of gen")
	}
	assert.Equal(t, 30, len(mock.keys), "should evict only 3 generations")
}

func TestEvictionFullMultipleGen(t *testing.T) {
	eviction := NewEviction()
	eviction.SetBlockNumber(1)

	// create 10kb or accounts
	for i := 0; i < 10; i++ {
		key := []byte{0x01, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 1024)
	}

	eviction.SetBlockNumber(2)

	// create 10kb or accounts
	for i := 0; i < 10; i++ {
		key := []byte{0x02, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 1024)
	}

	eviction.SetBlockNumber(4)

	// create 10kb or accounts
	for i := 0; i < 10; i++ {
		key := []byte{0x03, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 1024)
	}

	eviction.SetBlockNumber(5)

	// create 10kb or accounts
	for i := 0; i < 10; i++ {
		key := []byte{0x04, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 1024)
	}

	eviction.SetBlockNumber(7)

	// create 10kb or accounts
	for i := 0; i < 10; i++ {
		key := []byte{0x05, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 1024)
	}

	// 50 kb total

	assert.Equal(t, 50*1024, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 5, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 0, int(eviction.generations.oldestBlockNum), "should register block num")
	for _, i := range []uint64{1, 2, 4, 5, 7} {
		assert.Equal(t, 10*1024, int(eviction.generations.blockNumToGeneration[i].totalSize), "should register size of gen")
	}

	mock := newMockAccountEvicter()

	eviction.EvictToFitSize(mock, 0)

	assert.Equal(t, 0, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 0, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 8, int(eviction.generations.oldestBlockNum), "should register block num")

	assert.Equal(t, 50, len(mock.keys), "should evict only 3 generations")

}

func TestEvictionMoveBetweenGen(t *testing.T) {
	eviction := NewEviction()

	eviction.SetBlockNumber(2)

	for i := 0; i < 2; i++ {
		key := []byte{0x01, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 10*1024)
	}

	eviction.SetBlockNumber(4)

	// create 10kb or accounts
	for i := 0; i < 1; i++ {
		key := []byte{0x04, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 20*1024)
	}

	eviction.SetBlockNumber(5)

	// create 10kb or accounts
	for i := 0; i < 10; i++ {
		key := []byte{0x05, 0x01, 0x01, byte(i)}
		eviction.CodeNodeCreated(keybytesToHex(key), 1024)
	}

	assert.Equal(t, 50*1024, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 3, len(eviction.generations.blockNumToGeneration), "should register generation")

	eviction.CodeNodeTouched(keybytesToHex([]byte{0x01, 0x01, 0x01, 0x00}))

	assert.Equal(t, 50*1024, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 3, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 1, len(eviction.generations.blockNumToGeneration[2].keys()), "should move one acc")
	assert.Equal(t, 11, len(eviction.generations.blockNumToGeneration[5].keys()), "should move one acc to gen 5")
	assert.Equal(t, CodeKeyFromAddrHash(keybytesToHex([]byte{0x01, 0x01, 0x01, 0x01})), []byte(eviction.generations.blockNumToGeneration[2].keys()[0]), "should move one acc")

	// move the acc again to the new block!
	eviction.SetBlockNumber(10)
	eviction.CodeNodeTouched(keybytesToHex([]byte{0x01, 0x01, 0x01, 0x00}))

	assert.Equal(t, 50*1024, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 4, len(eviction.generations.blockNumToGeneration), "should register generation")
	assert.Equal(t, 10, len(eviction.generations.blockNumToGeneration[5].keys()), "should move one acc from gen 5, again 10")
	assert.Equal(t, CodeKeyFromAddrHash(keybytesToHex([]byte{0x01, 0x01, 0x01, 0x00})), []byte(eviction.generations.blockNumToGeneration[10].keys()[0]), "should move one acc")

	// move the last acc from the gen
	eviction.CodeNodeTouched(keybytesToHex([]byte{0x01, 0x01, 0x01, 0x01}))

	assert.Equal(t, 50*1024, int(eviction.TotalSize()), "should register all accounts")
	assert.Equal(t, 3, len(eviction.generations.blockNumToGeneration), "should register generation")
	_, found := eviction.generations.blockNumToGeneration[2]
	assert.False(t, found, "2nd generation is empty and should be removed")

	assert.Equal(t, 2, len(eviction.generations.blockNumToGeneration[10].keys()), "should move one acc")
}
