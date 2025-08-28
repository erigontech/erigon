// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package trie

// Experimental code for separating data and structural information

import (
	"encoding/binary"
	"fmt"
	"sort"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestHashWithModificationsEmpty(t *testing.T) {
	tr := New(common.Hash{})
	// Populate the trie
	// Build the root
	var stream Stream
	hb := NewHashBuilder(false)
	rootHash, err := HashWithModifications(
		tr,
		common.Hashes{}, []*accounts.Account{}, [][]byte{},
		common.StorageKeys{}, [][]byte{},
		32,
		&stream, // Streams that will be reused for old and new stream
		hb,      // HashBuilder will be reused
		false,
	)
	if err != nil {
		t.Errorf("Could not compute hash with modification: %v", err)
	}
	if rootHash != EmptyRoot {
		t.Errorf("Expected empty root, got: %x", rootHash)
	}
}

func TestHashWithModificationsNoChanges(t *testing.T) {
	tr := New(common.Hash{})
	// Populate the trie
	var preimage [4]byte
	var keys []string
	for b := uint32(0); b < 10; b++ {
		binary.BigEndian.PutUint32(preimage[:], b)
		key := crypto.Keccak256(preimage[:])
		keys = append(keys, string(key))
	}
	sort.Strings(keys)
	for i, key := range keys {
		if i > 0 && keys[i-1] == key {
			fmt.Printf("Duplicate!\n")
		}
	}
	var a0, a1 accounts.Account
	a0.Balance.SetUint64(100000)
	a0.Root = EmptyRoot
	a0.CodeHash = emptyState
	a0.Initialised = true
	a1.Balance.SetUint64(200000)
	a1.Root = EmptyRoot
	a1.CodeHash = emptyState
	a1.Initialised = true
	v := []byte("VALUE")
	for i, key := range keys {
		if i%2 == 0 {
			tr.UpdateAccount([]byte(key), &a0)
		} else {
			tr.UpdateAccount([]byte(key), &a1)
			// Add storage items too
			for _, storageKey := range keys {
				tr.Update([]byte(key+storageKey), v)
			}
		}
	}
	expectedHash := tr.Hash()
	// Build the root
	var stream Stream
	hb := NewHashBuilder(false)
	rootHash, err := HashWithModifications(
		tr,
		common.Hashes{}, []*accounts.Account{}, [][]byte{},
		common.StorageKeys{}, [][]byte{},
		40,
		&stream, // Streams that will be reused for old and new stream
		hb,      // HashBuilder will be reused
		false,
	)
	if err != nil {
		t.Errorf("Could not compute hash with modification: %v", err)
	}
	if rootHash != expectedHash {
		t.Errorf("Expected %x, got: %x", expectedHash, rootHash)
	}
}

func TestHashWithModificationsChanges(t *testing.T) {
	tr := New(common.Hash{})
	// Populate the trie
	var preimage [4]byte
	var keys []string
	for b := uint32(0); b < 10; b++ {
		binary.BigEndian.PutUint32(preimage[:], b)
		key := crypto.Keccak256(preimage[:])
		keys = append(keys, string(key))
	}
	sort.Strings(keys)
	for i, key := range keys {
		if i > 0 && keys[i-1] == key {
			fmt.Printf("Duplicate!\n")
		}
	}
	var a0, a1 accounts.Account
	a0.Balance.SetUint64(100000)
	a0.Root = EmptyRoot
	a0.CodeHash = emptyState
	a0.Initialised = true
	a1.Balance.SetUint64(200000)
	a1.Root = EmptyRoot
	a1.CodeHash = emptyState
	a1.Initialised = true
	v := []byte("VALUE")
	for i, key := range keys {
		if i%2 == 0 {
			tr.UpdateAccount([]byte(key), &a0)
		} else {
			tr.UpdateAccount([]byte(key), &a1)
			// Add storage items too
			for _, storageKey := range keys {
				tr.Update([]byte(key+storageKey), v)
			}
		}
	}
	tr.Hash()
	// Generate account change
	binary.BigEndian.PutUint32(preimage[:], 5000000)
	var insertKey common.Hash
	copy(insertKey[:], crypto.Keccak256(preimage[:]))
	var insertA accounts.Account
	insertA.Balance.SetUint64(300000)
	insertA.Root = EmptyRoot
	insertA.CodeHash = emptyState
	insertA.Initialised = true

	// Build the root
	var stream Stream
	hb := NewHashBuilder(false)
	rootHash, err := HashWithModifications(
		tr,
		common.Hashes{insertKey}, []*accounts.Account{&insertA}, [][]byte{nil},
		common.StorageKeys{}, [][]byte{},
		40,
		&stream, // Streams that will be reused for old and new stream
		hb,      // HashBuilder will be reused
		false,
	)
	if err != nil {
		t.Errorf("Could not compute hash with modification: %v", err)
	}
	tr.UpdateAccount(insertKey[:], &insertA)
	expectedHash := tr.Hash()
	if rootHash != expectedHash {
		t.Errorf("Expected %x, got: %x", expectedHash, rootHash)
	}
}
