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

package shards

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/c2h5oh/datasize"
	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestCacheBtreeOrderAccountStorage2(t *testing.T) {
	t.Parallel()
	sc := NewStateCache(32, datasize.ByteSize(128*accountItemSize))
	var a1 common.Address
	a1[0] = 1
	sc.SetAccountRead(a1[:], &accounts.Account{})
	sc.SetAccountWrite(a1[:], &accounts.Account{Nonce: 2})
	x, ok := sc.GetAccount(a1[:])
	fmt.Printf("%+v,%t\n", x.Nonce, ok)

}

func TestCacheBtreeOrderAccountStorage(t *testing.T) {
	t.Parallel()
	sc := NewStateCache(32, datasize.ByteSize(128*accountItemSize))
	var a1, a2 common.Address
	a1[0] = 1
	a2[0] = 2
	sc.SetAccountWrite(a2[:], &accounts.Account{})
	sc.SetAccountWrite(a1[:], &accounts.Account{})
	lastK := make([]byte, 0, 128)
	curK := make([]byte, 0, 128)
	if err := sc.WalkAccounts([]byte{}, func(addrHash common.Hash, account *accounts.Account) (bool, error) {
		curK = append(curK[:0], addrHash[:]...)
		assert.Negative(t, bytes.Compare(lastK, curK))
		lastK = append(lastK[:0], curK...)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	var l1, l2, l3 common.Hash
	l1[0] = 42
	l2[0] = 2
	l3[0] = 3
	sc.SetStorageWrite(a1[:], l1[:], nil)
	sc.SetStorageWrite(a1[:], l2[:], nil)
	sc.SetStorageWrite(a2[:], l3[:], nil)
	lastK = lastK[:0]
	if err := sc.WalkStorage(common.BytesToHash(keccak.NewFastKeccak().Sum(a1[:])), nil, func(locHash common.Hash, val []byte) error {
		curK = append(curK[:0], locHash[:]...)
		assert.Negative(t, bytes.Compare(lastK, curK))
		lastK = append(lastK[:0], curK...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sc.SetCodeWrite(a1[:], []byte{1})
	sc.SetCodeWrite(a2[:], []byte{2})
	lastK = lastK[:0]
}

func TestAccountReads(t *testing.T) {
	t.Parallel()
	sc := NewStateCache(32, datasize.ByteSize(4*accountItemSize))
	var account1 accounts.Account
	account1.Balance.SetUint64(1)
	var addr1 common.Address
	addr1[0] = 1
	sc.SetAccountRead(addr1[:], &account1)
	if _, ok := sc.GetAccount(addr1[:]); !ok {
		t.Fatalf("Expected to find account with addr1")
	}
	var addr2 common.Address
	addr2[0] = 2
	if _, ok := sc.GetAccount(addr2[:]); ok {
		t.Fatalf("Did not expect account with addr2")
	}
	var addr3 common.Address
	addr3[0] = 3
	sc.SetAccountAbsent(addr3[:])
	if a, ok := sc.GetAccount(addr3[:]); !ok || a != nil {
		t.Fatalf("Expected account with addr3 to be absent")
	}
	for i := 4; i <= 6; i++ {
		var account accounts.Account
		account.Balance.SetUint64(uint64(i))
		var addr common.Address
		addr[0] = byte(i)
		sc.SetAccountRead(addr[:], &account)
	}
	// Out of 6 addresses, one was not associated with an account or absence record. So 5 records would be in the cache
	// But since the limit is 4, the first addr will be evicted
	if _, ok := sc.GetAccount(addr1[:]); ok {
		t.Fatalf("Expected addr1 to be evicted")
	}
	for i := 4; i <= 6; i++ {
		var account accounts.Account
		account.Balance.SetUint64(uint64(i))
		var addr common.Address
		addr[0] = byte(i)
		if _, ok := sc.GetAccount(addr[:]); !ok {
			t.Fatalf("Expected to find account with addr %x", addr)
		}
	}
}

func TestAccountReadWrites(t *testing.T) {
	t.Parallel()
	sc := NewStateCache(32, datasize.ByteSize(9*accountItemSize))
	var account1 accounts.Account
	account1.Balance.SetUint64(1)
	var addr1 common.Address
	addr1[0] = 1
	sc.SetAccountWrite(addr1[:], &account1)
	if _, ok := sc.GetAccount(addr1[:]); !ok {
		t.Fatalf("Expected to find account with addr1")
	}
	if sc.WriteCount() != 1 {
		t.Fatalf("Write queue is expected to have 1 elements, got: %d", sc.WriteCount())
	}
	// Replace the existing value
	var account11 accounts.Account
	account11.Balance.SetUint64(11)
	sc.SetAccountWrite(addr1[:], &account11)
	if a, ok := sc.GetAccount(addr1[:]); !ok {
		t.Fatalf("Expected to find account with addr1")
	} else {
		if a.Balance.Uint64() != 11 {
			t.Fatalf("Expected account balance 11, got %d", a.Balance.Uint64())
		}
	}
	if sc.WriteCount() != 1 {
		t.Fatalf("Write queue is expected to have 1 elements, got: %d", sc.WriteCount())
	}
	// Add read and then replace it with the write
	var account2 accounts.Account
	account2.Balance.SetUint64(2)
	var addr2 common.Address
	addr2[0] = 2
	sc.SetAccountRead(addr2[:], &account2)
	// Check that readQueue is empty
	if sc.readQueuesLen() != 1 {
		t.Fatalf("Read queue is expected to be 1 element")
	}
	var account22 accounts.Account
	account22.Balance.SetUint64(22)
	sc.SetAccountWrite(addr2[:], &account22)
	if a, ok := sc.GetAccount(addr2[:]); !ok {
		t.Fatalf("Expected to find account with addr2")
	} else {
		if a.Balance.Uint64() != 22 {
			t.Fatalf("Expected account balance 22, got %d", a.Balance.Uint64())
		}
	}
	if sc.WriteCount() != 2 {
		t.Fatalf("Write queue is expected to have 2 elements, got: %d", sc.WriteCount())
	}
	// Check that readQueue is empty
	if sc.readQueuesLen() != 0 {
		t.Fatalf("Read queue is expected to be empty")
	}
	// Deleting written account
	var account3 accounts.Account
	account3.Balance.SetUint64(3)
	var addr3 common.Address
	addr3[0] = 3
	sc.SetAccountWrite(addr3[:], &account3)
	sc.SetAccountDelete(addr3[:])
	if a, ok := sc.GetAccount(addr3[:]); !ok || a != nil {
		t.Fatalf("Expected account addr3 to be deleted")
	}
	if sc.WriteCount() != 3 {
		t.Fatalf("Write queue is expected to have 3 elements, got: %d", sc.WriteCount())
	}
	// Deleting read account
	var account4 accounts.Account
	account4.Balance.SetUint64(4)
	var addr4 common.Address
	addr4[0] = 4
	sc.SetAccountRead(addr4[:], &account4)
	sc.SetAccountDelete(addr4[:])
	if a, ok := sc.GetAccount(addr4[:]); !ok || a != nil {
		t.Fatalf("Expected account addr4 to be deleted")
	}
	if sc.WriteCount() != 4 {
		t.Fatalf("Write queue is expected to have 4 elements, got: %d", sc.WriteCount())
	}
	// Check that readQueue is empty
	if sc.readQueuesLen() != 0 {
		t.Fatalf("Read queue is expected to be empty")
	}
	// Deleting account not seen before
	var addr5 common.Address
	addr5[0] = 5
	sc.SetAccountDelete(addr5[:])
	if a, ok := sc.GetAccount(addr5[:]); !ok || a != nil {
		t.Fatalf("Expected account addr5 to be deleted")
	}
	if sc.WriteCount() != 5 {
		t.Fatalf("Write queue is expected to have 5 elements, got: %d", sc.WriteCount())
	}
}

func TestReplaceAccountReadsWithWrites(t *testing.T) {
	t.Parallel()
	sc := NewStateCache(32, datasize.ByteSize(6*accountItemSize))
	for i := 1; i <= 4; i++ {
		var addr common.Address
		addr[0] = byte(i)
		var account accounts.Account
		account.Balance.SetUint64(uint64(i))
		sc.SetAccountWrite(addr[:], &account)
	}
	writes := sc.PrepareWrites()
	sc.TurnWritesToReads(writes)
	if sc.WriteCount() != 0 {
		t.Fatalf("Write queue is expected to be empty, got: %d", sc.WriteCount())
	}
	if sc.readQueuesLen() != 4 {
		t.Fatalf("Read queue is expected to have 4 elements, got: %d", sc.readQueuesLen())
	}
	// Do 4 more deletes
	for i := 5; i <= 8; i++ {
		var addr common.Address
		addr[0] = byte(i)
		var account accounts.Account
		account.Balance.SetUint64(uint64(i))
		sc.SetAccountWrite(addr[:], &account)
	}
	if sc.WriteCount() != 4 {
		t.Fatalf("Write queue is expected to have 4 elements, got: %d", sc.WriteCount())
	}
	if sc.readQueuesLen() != 2 {
		t.Fatalf("Read queue is expected to have 2 elements, got: %d", sc.readQueuesLen())
	}
	// Check that the first two address are evicted
	for i := 1; i <= 2; i++ {
		var addr common.Address
		addr[0] = byte(i)
		if _, ok := sc.GetAccount(addr[:]); ok {
			t.Fatalf("Expected not to find address %d", i)
		}
	}
	// Check that the other 6 addresses are there
	for i := 3; i <= 8; i++ {
		var addr common.Address
		addr[0] = byte(i)
		if _, ok := sc.GetAccount(addr[:]); !ok {
			t.Errorf("Expected to find address %d", i)
		}
	}
}

func TestReadAccountExisting(t *testing.T) {
	t.Parallel()
	sc := NewStateCache(32, datasize.ByteSize(2*accountItemSize))
	var account1 accounts.Account
	account1.Balance.SetUint64(1)
	var addr1 common.Address
	sc.SetAccountRead(addr1[:], &account1)
	defer func() {
		//nolint:staticcheck
		if r := recover(); r != nil {
		}
	}()
	sc.SetAccountRead(addr1[:], &account1)
	t.Fatalf("Expected to panic")
}

func TestWriteAccountExceedLimit(t *testing.T) {
	t.Parallel()
	sc := NewStateCache(32, datasize.ByteSize(2*accountItemSize))
	defer func() {
		//nolint:staticcheck
		if r := recover(); r != nil {
		}
	}()
	for i := 1; i <= 3; i++ {
		var addr common.Address
		addr[0] = byte(i)
		var account accounts.Account
		account.Balance.SetUint64(uint64(i))
		sc.SetAccountWrite(addr[:], &account)
	}
}

func TestGetDeletedAccount(t *testing.T) {
	t.Parallel()
	sc := NewStateCache(32, datasize.ByteSize(4*accountItemSize))
	var account1 accounts.Account
	account1.Balance.SetUint64(1)
	account1.Nonce = 1
	var addr1 common.Address
	addr1[0] = 1
	sc.SetAccountRead(addr1[:], &account1)
	var account11 accounts.Account
	account11.Nonce = 2
	sc.SetAccountWrite(addr1[:], &account11)
	acc := sc.GetDeletedAccount(addr1[:])
	if acc != nil {
		t.Fatalf("Did not expect to find deleted account before deletion")
	}
	sc.SetAccountDelete(addr1[:])
	acc = sc.GetDeletedAccount(addr1[:])
	if acc == nil {
		t.Fatalf("Expected to find deleted account")
	}
	if acc.Nonce != 2 {
		t.Fatalf("Expected to find deleted account with nonce 2, got %d", acc.Nonce)
	}
}

func TestReadWriteAbsentDeleteStorage(t *testing.T) {
	t.Parallel()
	sc := NewStateCache(32, datasize.ByteSize(4*storageItemSize))
	// Add absents
	for i := 1; i <= 4; i++ {
		var addr common.Address
		addr[0] = byte(i)
		var loc common.Hash
		loc[1] = byte(i)
		sc.SetStorageAbsent(addr[:], loc[:])
	}
	if sc.readQueuesLen() != 4 {
		t.Fatalf("expected 4 reads got: %d", sc.readQueuesLen())
	}
	for i := 1; i <= 4; i++ {
		var addr common.Address
		addr[0] = byte(i)
		var loc common.Hash
		loc[1] = byte(i)
		if s, ok := sc.GetStorage(addr[:], loc[:]); !ok || s != nil {
			t.Fatalf("expected entry with %x,%x not to exist", addr, loc)
		}
	}
	// Replace all 4 absents with deletes
	for i := 1; i <= 4; i++ {
		var addr common.Address
		addr[0] = byte(i)
		var loc common.Hash
		loc[1] = byte(i)
		sc.SetStorageDelete(addr[:], loc[:])
	}
	if sc.readQueuesLen() != 0 {
		t.Fatalf("expected 0 reads got: %d", sc.readQueuesLen())
	}
	for i := 1; i <= 4; i++ {
		var addr common.Address
		addr[0] = byte(i)
		var loc common.Hash
		loc[1] = byte(i)
		if s, ok := sc.GetStorage(addr[:], loc[:]); !ok || s != nil {
			t.Fatalf("expected entry with %x,%x not to exist", addr, loc)
		}
	}
	// Replace all 4 deletes with writes
	for i := 1; i <= 4; i++ {
		var addr common.Address
		addr[0] = byte(i)
		var loc common.Hash
		loc[1] = byte(i)
		var val common.Hash
		val[2] = byte(i)
		sc.SetStorageWrite(addr[:], loc[:], val[:])
	}
	if sc.WriteCount() != 4 {
		t.Fatalf("expected 4 writes, got %d", sc.WriteCount())
	}
	for i := 1; i <= 4; i++ {
		var addr common.Address
		addr[0] = byte(i)
		var loc common.Hash
		loc[1] = byte(i)
		if _, ok := sc.GetStorage(addr[:], loc[:]); !ok {
			t.Fatalf("expected entry with %x,%x to exist", addr, loc)
		}
	}
}

func TestReadStorageExisting(t *testing.T) {
	t.Parallel()
	sc := NewStateCache(32, datasize.ByteSize(2*storageItemSize))
	var addr1 common.Address
	addr1[0] = 1
	var loc1 common.Hash
	var val1 common.Hash
	val1[2] = 1
	sc.SetStorageRead(addr1[:], loc1[:], val1[:])
	defer func() {
		//nolint:staticcheck
		if r := recover(); r != nil {
		}
	}()
	sc.SetStorageRead(addr1[:], loc1[:], val1[:])
	t.Fatalf("Expected to panic")
}

func TestWriteStorageExceedLimit(t *testing.T) {
	t.Parallel()
	sc := NewStateCache(32, datasize.ByteSize(2*storageItemSize))
	defer func() {
		//nolint:staticcheck
		if r := recover(); r != nil {
		}
	}()
	for i := 1; i <= 3; i++ {
		var addr common.Address
		addr[0] = byte(i)
		var loc common.Hash
		loc[1] = byte(i)
		var val common.Hash
		val[2] = byte(i)
		sc.SetStorageWrite(addr[:], loc[:], val[:])
	}
}

func TestCodeReadWriteAbsentDelete(t *testing.T) {
	t.Parallel()
	sc := NewStateCache(32, datasize.ByteSize(4*(codeItemSize+3)))
	// Add absents
	for i := 1; i <= 4; i++ {
		var addr common.Address
		addr[0] = byte(i)
		sc.SetCodeAbsent(addr[:])
	}
	if sc.readQueuesLen() != 4 {
		t.Fatalf("expected 4 reads got: %d", sc.readQueuesLen())
	}
	for i := 1; i <= 4; i++ {
		var addr common.Address
		addr[0] = byte(i)
		if c, ok := sc.GetCode(addr[:]); !ok || c != nil {
			t.Fatalf("expected entry with %x not to exist", addr)
		}
	}
	// Replace all 4 absents with deletes
	for i := 1; i <= 4; i++ {
		var addr common.Address
		addr[0] = byte(i)
		sc.SetCodeDelete(addr[:])
	}
	if sc.readQueuesLen() != 0 {
		t.Fatalf("expected 0 reads got: %d", sc.readQueuesLen())
	}
	for i := 1; i <= 4; i++ {
		var addr common.Address
		addr[0] = byte(i)
		if c, ok := sc.GetCode(addr[:]); !ok || c != nil {
			t.Fatalf("expected entry with %x not to exist", addr)
		}
	}
	// Replace all 4 deletes with writes
	for i := 1; i <= 4; i++ {
		var addr common.Address
		addr[0] = byte(i)
		var code = []byte{byte(i), 2, 3}
		sc.SetCodeWrite(addr[:], code)
	}
	if sc.WriteCount() != 4 {
		t.Fatalf("expected 4 writes, got %d", sc.WriteCount())
	}
	for i := 1; i <= 4; i++ {
		var addr common.Address
		addr[0] = byte(i)
		if _, ok := sc.GetCode(addr[:]); !ok {
			t.Fatalf("expected entry with %x to exist", addr)
		}
	}
}

func TestReadCodeExisting(t *testing.T) {
	t.Parallel()
	sc := NewStateCache(32, datasize.ByteSize(2*(codeItemSize+3)))
	var addr1 common.Address
	addr1[0] = 1
	code1 := []byte{1, 2, 3}
	sc.SetCodeRead(addr1[:], code1)
	defer func() {
		//nolint:staticcheck
		if r := recover(); r != nil {
		}
	}()
	sc.SetCodeRead(addr1[:], code1)
	t.Fatalf("Expected to panic")
}

func TestWriteCodeExceedLimit(t *testing.T) {
	t.Parallel()
	sc := NewStateCache(32, datasize.ByteSize(2*(codeItemSize+3)))
	defer func() {
		//nolint:staticcheck
		if r := recover(); r != nil {
		}
	}()
	for i := 1; i <= 3; i++ {
		var addr common.Address
		addr[0] = byte(i)
		code := []byte{byte(i), 2, 3}
		sc.SetCodeWrite(addr[:], code)
	}
}
