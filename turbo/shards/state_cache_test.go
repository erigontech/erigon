package shards

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/c2h5oh/datasize"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon/core/types/accounts"
)

func TestCacheBtreeOrderAccountStorage2(t *testing.T) {
	sc := NewStateCache(32, datasize.ByteSize(128*accountItemSize))
	var a1 libcommon.Address
	a1[0] = 1
	sc.SetAccountRead(a1.Bytes(), &accounts.Account{})
	sc.SetAccountWrite(a1.Bytes(), &accounts.Account{Incarnation: 2})
	x, ok := sc.GetAccount(a1.Bytes())
	fmt.Printf("%+v,%t\n", x.Incarnation, ok)

}

func TestCacheBtreeOrderAccountStorage(t *testing.T) {
	sc := NewStateCache(32, datasize.ByteSize(128*accountItemSize))
	var a1, a2 libcommon.Address
	a1[0] = 1
	a2[0] = 2
	sc.SetAccountWrite(a2.Bytes(), &accounts.Account{})
	sc.SetAccountWrite(a1.Bytes(), &accounts.Account{})
	lastK := make([]byte, 0, 128)
	curK := make([]byte, 0, 128)
	if err := sc.WalkAccounts([]byte{}, func(addrHash libcommon.Hash, account *accounts.Account) (bool, error) {
		curK = append(curK[:0], addrHash.Bytes()...)
		assert.True(t, bytes.Compare(lastK, curK) < 0)
		lastK = append(lastK[:0], curK...)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	var l1, l2, l3 libcommon.Hash
	l1[0] = 42
	l2[0] = 2
	l3[0] = 3
	sc.SetStorageWrite(a1.Bytes(), 1, l1.Bytes(), nil)
	sc.SetStorageWrite(a1.Bytes(), 1, l2.Bytes(), nil)
	sc.SetStorageWrite(a2.Bytes(), 1, l3.Bytes(), nil)
	lastK = lastK[:0]
	if err := sc.WalkStorage(libcommon.BytesToHash(sha3.NewLegacyKeccak256().Sum(a1.Bytes())), 1, nil, func(locHash libcommon.Hash, val []byte) error {
		curK = append(curK[:0], locHash.Bytes()...)
		assert.True(t, bytes.Compare(lastK, curK) < 0)
		lastK = append(lastK[:0], curK...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sc.SetCodeWrite(a1.Bytes(), 1, []byte{1})
	sc.SetCodeWrite(a2.Bytes(), 1, []byte{2})
	lastK = lastK[:0]
	//if err := WalkWrites(sc.PrepareWrites(), nil, nil, nil, nil, func(address []byte, incarnation uint64, code []byte) error {
	//	i++
	//	if i == 1 {
	//		assert.Equal(t, a1.Bytes(), address)
	//		assert.Equal(t, 1, incarnation)
	//		assert.Equal(t, []byte{1}, code)
	//	}
	//	if i == 2 {
	//		assert.Equal(t, a2.Bytes(), address)
	//		assert.Equal(t, 1, incarnation)
	//		assert.Equal(t, []byte{2}, code)
	//	}
	//	return nil
	//}, nil); err != nil {
	//	t.Fatal(err)
	//}
}

func TestAccountReads(t *testing.T) {
	sc := NewStateCache(32, datasize.ByteSize(4*accountItemSize))
	var account1 accounts.Account
	account1.Balance.SetUint64(1)
	var addr1 libcommon.Address
	addr1[0] = 1
	sc.SetAccountRead(addr1.Bytes(), &account1)
	if _, ok := sc.GetAccount(addr1.Bytes()); !ok {
		t.Fatalf("Expected to find account with addr1")
	}
	var addr2 libcommon.Address
	addr2[0] = 2
	if _, ok := sc.GetAccount(addr2.Bytes()); ok {
		t.Fatalf("Did not expect account with addr2")
	}
	var addr3 libcommon.Address
	addr3[0] = 3
	sc.SetAccountAbsent(addr3.Bytes())
	if a, ok := sc.GetAccount(addr3.Bytes()); !ok || a != nil {
		t.Fatalf("Expected account with addr3 to be absent")
	}
	for i := 4; i <= 6; i++ {
		var account accounts.Account
		account.Balance.SetUint64(uint64(i))
		var addr libcommon.Address
		addr[0] = byte(i)
		sc.SetAccountRead(addr.Bytes(), &account)
	}
	// Out of 6 addresses, one was not associated with an account or absence record. So 5 records would be in the cache
	// But since the limit is 4, the first addr will be evicted
	if _, ok := sc.GetAccount(addr1.Bytes()); ok {
		t.Fatalf("Expected addr1 to be evicted")
	}
	for i := 4; i <= 6; i++ {
		var account accounts.Account
		account.Balance.SetUint64(uint64(i))
		var addr libcommon.Address
		addr[0] = byte(i)
		if _, ok := sc.GetAccount(addr.Bytes()); !ok {
			t.Fatalf("Expected to find account with addr %x", addr)
		}
	}
}

func TestAccountReadWrites(t *testing.T) {
	sc := NewStateCache(32, datasize.ByteSize(9*accountItemSize))
	var account1 accounts.Account
	account1.Balance.SetUint64(1)
	var addr1 libcommon.Address
	addr1[0] = 1
	sc.SetAccountWrite(addr1.Bytes(), &account1)
	if _, ok := sc.GetAccount(addr1.Bytes()); !ok {
		t.Fatalf("Expected to find account with addr1")
	}
	if sc.WriteCount() != 1 {
		t.Fatalf("Write queue is expected to have 1 elements, got: %d", sc.WriteCount())
	}
	// Replace the existing value
	var account11 accounts.Account
	account11.Balance.SetUint64(11)
	sc.SetAccountWrite(addr1.Bytes(), &account11)
	if a, ok := sc.GetAccount(addr1.Bytes()); !ok {
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
	var addr2 libcommon.Address
	addr2[0] = 2
	sc.SetAccountRead(addr2.Bytes(), &account2)
	// Check that readQueue is empty
	if sc.readQueuesLen() != 1 {
		t.Fatalf("Read queue is expected to be 1 element")
	}
	var account22 accounts.Account
	account22.Balance.SetUint64(22)
	sc.SetAccountWrite(addr2.Bytes(), &account22)
	if a, ok := sc.GetAccount(addr2.Bytes()); !ok {
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
	var addr3 libcommon.Address
	addr3[0] = 3
	sc.SetAccountWrite(addr3.Bytes(), &account3)
	sc.SetAccountDelete(addr3.Bytes())
	if a, ok := sc.GetAccount(addr3.Bytes()); !ok || a != nil {
		t.Fatalf("Expected account addr3 to be deleted")
	}
	if sc.WriteCount() != 3 {
		t.Fatalf("Write queue is expected to have 3 elements, got: %d", sc.WriteCount())
	}
	// Deleting read account
	var account4 accounts.Account
	account4.Balance.SetUint64(4)
	var addr4 libcommon.Address
	addr4[0] = 4
	sc.SetAccountRead(addr4.Bytes(), &account4)
	sc.SetAccountDelete(addr4.Bytes())
	if a, ok := sc.GetAccount(addr4.Bytes()); !ok || a != nil {
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
	var addr5 libcommon.Address
	addr5[0] = 5
	sc.SetAccountDelete(addr5.Bytes())
	if a, ok := sc.GetAccount(addr5.Bytes()); !ok || a != nil {
		t.Fatalf("Expected account addr5 to be deleted")
	}
	if sc.WriteCount() != 5 {
		t.Fatalf("Write queue is expected to have 5 elements, got: %d", sc.WriteCount())
	}
}

func TestReplaceAccountReadsWithWrites(t *testing.T) {
	sc := NewStateCache(32, datasize.ByteSize(6*accountItemSize))
	for i := 1; i <= 4; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		var account accounts.Account
		account.Balance.SetUint64(uint64(i))
		sc.SetAccountWrite(addr.Bytes(), &account)
	}
	writes := sc.PrepareWrites()
	sc.TurnWritesToReads(writes)
	if sc.WriteCount() != 0 {
		t.Fatalf("Write queue is expected to be empty, got: %d", sc.WriteCount())
	}
	if sc.readQueuesLen() != 4 {
		t.Fatalf("Read queue is expected to have 4 elements, got: %d", sc.readQueuesLen())
	}
	// Do 4 more delets
	for i := 5; i <= 8; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		var account accounts.Account
		account.Balance.SetUint64(uint64(i))
		sc.SetAccountWrite(addr.Bytes(), &account)
	}
	if sc.WriteCount() != 4 {
		t.Fatalf("Write queue is expected to have 4 elements, got: %d", sc.WriteCount())
	}
	if sc.readQueuesLen() != 2 {
		t.Fatalf("Read queue is expected to have 2 elements, got: %d", sc.readQueuesLen())
	}
	// Check that the first two address are evicted
	for i := 1; i <= 2; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		if _, ok := sc.GetAccount(addr.Bytes()); ok {
			t.Fatalf("Expected not to find address %d", i)
		}
	}
	// Check that the other 6 addresses are there
	for i := 3; i <= 8; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		if _, ok := sc.GetAccount(addr.Bytes()); !ok {
			t.Errorf("Expected to find address %d", i)
		}
	}
}

func TestReadAccountExisting(t *testing.T) {
	sc := NewStateCache(32, datasize.ByteSize(2*accountItemSize))
	var account1 accounts.Account
	account1.Balance.SetUint64(1)
	var addr1 libcommon.Address
	sc.SetAccountRead(addr1.Bytes(), &account1)
	defer func() {
		//nolint:staticcheck
		if r := recover(); r != nil {
		}
	}()
	sc.SetAccountRead(addr1.Bytes(), &account1)
	t.Fatalf("Expected to panic")
}

func TestWriteAccountExceedLimit(t *testing.T) {
	sc := NewStateCache(32, datasize.ByteSize(2*accountItemSize))
	defer func() {
		//nolint:staticcheck
		if r := recover(); r != nil {
		}
	}()
	for i := 1; i <= 3; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		var account accounts.Account
		account.Balance.SetUint64(uint64(i))
		sc.SetAccountWrite(addr.Bytes(), &account)
	}
}

func TestGetDeletedAccount(t *testing.T) {
	sc := NewStateCache(32, datasize.ByteSize(4*accountItemSize))
	var account1 accounts.Account
	account1.Balance.SetUint64(1)
	account1.Incarnation = 1
	var addr1 libcommon.Address
	addr1[0] = 1
	sc.SetAccountRead(addr1.Bytes(), &account1)
	var account11 accounts.Account
	account11.Incarnation = 2
	sc.SetAccountWrite(addr1.Bytes(), &account11)
	acc := sc.GetDeletedAccount(addr1.Bytes())
	if acc != nil {
		t.Fatalf("Did not expect to find deleted account before deletion")
	}
	sc.SetAccountDelete(addr1.Bytes())
	acc = sc.GetDeletedAccount(addr1.Bytes())
	if acc == nil {
		t.Fatalf("Expected to find deleted account")
	}
	if acc.Incarnation != 2 {
		t.Fatalf("Expected to find deleted account with incarnation 2, got %d", acc.Incarnation)
	}
}

func TestReadWriteAbsentDeleteStorage(t *testing.T) {
	sc := NewStateCache(32, datasize.ByteSize(4*storageItemSize))
	// Add absents
	for i := 1; i <= 4; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		var loc libcommon.Hash
		loc[1] = byte(i)
		sc.SetStorageAbsent(addr.Bytes(), 1, loc.Bytes())
	}
	if sc.readQueuesLen() != 4 {
		t.Fatalf("expected 4 reads got: %d", sc.readQueuesLen())
	}
	for i := 1; i <= 4; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		var loc libcommon.Hash
		loc[1] = byte(i)
		if s, ok := sc.GetStorage(addr.Bytes(), 1, loc.Bytes()); !ok || s != nil {
			t.Fatalf("expected entry with %x,1,%x not to exist", addr, loc)
		}
	}
	// Add reads for incarnation 2 (instead of 1), 6 records instead of 4, so only last 4 will stay
	for i := 1; i <= 6; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		var loc libcommon.Hash
		loc[1] = byte(i)
		var val libcommon.Hash
		val[2] = byte(i)
		sc.SetStorageRead(addr.Bytes(), 2, loc.Bytes(), val.Bytes())
	}
	if sc.readQueuesLen() != 4 {
		t.Fatalf("expected 4 reads got: %d", sc.readQueuesLen())
	}
	// Check that first two records were evicted
	for i := 1; i <= 2; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		var loc libcommon.Hash
		loc[1] = byte(i)
		if _, ok := sc.GetStorage(addr.Bytes(), 2, loc.Bytes()); ok {
			t.Fatalf("expected entry with %x,2,%x to be evicted", addr, loc)
		}
	}
	// Check that last 4 records stayed
	for i := 3; i <= 6; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		var loc libcommon.Hash
		loc[1] = byte(i)
		if _, ok := sc.GetStorage(addr.Bytes(), 2, loc.Bytes()); !ok {
			t.Errorf("expected entry with %x,2,%x to exist", addr, loc)
		}
	}
	// Replace all 4 entries with deletes
	for i := 1; i <= 4; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		var loc libcommon.Hash
		loc[1] = byte(i)
		sc.SetStorageDelete(addr.Bytes(), 1, loc.Bytes())
	}
	if sc.readQueuesLen() != 0 {
		t.Fatalf("expected 0 reads got: %d", sc.readQueuesLen())
	}
	for i := 1; i <= 4; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		var loc libcommon.Hash
		loc[1] = byte(i)
		if s, ok := sc.GetStorage(addr.Bytes(), 1, loc.Bytes()); !ok || s != nil {
			t.Fatalf("expected entry with %x,1,%x not to exist", addr, loc)
		}
	}
	// Replace all 4 deletes with writes
	for i := 1; i <= 4; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		var loc libcommon.Hash
		loc[1] = byte(i)
		var val libcommon.Hash
		val[2] = byte(i)
		sc.SetStorageWrite(addr.Bytes(), 1, loc.Bytes(), val.Bytes())
	}
	if sc.WriteCount() != 4 {
		t.Fatalf("expected 4 writes, got %d", sc.WriteCount())
	}
	for i := 1; i <= 4; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		var loc libcommon.Hash
		loc[1] = byte(i)
		if _, ok := sc.GetStorage(addr.Bytes(), 1, loc.Bytes()); !ok {
			t.Fatalf("expected entry with %x,1,%x to exist", addr, loc)
		}
	}
}

func TestReadStorageExisting(t *testing.T) {
	sc := NewStateCache(32, datasize.ByteSize(2*storageItemSize))
	var addr1 libcommon.Address
	addr1[0] = 1
	var loc1 libcommon.Hash
	var val1 libcommon.Hash
	val1[2] = 1
	sc.SetStorageRead(addr1.Bytes(), 1, loc1.Bytes(), val1.Bytes())
	defer func() {
		//nolint:staticcheck
		if r := recover(); r != nil {
		}
	}()
	sc.SetStorageRead(addr1.Bytes(), 1, loc1.Bytes(), val1.Bytes())
	t.Fatalf("Expected to panic")
}

func TestWriteStorageExceedLimit(t *testing.T) {
	sc := NewStateCache(32, datasize.ByteSize(2*storageItemSize))
	defer func() {
		//nolint:staticcheck
		if r := recover(); r != nil {
		}
	}()
	for i := 1; i <= 3; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		var loc libcommon.Hash
		loc[1] = byte(i)
		var val libcommon.Hash
		val[2] = byte(i)
		sc.SetStorageWrite(addr.Bytes(), 1, loc.Bytes(), val.Bytes())
	}
}

func TestCodeReadWriteAbsentDelete(t *testing.T) {
	sc := NewStateCache(32, datasize.ByteSize(4*(codeItemSize+3)))
	// Add absents
	for i := 1; i <= 4; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		sc.SetCodeAbsent(addr.Bytes(), 1)
	}
	if sc.readQueuesLen() != 4 {
		t.Fatalf("expected 4 reads got: %d", sc.readQueuesLen())
	}
	for i := 1; i <= 4; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		if c, ok := sc.GetCode(addr.Bytes(), 1); !ok || c != nil {
			t.Fatalf("expected entry with %x,1 not to exist", addr)
		}
	}
	// Add reads for incarnation 2 (instead of 1), 6 records instead of 4, so only last 4 will stay
	for i := 1; i <= 6; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		var code = []byte{byte(i), 2, 3}
		sc.SetCodeRead(addr.Bytes(), 2, code)
	}
	if sc.readQueuesLen() != 4 {
		t.Fatalf("expected 4 reads got: %d", sc.readQueuesLen())
	}
	// Check that first two records were evicted
	for i := 1; i <= 2; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		if _, ok := sc.GetCode(addr.Bytes(), 2); ok {
			t.Fatalf("expected entry with %x,2 to be evicted", addr)
		}
	}
	// Check that last 4 records stayed
	for i := 3; i <= 6; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		if _, ok := sc.GetCode(addr.Bytes(), 2); !ok {
			t.Errorf("expected entry with %x,2 to exist", addr)
		}
	}
	// Replace all 4 entries with deletes
	for i := 1; i <= 4; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		sc.SetCodeDelete(addr.Bytes(), 1)
	}
	if sc.readQueuesLen() != 0 {
		t.Fatalf("expected 0 reads got: %d", sc.readQueuesLen())
	}
	for i := 1; i <= 4; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		if c, ok := sc.GetCode(addr.Bytes(), 1); !ok || c != nil {
			t.Fatalf("expected entry with %x,1 not to exist", addr)
		}
	}
	// Replace all 4 deletes with writes
	for i := 1; i <= 4; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		var code = []byte{byte(i), 2, 3}
		sc.SetCodeWrite(addr.Bytes(), 1, code)
	}
	if sc.WriteCount() != 4 {
		t.Fatalf("expected 4 writes, got %d", sc.WriteCount())
	}
	for i := 1; i <= 4; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		if _, ok := sc.GetCode(addr.Bytes(), 1); !ok {
			t.Fatalf("expected entry with %x,1 to exist", addr)
		}
	}
}

func TestReadCodeExisting(t *testing.T) {
	sc := NewStateCache(32, datasize.ByteSize(2*(codeItemSize+3)))
	var addr1 libcommon.Address
	addr1[0] = 1
	code1 := []byte{1, 2, 3}
	sc.SetCodeRead(addr1.Bytes(), 1, code1)
	defer func() {
		//nolint:staticcheck
		if r := recover(); r != nil {
		}
	}()
	sc.SetCodeRead(addr1.Bytes(), 1, code1)
	t.Fatalf("Expected to panic")
}

func TestWriteCodeExceedLimit(t *testing.T) {
	sc := NewStateCache(32, datasize.ByteSize(2*(codeItemSize+3)))
	defer func() {
		//nolint:staticcheck
		if r := recover(); r != nil {
		}
	}()
	for i := 1; i <= 3; i++ {
		var addr libcommon.Address
		addr[0] = byte(i)
		code := []byte{byte(i), 2, 3}
		sc.SetCodeWrite(addr.Bytes(), 1, code)
	}
}
