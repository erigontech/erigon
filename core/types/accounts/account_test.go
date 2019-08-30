package accounts

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common/pool"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"
)

func TestEmptyAccount(t *testing.T) {
	a := Account{
		Initialised: true,
		Nonce:       100,
		Balance:     *new(big.Int),
		Root:        emptyRoot,     // extAccount doesn't have Root value
		CodeHash:    emptyCodeHash, // extAccount doesn't have CodeHash value
	}

	encodedAccount := pool.GetBuffer(a.EncodingLengthForStorage())
	a.EncodeForStorage(encodedAccount.B)

	var decodedAccount Account
	if err := decodedAccount.Decode(encodedAccount.B); err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)
	isStorageSizeEqual(t, a, decodedAccount)

	pool.PutBuffer(encodedAccount)
}

func TestAccountEncodeWithCode(t *testing.T) {
	a := Account{
		Initialised: true,
		Nonce:       2,
		Balance:     *new(big.Int).SetInt64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
	}

	encodedLen := a.EncodingLengthForStorage()
	encodedAccount := make([]byte, encodedLen)
	a.EncodeForStorage(encodedAccount)

	var decodedAccount Account
	if err := decodedAccount.Decode(encodedAccount); err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)
	isStorageSizeEqual(t, a, decodedAccount)
}

func TestAccountEncodeWithCodeWithStorageSizeHack(t *testing.T) {
	a := Account{
		Initialised:    true,
		Nonce:          2,
		Balance:        *new(big.Int).SetInt64(1000),
		Root:           common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:       common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
		HasStorageSize: true,
		StorageSize:    0,
	}
	a.StorageSize = 10

	encodedLen := a.EncodingLengthForStorage()
	encodedAccount := make([]byte, encodedLen)
	a.EncodeForStorage(encodedAccount)

	var decodedAccount Account
	if err := decodedAccount.Decode(encodedAccount); err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)
}

func TestAccountEncodeWithoutCode(t *testing.T) {
	a := Account{
		Initialised: true,
		Nonce:       2,
		Balance:     *new(big.Int).SetInt64(1000),
		Root:        emptyRoot,     // extAccount doesn't have Root value
		CodeHash:    emptyCodeHash, // extAccount doesn't have CodeHash value
	}

	encodedLen := a.EncodingLengthForStorage()
	encodedAccount := make([]byte, encodedLen)
	a.EncodeForStorage(encodedAccount)

	var decodedAccount Account
	if err := decodedAccount.Decode(encodedAccount); err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)
	isStorageSizeEqual(t, a, decodedAccount)
}

func TestAccountEncodeWithCodeEIP2027(t *testing.T) {
	account := Account{
		Initialised: true,
		Nonce:       2,
		Balance:     *new(big.Int).SetInt64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
	}
	account.HasStorageSize = true
	account.StorageSize = 10

	encodedLen := account.EncodingLengthForStorage()
	encodedAccount := make([]byte, encodedLen)
	account.EncodeForStorage(encodedAccount)

	var decodedAccount Account
	if err := decodedAccount.Decode(encodedAccount); err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	//after enable eip2027 storage size for account with empty storage size equals to 0
	isAccountsEqual(t, account, decodedAccount)
	isStorageSizeEqual(t, account, decodedAccount)
}

func TestAccountEncodeWithCodeWithStorageSizeEIP2027(t *testing.T) {
	const HugeNumber = uint64(1 << 63)

	storageSizes := []uint64{0, 1, 2, 3, 10, 100,
		HugeNumber, HugeNumber + 1, HugeNumber + 2, HugeNumber + 10, HugeNumber + 100, HugeNumber + 10000000}

	for _, storageSize := range storageSizes {
		a := Account{
			Initialised:    true,
			Nonce:          2,
			Balance:        *new(big.Int).SetInt64(1000),
			Root:           common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
			CodeHash:       common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			HasStorageSize: true,
			StorageSize:    storageSize,
		}

		encodedLen := a.EncodingLengthForStorage()
		encodedAccount := make([]byte, encodedLen)
		a.EncodeForStorage(encodedAccount)

		var decodedAccount Account
		if err := decodedAccount.Decode(encodedAccount); err != nil {
			t.Fatal("cant decode the account", err, encodedAccount)
		}

		isAccountsEqual(t, a, decodedAccount)

		if decodedAccount.StorageSize != a.StorageSize {
			t.Fatal("cant decode the account StorageSize", decodedAccount.StorageSize, a.StorageSize)
		}
	}
}

func TestAccountEncodeWithoutCodeEIP2027(t *testing.T) {
	a := Account{
		Initialised: true,
		Nonce:       2,
		Balance:     *new(big.Int).SetInt64(1000),
		Root:        emptyRoot,     // extAccount doesn't have Root value
		CodeHash:    emptyCodeHash, // extAccount doesn't have CodeHash value
	}

	encodedLen := a.EncodingLengthForStorage()
	encodedAccount := make([]byte, encodedLen)
	a.EncodeForStorage(encodedAccount)

	var decodedAccount Account
	if err := decodedAccount.Decode(encodedAccount); err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)
	isStorageSizeEqual(t, a, decodedAccount)
}

func isAccountsEqual(t *testing.T, src, dst Account) {
	if dst.Initialised != src.Initialised {
		t.Fatal("cant decode the account Initialised", src.Initialised, dst.Initialised)
	}

	if dst.CodeHash != src.CodeHash {
		t.Fatal("cant decode the account CodeHash", src.CodeHash, dst.CodeHash)
	}

	if !bytes.Equal(dst.Root.Bytes(), src.Root.Bytes()) {
		t.Fatal("cant decode the account Root", src.Root, dst.Root)
	}

	if dst.Balance.Cmp(&src.Balance) != 0 {
		t.Fatal("cant decode the account Balance", src.Balance, dst.Balance)
	}

	if dst.Nonce != src.Nonce {
		t.Fatal("cant decode the account Nonce", src.Nonce, dst.Nonce)
	}
}

func isStorageSizeEqual(t *testing.T, src, dst Account) {
	t.Helper()

	if !src.HasStorageSize {
		if dst.HasStorageSize {
			t.Fatal("cant decode the account StorageSize - should be nil", src.StorageSize, dst.StorageSize)
		}
	} else {
		if src.HasStorageSize && !dst.HasStorageSize {
			t.Fatal("cant decode the account StorageSize - should be not nil", src.StorageSize, dst.StorageSize)
		}

		if dst.StorageSize != src.StorageSize {
			t.Fatal("cant decode the account StorageSize", src.StorageSize, dst.StorageSize)
		}
	}
}
