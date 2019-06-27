package accounts

import (
	"bytes"
	"math/big"
	"testing"

	"context"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/params"
)

func TestAccountEncodeWithCode(t *testing.T) {
	a := &Account{
		Nonce:    2,
		Balance:  new(big.Int).SetInt64(1000),
		Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash: crypto.Keccak256([]byte{1, 2, 3}),
	}

	encodedAccount, err := a.Encode(context.WithValue(context.Background(), params.IsEIP2027Enabled, false))
	if err != nil {
		t.Fatal("cant encode the account", err, a)
	}

	decodedAccount, err := Decode(encodedAccount)
	if err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)
	isStorageSizeEqual(t, a, decodedAccount)
}

func TestAccountDecodeEmpty(t *testing.T) {
	decodedAccount, err := Decode([]byte{})
	if err != nil {
		t.Fatal("cant decode the account")
	}

	if decodedAccount != nil {
		t.Fatal("account should be nil")
	}
}

func TestAccountDecodeNil(t *testing.T) {
	decodedAccount, err := Decode(nil)
	if err != nil {
		t.Fatal("cant decode the account")
	}

	if decodedAccount != nil {
		t.Fatal("account should be nil")
	}
}

func TestAccountEncodeWithCodeWithStorageSizeHack(t *testing.T) {
	a := &Account{
		Nonce:       2,
		Balance:     new(big.Int).SetInt64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    crypto.Keccak256([]byte{1, 2, 3}),
		StorageSize: new(uint64),
	}
	*a.StorageSize = 10

	encodedAccount, err := a.Encode(context.WithValue(context.Background(), params.IsEIP2027Enabled, false))
	if err != nil {
		t.Fatal("cant encode the account", err, a)
	}

	decodedAccount, err := Decode(encodedAccount)
	if err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)

	if decodedAccount.StorageSize == nil {
		t.Fatal("storage size for decoded account shouldn't be nil")
	}

	if *decodedAccount.StorageSize != *a.StorageSize {
		t.Fatal("cant decode the account StorageSize - should not nil", decodedAccount.StorageSize, a.StorageSize)
	}
}

func TestAccountEncodeWithoutCode(t *testing.T) {
	a := &Account{
		Nonce:    2,
		Balance:  new(big.Int).SetInt64(1000),
		Root:     emptyRoot,     // extAccount doesn't have Root value
		CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
	}

	encodedAccount, err := a.Encode(context.WithValue(context.Background(), params.IsEIP2027Enabled, false))
	if err != nil {
		t.Fatal("cant encode the account", err, a)
	}

	decodedAccount, err := Decode(encodedAccount)
	if err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)
	isStorageSizeEqual(t, a, decodedAccount)
}

func TestAccountEncodeWithCodeEIP2027(t *testing.T) {
	account := &Account{
		Nonce:    2,
		Balance:  new(big.Int).SetInt64(1000),
		Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash: crypto.Keccak256([]byte{1, 2, 3}),
	}

	encodedAccount, err := account.Encode(context.WithValue(context.Background(), params.IsEIP2027Enabled, true))
	if err != nil {
		t.Fatal("cant encode the account", err, account)
	}

	decodedAccount, err := Decode(encodedAccount)
	if err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	//after enable eip2027 storage size for account with empty storage size equals to 0
	account.StorageSize = new(uint64)
	isAccountsEqual(t, account, decodedAccount)
	isStorageSizeEqual(t, account, decodedAccount)
}

func TestAccountEncodeWithCodeWithStorageSizeEIP2027(t *testing.T) {
	const HugeNumber = uint64(1 << 63)

	storageSizes := []uint64{0, 1, 2, 3, 10, 100,
		HugeNumber, HugeNumber + 1, HugeNumber + 2, HugeNumber + 10, HugeNumber + 100, HugeNumber + 10000000}

	for _, storageSize := range storageSizes {
		a := &Account{
			Nonce:       2,
			Balance:     new(big.Int).SetInt64(1000),
			Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
			CodeHash:    crypto.Keccak256([]byte{1, 2, 3}),
			StorageSize: new(uint64),
		}
		*a.StorageSize = storageSize

		encodedAccount, err := a.Encode(context.WithValue(context.Background(), params.IsEIP2027Enabled, true))
		if err != nil {
			t.Fatal("cant encode the account", err, a)
		}

		decodedAccount, err := Decode(encodedAccount)
		if err != nil {
			t.Fatal("cant decode the account", err, encodedAccount)
		}

		isAccountsEqual(t, a, decodedAccount)

		if *decodedAccount.StorageSize != *a.StorageSize {
			t.Fatal("cant decode the account StorageSize", *decodedAccount.StorageSize, *a.StorageSize)
		}
	}
}

func TestAccountEncodeWithoutCodeEIP2027(t *testing.T) {
	a := &Account{
		Nonce:    2,
		Balance:  new(big.Int).SetInt64(1000),
		Root:     emptyRoot,     // extAccount doesn't have Root value
		CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
	}

	encodedAccount, err := a.Encode(context.WithValue(context.Background(), params.IsEIP2027Enabled, true))
	if err != nil {
		t.Fatal("cant encode the account", err, a)
	}

	decodedAccount, err := Decode(encodedAccount)
	if err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)
	isStorageSizeEqual(t, a, decodedAccount)
}

func isAccountsEqual(t *testing.T, src, dst *Account) {
	if !bytes.Equal(dst.CodeHash, src.CodeHash) {
		t.Fatal("cant decode the account CodeHash", src.CodeHash, dst.CodeHash)
	}

	if !bytes.Equal(dst.Root.Bytes(), src.Root.Bytes()) {
		t.Fatal("cant decode the account Root", src.Root, dst.Root)
	}

	if dst.Balance.Cmp(src.Balance) != 0 {
		t.Fatal("cant decode the account Balance", src.Balance, dst.Balance)
	}

	if dst.Nonce != src.Nonce {
		t.Fatal("cant decode the account Nonce", src.Nonce, dst.Nonce)
	}
}

func isStorageSizeEqual(t *testing.T, src, dst *Account) {
	if src.StorageSize == nil {
		if dst.StorageSize != nil {
			t.Fatal("cant decode the account StorageSize - should be nil", src.StorageSize, dst.StorageSize)
		}
	} else {
		if dst.StorageSize == nil {
			t.Fatal("cant decode the account StorageSize - should be not nil", src.StorageSize, dst.StorageSize)
		}

		if *dst.StorageSize != *src.StorageSize {
			t.Fatal("cant decode the account StorageSize", src.StorageSize, dst.StorageSize)
		}
	}
}
