package accounts

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"
)

func TestAccountEncodeWithCode(t *testing.T) {
	a := &Account{
		Nonce:    2,
		Balance:  new(big.Int).SetInt64(1000),
		Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash: crypto.Keccak256([]byte{1, 2, 3}),
	}

	encodedAccount, err := a.Encode(false)
	if err != nil {
		t.Fatal("cant encode the account", err, a)
	}

	decodedAccount, err := Decode(encodedAccount)
	if err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)
}

func TestAccountEncodeWithCodeWithStorageSize(t *testing.T) {
	a := &Account{
		Nonce:       2,
		Balance:     new(big.Int).SetInt64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    crypto.Keccak256([]byte{1, 2, 3}),
		StorageSize: new(uint64),
	}
	*a.StorageSize = 10

	encodedAccount, err := a.Encode(false)
	if err != nil {
		t.Fatal("cant encode the account", err, a)
	}

	decodedAccount, err := Decode(encodedAccount)
	if err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)
}

func TestAccountEncodeWithoutCode(t *testing.T) {
	a := &Account{
		Nonce:    2,
		Balance:  new(big.Int).SetInt64(1000),
		Root:     emptyRoot,     // extAccount doesn't have Root value
		CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
	}

	encodedAccount, err := a.Encode(false)
	if err != nil {
		t.Fatal("cant encode the account", err, a)
	}

	decodedAccount, err := Decode(encodedAccount)
	if err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)
}

func isAccountsEqual(t *testing.T, src, dst *Account) {
	if !bytes.Equal(dst.CodeHash, src.CodeHash) {
		t.Fatal("cant decode the account CodeHash", src.CodeHash, dst.CodeHash)
	}

	if src.StorageSize == nil {
		if dst.StorageSize != nil {
			t.Fatal("cant decode the account StorageSize - should be nil", dst.StorageSize)
		}
	} else if *dst.StorageSize != *src.StorageSize {
		t.Fatal("cant decode the account StorageSize", src.StorageSize, dst.StorageSize)
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
