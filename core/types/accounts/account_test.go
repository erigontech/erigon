package accounts

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"
)

func TestEmptyAccount(t *testing.T) {
	a := Account{
		Initialised: true,
		Nonce:       100,
		Balance:     *new(uint256.Int),
		Root:        emptyRoot,     // extAccount doesn't have Root value
		CodeHash:    emptyCodeHash, // extAccount doesn't have CodeHash value
		Incarnation: 5,
	}

	encodedAccount := make([]byte, a.EncodingLengthForStorage())
	a.EncodeForStorage(encodedAccount)

	var decodedAccount Account
	if err := decodedAccount.DecodeForStorage(encodedAccount); err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)
}

func TestEmptyAccount2(t *testing.T) {
	encodedAccount := Account{}

	b := make([]byte, encodedAccount.EncodingLengthForStorage())
	encodedAccount.EncodeForStorage(b)

	var decodedAccount Account
	if err := decodedAccount.DecodeForStorage(b); err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}
}

// fails if run package tests
// account_test.go:57: cant decode the account malformed RLP for Account(c064): prefixLength(1) + dataLength(0) != sliceLength(2) �d
func TestEmptyAccount_BufferStrangeBehaviour(t *testing.T) {
	a := Account{}

	encodedAccount := make([]byte, a.EncodingLengthForStorage())
	a.EncodeForStorage(encodedAccount)

	var decodedAccount Account
	if err := decodedAccount.DecodeForStorage(encodedAccount); err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}
}

func TestAccountEncodeWithCode(t *testing.T) {
	a := Account{
		Initialised: true,
		Nonce:       2,
		Balance:     *new(uint256.Int).SetUint64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
		Incarnation: 4,
	}

	encodedLen := a.EncodingLengthForStorage()
	encodedAccount := make([]byte, encodedLen)
	a.EncodeForStorage(encodedAccount)

	var decodedAccount Account
	if err := decodedAccount.DecodeForStorage(encodedAccount); err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)
}

func TestAccountEncodeWithCodeWithStorageSizeHack(t *testing.T) {
	a := Account{
		Initialised: true,
		Nonce:       2,
		Balance:     *new(uint256.Int).SetUint64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
		Incarnation: 5,
	}

	encodedLen := a.EncodingLengthForStorage()
	encodedAccount := make([]byte, encodedLen)
	a.EncodeForStorage(encodedAccount)

	var decodedAccount Account
	if err := decodedAccount.DecodeForStorage(encodedAccount); err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)
}

func TestAccountEncodeWithoutCode(t *testing.T) {
	a := Account{
		Initialised: true,
		Nonce:       2,
		Balance:     *new(uint256.Int).SetUint64(1000),
		Root:        emptyRoot,     // extAccount doesn't have Root value
		CodeHash:    emptyCodeHash, // extAccount doesn't have CodeHash value
		Incarnation: 5,
	}

	encodedLen := a.EncodingLengthForStorage()
	encodedAccount := make([]byte, encodedLen)
	a.EncodeForStorage(encodedAccount)

	var decodedAccount Account
	if err := decodedAccount.DecodeForStorage(encodedAccount); err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)
}

func TestEncodeAccountWithEmptyBalanceNonNilContractAndNotZeroIncarnation(t *testing.T) {
	a := Account{
		Initialised: true,
		Nonce:       0,
		Balance:     *uint256.NewInt(),
		Root:        common.HexToHash("123"),
		CodeHash:    common.HexToHash("123"),
		Incarnation: 1,
	}
	encodedLen := a.EncodingLengthForStorage()
	encodedAccount := make([]byte, encodedLen)
	a.EncodeForStorage(encodedAccount)

	var decodedAccount Account
	if err := decodedAccount.DecodeForStorage(encodedAccount); err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	isAccountsEqual(t, a, decodedAccount)
}
func TestEncodeAccountWithEmptyBalanceAndNotZeroIncarnation(t *testing.T) {
	a := Account{
		Initialised: true,
		Nonce:       0,
		Balance:     *uint256.NewInt(),
		Incarnation: 1,
	}
	encodedLen := a.EncodingLengthForStorage()
	encodedAccount := make([]byte, encodedLen)
	a.EncodeForStorage(encodedAccount)

	var decodedAccount Account
	if err := decodedAccount.DecodeForStorage(encodedAccount); err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}

	if a.Incarnation != decodedAccount.Incarnation {
		t.FailNow()
	}
	if a.Balance.Cmp(&decodedAccount.Balance) != 0 {
		t.FailNow()
	}
	if a.Nonce != decodedAccount.Nonce {
		t.FailNow()
	}
}

func isAccountsEqual(t *testing.T, src, dst Account) {
	if dst.Initialised != src.Initialised {
		t.Fatal("cant decode the account Initialised", src.Initialised, dst.Initialised)
	}

	if dst.CodeHash != src.CodeHash {
		t.Fatal("cant decode the account CodeHash", src.CodeHash, dst.CodeHash)
	}

	if dst.Balance.Cmp(&src.Balance) != 0 {
		t.Fatal("cant decode the account Balance", src.Balance, dst.Balance)
	}

	if dst.Nonce != src.Nonce {
		t.Fatal("cant decode the account Nonce", src.Nonce, dst.Nonce)
	}
	if dst.Incarnation != src.Incarnation {
		t.Fatal("cant decode the account Version", src.Incarnation, dst.Incarnation)
	}
}
