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

package accounts

import (
	"testing"

	"github.com/holiman/uint256"

	libcommon "github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/crypto"
)

func TestEmptyAccount(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	a := Account{}

	encodedAccount := make([]byte, a.EncodingLengthForStorage())
	a.EncodeForStorage(encodedAccount)

	var decodedAccount Account
	if err := decodedAccount.DecodeForStorage(encodedAccount); err != nil {
		t.Fatal("cant decode the account", err, encodedAccount)
	}
}

func TestAccountEncodeWithCode(t *testing.T) {
	t.Parallel()
	a := Account{
		Initialised: true,
		Nonce:       2,
		Balance:     *new(uint256.Int).SetUint64(1000),
		Root:        libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
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
	t.Parallel()
	a := Account{
		Initialised: true,
		Nonce:       2,
		Balance:     *new(uint256.Int).SetUint64(1000),
		Root:        libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
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
	t.Parallel()
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
	t.Parallel()
	a := Account{
		Initialised: true,
		Nonce:       0,
		Balance:     *uint256.NewInt(0),
		Root:        libcommon.HexToHash("123"),
		CodeHash:    libcommon.HexToHash("123"),
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
	t.Parallel()
	a := Account{
		Initialised: true,
		Nonce:       0,
		Balance:     *uint256.NewInt(0),
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

func TestIncarnationForEmptyAccount(t *testing.T) {
	t.Parallel()
	a := Account{
		Initialised: true,
		Nonce:       100,
		Balance:     *new(uint256.Int),
		Root:        emptyRoot,
		CodeHash:    emptyCodeHash,
		Incarnation: 4,
	}

	encodedAccount := make([]byte, a.EncodingLengthForStorage())
	a.EncodeForStorage(encodedAccount)

	if _, err := DecodeIncarnationFromStorage(encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	decodedIncarnation, err := DecodeIncarnationFromStorage(encodedAccount)
	if err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedIncarnation)
}

func TestEmptyIncarnationForEmptyAccount2(t *testing.T) {
	t.Parallel()
	a := Account{}

	encodedAccount := make([]byte, a.EncodingLengthForStorage())
	a.EncodeForStorage(encodedAccount)

	if _, err := DecodeIncarnationFromStorage(encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	decodedIncarnation, err := DecodeIncarnationFromStorage(encodedAccount)
	if err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedIncarnation)

}

func TestIncarnationWithNonEmptyAccount(t *testing.T) {
	t.Parallel()
	a := Account{
		Initialised: true,
		Nonce:       2,
		Balance:     *new(uint256.Int).SetUint64(1000),
		Root:        libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
		Incarnation: 4,
	}

	encodedAccount := make([]byte, a.EncodingLengthForStorage())
	a.EncodeForStorage(encodedAccount)

	if _, err := DecodeIncarnationFromStorage(encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	decodedIncarnation, err := DecodeIncarnationFromStorage(encodedAccount)
	if err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedIncarnation)

}

func TestIncarnationWithNoIncarnation(t *testing.T) {
	t.Parallel()
	a := Account{
		Initialised: true,
		Nonce:       2,
		Balance:     *new(uint256.Int).SetUint64(1000),
		Root:        libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
		Incarnation: 0,
	}

	encodedAccount := make([]byte, a.EncodingLengthForStorage())
	a.EncodeForStorage(encodedAccount)

	if _, err := DecodeIncarnationFromStorage(encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	decodedIncarnation, err := DecodeIncarnationFromStorage(encodedAccount)
	if err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedIncarnation)

}

func TestIncarnationWithInvalidEncodedAccount(t *testing.T) {

	var failingSlice = []byte{1, 12}

	if incarnation, err := DecodeIncarnationFromStorage(failingSlice); err == nil {
		t.Fatal("decoded the incarnation", incarnation, failingSlice)
	}

}

func isIncarnationEqual(t *testing.T, initialIncarnation uint64, decodedIncarnation uint64) {
	if initialIncarnation != decodedIncarnation {
		t.Fatal("Can't decode the incarnation", initialIncarnation, decodedIncarnation)
	}
}
