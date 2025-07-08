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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/crypto"
)

func TestEmptyAccount(t *testing.T) {
	t.Parallel()
	a := Account{
		Initialised: true,
		Nonce:       100,
		Balance:     *new(uint256.Int),
		Root:        empty.RootHash, // extAccount doesn't have Root value
		CodeHash:    empty.CodeHash, // extAccount doesn't have CodeHash value
		Incarnation: 5,
	}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestEmptyAccount2(t *testing.T) {
	t.Parallel()
	emptyAcc := Account{}

	encodedAccount := SerialiseV3(&emptyAcc)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, emptyAcc.Incarnation, decodedAcc.Incarnation)
}

// fails if run package tests
// account_test.go:57: cant decode the account malformed RLP for Account(c064): prefixLength(1) + dataLength(0) != sliceLength(2) �d
func TestEmptyAccount_BufferStrangeBehaviour(t *testing.T) {
	t.Parallel()
	a := Account{}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestAccountEncodeWithCode(t *testing.T) {
	t.Parallel()
	a := Account{
		Initialised: true,
		Nonce:       2,
		Balance:     *new(uint256.Int).SetUint64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
		Incarnation: 4,
	}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestAccountEncodeWithCodeWithStorageSizeHack(t *testing.T) {
	t.Parallel()
	a := Account{
		Initialised: true,
		Nonce:       2,
		Balance:     *new(uint256.Int).SetUint64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
		Incarnation: 5,
	}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestAccountEncodeWithoutCode(t *testing.T) {
	t.Parallel()
	a := Account{
		Initialised: true,
		Nonce:       2,
		Balance:     *new(uint256.Int).SetUint64(1000),
		Root:        empty.RootHash, // extAccount doesn't have Root value
		CodeHash:    empty.CodeHash, // extAccount doesn't have CodeHash value
		Incarnation: 5,
	}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestEncodeAccountWithEmptyBalanceNonNilContractAndNotZeroIncarnation(t *testing.T) {
	t.Parallel()
	a := Account{
		Initialised: true,
		Nonce:       0,
		Balance:     *uint256.NewInt(0),
		Root:        common.HexToHash("123"),
		CodeHash:    common.HexToHash("123"),
		Incarnation: 1,
	}
	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}
func TestEncodeAccountWithEmptyBalanceAndNotZeroIncarnation(t *testing.T) {
	t.Parallel()
	a := Account{
		Initialised: true,
		Nonce:       0,
		Balance:     *uint256.NewInt(0),
		Incarnation: 1,
	}
	encodedAccount := SerialiseV3(&a)

	decodedAccount := Account{}
	if err := DeserialiseV3(&decodedAccount, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
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
	t.Helper()
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
		Root:        empty.RootHash,
		CodeHash:    empty.CodeHash,
		Incarnation: 4,
	}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestEmptyIncarnationForEmptyAccount2(t *testing.T) {
	t.Parallel()
	a := Account{}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)

}

func TestIncarnationWithNonEmptyAccount(t *testing.T) {
	t.Parallel()
	a := Account{
		Initialised: true,
		Nonce:       2,
		Balance:     *new(uint256.Int).SetUint64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
		Incarnation: 4,
	}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestIncarnationWithNoIncarnation(t *testing.T) {
	t.Parallel()
	a := Account{
		Initialised: true,
		Nonce:       2,
		Balance:     *new(uint256.Int).SetUint64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
		Incarnation: 0,
	}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)

}

func TestIncarnationWithInvalidEncodedAccount(t *testing.T) {

	var failingSlice = []byte{1, 12}

	if incarnation, err := DecodeIncarnationFromStorage(failingSlice); err == nil {
		t.Fatal("decoded the incarnation", incarnation, failingSlice)
	}

}

func isIncarnationEqual(t *testing.T, initialIncarnation uint64, decodedIncarnation uint64) {
	t.Helper()
	if initialIncarnation != decodedIncarnation {
		t.Fatal("Can't decode the incarnation", initialIncarnation, decodedIncarnation)
	}
}
