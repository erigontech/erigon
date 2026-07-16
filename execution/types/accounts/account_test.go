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
	"bytes"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
)

func TestEmptyAccount(t *testing.T) {
	t.Parallel()
	a := Account{
		Nonce:       100,
		Balance:     uint256.Int{},
		Root:        empty.RootHash, // extAccount doesn't have Root value
		CodeHash:    EmptyCodeHash,  // extAccount doesn't have CodeHash value
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

func TestDeserialiseV3CodeHash(t *testing.T) {
	t.Parallel()
	balances := []uint256.Int{{}, *uint256.NewInt(1), *uint256.NewInt(1e18), *new(uint256.Int).Lsh(uint256.NewInt(1), 200)}
	nonces := []uint64{0, 1, 255, 1 << 40}
	codeHashes := []CodeHash{EmptyCodeHash, InternCodeHash(common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})))}
	incarnations := []uint64{0, 7}

	for _, nonce := range nonces {
		for i := range balances {
			for _, ch := range codeHashes {
				for _, inc := range incarnations {
					a := Account{Nonce: nonce, Balance: balances[i], CodeHash: ch, Incarnation: inc}
					enc := SerialiseV3(&a)

					var full Account
					if err := DeserialiseV3(&full, enc); err != nil {
						t.Fatal(err)
					}
					got := DeserialiseV3CodeHash(enc)
					if full.CodeHash.IsEmpty() {
						if got != nil {
							t.Fatalf("empty codeHash must extract as nil, got %x (acc %+v)", got, a)
						}
					} else {
						want := full.CodeHash.Value()
						if !bytes.Equal(got, want[:]) {
							t.Fatalf("extracted %x, want %x (acc %+v)", got, want, a)
						}
					}
				}
			}
		}
	}
}

func TestDeserialiseV3CodeHashMalformed(t *testing.T) {
	t.Parallel()
	a := Account{
		Nonce:       255,
		Balance:     *uint256.NewInt(1e18),
		CodeHash:    InternCodeHash(common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3}))),
		Incarnation: 4,
	}
	enc := SerialiseV3(&a)
	// [1+nonce][1+balance][1+codeHash]... — the codeHash field is complete at:
	codeHashEnd := 1 + int(enc[0]) + 1
	codeHashEnd += int(enc[codeHashEnd-1]) + 1
	codeHashEnd += int(enc[codeHashEnd-1])
	// Any truncation cutting into (or before) the codeHash must yield nil,
	// never an out-of-bounds read; beyond it the codeHash is extractable.
	for cut := 0; cut <= len(enc); cut++ {
		got := DeserialiseV3CodeHash(enc[:cut])
		if cut < codeHashEnd && got != nil {
			t.Fatalf("cut=%d (codeHash complete at %d): expected nil, got %x", cut, codeHashEnd, got)
		}
		if cut >= codeHashEnd && got == nil {
			t.Fatalf("cut=%d (codeHash complete at %d): expected hash, got nil", cut, codeHashEnd)
		}
	}
	if got := DeserialiseV3CodeHash(nil); got != nil {
		t.Fatalf("nil input: expected nil, got %x", got)
	}
	// A record claiming a non-32-byte codeHash is malformed for extraction.
	odd := append([]byte{0, 0, 31}, make([]byte, 31)...)
	if got := DeserialiseV3CodeHash(odd); got != nil {
		t.Fatalf("non-32-byte codeHash field: expected nil, got %x", got)
	}
	// Non-canonical records spelling out the no-code sentinels (canonical
	// SerialiseV3 writes length 0 instead) must extract as nil, matching
	// CodeHash.IsEmpty.
	for _, sentinel := range [][]byte{make([]byte, 32), empty.CodeHash[:]} {
		rec := append([]byte{0, 0, 32}, sentinel...)
		rec = append(rec, 0)
		if got := DeserialiseV3CodeHash(rec); got != nil {
			t.Fatalf("sentinel codeHash %x: expected nil, got %x", sentinel, got)
		}
	}
}

func TestAccountEncodeWithCode(t *testing.T) {
	t.Parallel()
	a := Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(0).SetUint64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
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
		Nonce:       2,
		Balance:     *uint256.NewInt(0).SetUint64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
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
		Nonce:       2,
		Balance:     *uint256.NewInt(0).SetUint64(1000),
		Root:        empty.RootHash, // extAccount doesn't have Root value
		CodeHash:    EmptyCodeHash,  // extAccount doesn't have CodeHash value
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
		Nonce:       0,
		Balance:     uint256.Int{},
		Root:        common.HexToHash("123"),
		CodeHash:    InternCodeHash(common.HexToHash("123")),
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
		Nonce:       0,
		Balance:     uint256.Int{},
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
		Nonce:       100,
		Balance:     uint256.Int{},
		Root:        empty.RootHash,
		CodeHash:    EmptyCodeHash,
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
		Nonce:       2,
		Balance:     *uint256.NewInt(0).SetUint64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
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
		Nonce:       2,
		Balance:     *uint256.NewInt(0).SetUint64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
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
