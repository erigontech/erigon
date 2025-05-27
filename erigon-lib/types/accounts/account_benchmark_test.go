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
	"fmt"
	"io"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/crypto"
)

func BenchmarkEncodingLengthForStorage(b *testing.B) {
	accountCases := []struct {
		name string
		acc  *Account
	}{
		{
			name: "EmptyAccount",
			acc: &Account{
				Nonce:    0,
				Balance:  *new(uint256.Int),
				Root:     empty.RootHash, // extAccount doesn't have Root value
				CodeHash: empty.CodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},
	}

	b.ResetTimer()
	for _, test := range accountCases {
		test := test
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			lengths := make([]uint, b.N)

			for i := 0; i < b.N; i++ {
				b.StartTimer()
				lengths[i] = test.acc.EncodingLengthForStorage()
				b.StopTimer()
			}

			fmt.Fprint(io.Discard, lengths)
		})
	}
}

func BenchmarkEncodingLengthForHashing(b *testing.B) {
	accountCases := []struct {
		name string
		acc  *Account
	}{
		{
			name: "EmptyAccount",
			acc: &Account{
				Nonce:    0,
				Balance:  *new(uint256.Int),
				Root:     empty.RootHash, // extAccount doesn't have Root value
				CodeHash: empty.CodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},
	}

	b.ResetTimer()
	for _, test := range accountCases {
		test := test
		b.Run(fmt.Sprint(test.name), func(bn *testing.B) {
			lengths := make([]uint, bn.N)

			for i := 0; i < bn.N; i++ {
				bn.StartTimer()
				lengths[i] = test.acc.EncodingLengthForHashing()
				bn.StopTimer()
			}

			fmt.Fprint(io.Discard, lengths)
		})

	}

}

func BenchmarkEncodingAccountForStorage(b *testing.B) {
	accountCases := []struct {
		name string
		acc  *Account
	}{
		{
			name: "EmptyAccount",
			acc: &Account{
				Nonce:    0,
				Balance:  *new(uint256.Int),
				Root:     empty.RootHash, // extAccount doesn't have Root value
				CodeHash: empty.CodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},
	}

	b.ResetTimer()
	for _, test := range accountCases {
		test := test

		//buf := make([]byte, test.acc.EncodingLengthForStorage())
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				SerialiseV3(test.acc)
				//test.acc.EncodeForStorage(buf) performance has degraded a bit because we are not using the same buf now
			}
		})
	}

	b.StopTimer()

	for _, test := range accountCases {
		fmt.Fprint(io.Discard, test.acc)
	}
}

func BenchmarkEncodingAccountForHashing(b *testing.B) {
	accountCases := []struct {
		name string
		acc  *Account
	}{
		{
			name: "EmptyAccount",
			acc: &Account{
				Nonce:    0,
				Balance:  *new(uint256.Int),
				Root:     empty.RootHash, // extAccount doesn't have Root value
				CodeHash: empty.CodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},
	}

	b.ResetTimer()
	for _, test := range accountCases {
		test := test
		buf := make([]byte, test.acc.EncodingLengthForStorage())
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				test.acc.EncodeForHashing(buf)
			}
		})
	}

	b.StopTimer()

	for _, test := range accountCases {
		fmt.Fprint(io.Discard, test.acc)
	}
}

func BenchmarkDecodingAccount(b *testing.B) {
	accountCases := []struct {
		name string
		acc  *Account
	}{
		{
			name: "EmptyAccount",
			acc: &Account{
				Nonce:    0,
				Balance:  *new(uint256.Int),
				Root:     empty.RootHash, // extAccount doesn't have Root value
				CodeHash: empty.CodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},
	}

	var decodedAccounts []Account
	b.ResetTimer()
	for _, test := range accountCases {
		test := test
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				println(test.name, i, b.N) //TODO: it just stucks w/o that print
				b.StopTimer()
				test.acc.Nonce = uint64(i)
				test.acc.Balance.SetUint64(uint64(i))
				encodedAccount := SerialiseV3(test.acc)

				b.StartTimer()

				var decodedAccount Account
				if err := DeserialiseV3(&decodedAccount, encodedAccount); err != nil {
					b.Fatal("cant decode the account", err, encodedAccount)
				}

				b.StopTimer()
				decodedAccounts = append(decodedAccounts, decodedAccount)
				b.StartTimer()
			}
		})

	}

	b.StopTimer()
	for _, acc := range decodedAccounts {
		fmt.Fprint(io.Discard, acc)
	}
}

func BenchmarkDecodingIncarnation(b *testing.B) { // V2 version of bench was a panic one
	accountCases := []struct {
		name string
		acc  *Account
	}{
		{
			name: "EmptyAccount",
			acc: &Account{
				Nonce:    0,
				Balance:  *new(uint256.Int),
				Root:     empty.RootHash, // extAccount doesn't have Root value
				CodeHash: empty.CodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},
	}

	var decodedIncarnations []uint64
	b.ResetTimer()
	for _, test := range accountCases {
		test := test
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				println(test.name, i, b.N) //TODO: it just stucks w/o that print
				b.StopTimer()

				test.acc.Nonce = uint64(i)
				test.acc.Balance.SetUint64(uint64(i))
				encodedAccount := SerialiseV3(test.acc)

				b.StartTimer()

				decodedAcc := Account{}
				if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
					b.Fatal("can't decode the incarnation", err, encodedAccount)
				}

				b.StopTimer()
				decodedIncarnations = append(decodedIncarnations, decodedAcc.Incarnation)

				b.StartTimer()
			}
		})
	}

	b.StopTimer()
	for _, incarnation := range decodedIncarnations {
		fmt.Fprint(io.Discard, incarnation)
	}

}

func BenchmarkRLPEncodingAccount(b *testing.B) {
	accountCases := []struct {
		name string
		acc  *Account
	}{
		{
			name: "EmptyAccount",
			acc: &Account{
				Nonce:    0,
				Balance:  *new(uint256.Int),
				Root:     empty.RootHash, // extAccount doesn't have Root value
				CodeHash: empty.CodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},
	}

	b.ResetTimer()
	for _, test := range accountCases {
		test := test
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if err := test.acc.EncodeRLP(io.Discard); err != nil {
					b.Fatal("cant encode the account", err, test)
				}
			}
		})
	}
}

func BenchmarkIsEmptyCodeHash(b *testing.B) {
	acc := &Account{
		Nonce:    0,
		Balance:  *new(uint256.Int),
		Root:     empty.RootHash, // extAccount doesn't have Root value
		CodeHash: empty.CodeHash, // extAccount doesn't have CodeHash value
	}

	var isEmpty bool
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isEmpty = acc.IsEmptyCodeHash()
	}
	b.StopTimer()

	fmt.Fprint(io.Discard, isEmpty)
}

func BenchmarkIsEmptyRoot(b *testing.B) {
	acc := &Account{
		Nonce:    0,
		Balance:  *new(uint256.Int),
		Root:     empty.RootHash, // extAccount doesn't have Root value
		CodeHash: empty.CodeHash, // extAccount doesn't have CodeHash value
	}

	var isEmpty bool
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isEmpty = acc.IsEmptyRoot()
	}
	b.StopTimer()

	fmt.Fprint(io.Discard, isEmpty)
}
