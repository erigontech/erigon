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
	"sync/atomic"
	"testing"
	"unique"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
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
				Balance:  uint256.Int{},
				Root:     empty.RootHash, // extAccount doesn't have Root value
				CodeHash: EmptyCodeHash,  // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *uint256.NewInt(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *uint256.NewInt(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
			},
		},
	}

	b.ResetTimer()
	for _, test := range accountCases {
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			var length uint
			for b.Loop() {
				length = test.acc.EncodingLengthForStorage()
			}
			fmt.Fprint(io.Discard, length)
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
				Balance:  uint256.Int{},
				Root:     empty.RootHash, // extAccount doesn't have Root value
				CodeHash: EmptyCodeHash,  // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *uint256.NewInt(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *uint256.NewInt(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
			},
		},
	}

	b.ResetTimer()
	for _, test := range accountCases {
		b.Run(fmt.Sprint(test.name), func(bn *testing.B) {
			var length uint
			for bn.Loop() {
				length = test.acc.EncodingLengthForHashing()
			}
			fmt.Fprint(io.Discard, length)
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
				Balance:  uint256.Int{},
				Root:     empty.RootHash, // extAccount doesn't have Root value
				CodeHash: EmptyCodeHash,  // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *uint256.NewInt(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *uint256.NewInt(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
			},
		},
	}

	b.ResetTimer()
	for _, test := range accountCases {
		//buf := make([]byte, test.acc.EncodingLengthForStorage())
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for b.Loop() {
				SerialiseV3(test.acc)
				// test.acc.EncodeForStorage(buf) performance has degraded a bit because we are not using the same buf now
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
				Balance:  uint256.Int{},
				Root:     empty.RootHash, // extAccount doesn't have Root value
				CodeHash: EmptyCodeHash,  // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *uint256.NewInt(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *uint256.NewInt(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
			},
		},
	}

	b.ResetTimer()
	for _, test := range accountCases {
		buf := make([]byte, test.acc.EncodingLengthForHashing())
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for b.Loop() {
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
				Balance:  uint256.Int{},
				Root:     empty.RootHash, // extAccount doesn't have Root value
				CodeHash: EmptyCodeHash,  // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *uint256.NewInt(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *uint256.NewInt(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
			},
		},
	}

	var decodedAccounts []Account
	b.ResetTimer()
	for _, test := range accountCases {
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for i := 0; b.Loop(); i++ {
				println(test.name, i, b.N) // TODO: it just stucks w/o that print
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
				Balance:  uint256.Int{},
				Root:     empty.RootHash, // extAccount doesn't have Root value
				CodeHash: EmptyCodeHash,  // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *uint256.NewInt(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *uint256.NewInt(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
			},
		},
	}

	var decodedIncarnations []uint64
	b.ResetTimer()
	for _, test := range accountCases {
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for i := 0; b.Loop(); i++ {
				println(test.name, i, b.N) // TODO: it just stucks w/o that print
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
				Balance:  uint256.Int{},
				Root:     empty.RootHash, // extAccount doesn't have Root value
				CodeHash: EmptyCodeHash,  // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *uint256.NewInt(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *uint256.NewInt(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
			},
		},
	}

	b.ResetTimer()
	for _, test := range accountCases {
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for b.Loop() {
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
		Balance:  uint256.Int{},
		Root:     empty.RootHash, // extAccount doesn't have Root value
		CodeHash: EmptyCodeHash,  // extAccount doesn't have CodeHash value
	}

	var isEmpty bool

	for b.Loop() {
		isEmpty = acc.IsEmptyCodeHash()
	}
	b.StopTimer()

	fmt.Fprint(io.Discard, isEmpty)
}

func BenchmarkIsEmptyRoot(b *testing.B) {
	acc := &Account{
		Nonce:    0,
		Balance:  uint256.Int{},
		Root:     empty.RootHash, // extAccount doesn't have Root value
		CodeHash: EmptyCodeHash,  // extAccount doesn't have CodeHash value
	}

	var isEmpty bool

	for b.Loop() {
		isEmpty = acc.IsEmptyRoot()
	}
	b.StopTimer()

	fmt.Fprint(io.Discard, isEmpty)
}

// BenchmarkInternKeyParallel compares the memo with unique.Make under
// concurrency: one key shared by all goroutines, and a 1024-key working set.
func BenchmarkInternKeyParallel(b *testing.B) {
	for _, n := range []int{1, 1024} {
		keys := make([]common.Hash, n)
		for i := range keys {
			keys[i] = common.BigToHash(uint256.NewInt(uint64(i) + 1).ToBig())
		}
		for _, f := range []struct {
			name   string
			intern func(common.Hash) StorageKey
		}{
			{"memo", InternKey},
			{"unique", func(k common.Hash) StorageKey { return StorageKey(unique.Make(k)) }},
		} {
			b.Run(fmt.Sprintf("keys=%d/%s", n, f.name), func(b *testing.B) {
				live := make([]StorageKey, n)
				for i, k := range keys {
					f.intern(k) // the memo keeps a handle from the second sighting on
					live[i] = f.intern(k)
				}
				var start atomic.Uint64
				b.RunParallel(func(pb *testing.PB) {
					i := int(start.Add(7919))
					for pb.Next() {
						f.intern(keys[i&(n-1)])
						i++
					}
				})
				_ = live
			})
		}
	}
}

func BenchmarkAccountCopy(b *testing.B) {
	a := Account{Nonce: 1, Balance: *uint256.NewInt(2), Root: empty.RootHash, CodeHash: EmptyCodeHash, Incarnation: 5, PrevIncarnation: 6}
	var c Account
	for b.Loop() {
		c.Copy(&a)
	}
}

var accountSink *Account

func BenchmarkAccountSelfCopy(b *testing.B) {
	a := Account{Nonce: 1, Balance: *uint256.NewInt(2), Root: empty.RootHash, CodeHash: EmptyCodeHash, Incarnation: 5, PrevIncarnation: 6}
	for b.Loop() {
		accountSink = a.SelfCopy()
	}
}
