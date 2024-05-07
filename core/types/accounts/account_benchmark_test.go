package accounts

import (
	"fmt"
	"io"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/crypto"
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
				Root:     emptyRoot,     // extAccount doesn't have Root value
				CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
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
				Root:     emptyRoot,     // extAccount doesn't have Root value
				CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
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
				Root:     emptyRoot,     // extAccount doesn't have Root value
				CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},
	}

	b.ResetTimer()
	for _, test := range accountCases {
		test := test

		buf := make([]byte, test.acc.EncodingLengthForStorage())
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				test.acc.EncodeForStorage(buf)
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
				Root:     emptyRoot,     // extAccount doesn't have Root value
				CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
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
				Root:     emptyRoot,     // extAccount doesn't have Root value
				CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},
	}

	var decodedAccounts []Account
	b.ResetTimer()
	for _, test := range accountCases {
		test := test
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				test.acc.Nonce = uint64(i)
				test.acc.Balance.SetUint64(uint64(i))
				encodedAccount := make([]byte, test.acc.EncodingLengthForStorage())

				test.acc.EncodeForStorage(encodedAccount)

				b.StartTimer()

				var decodedAccount Account
				if err := decodedAccount.DecodeForStorage(encodedAccount); err != nil {
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

func BenchmarkDecodingIncarnation(b *testing.B) {
	accountCases := []struct {
		name string
		acc  *Account
	}{
		{
			name: "EmptyAccount",
			acc: &Account{
				Nonce:    0,
				Balance:  *new(uint256.Int),
				Root:     emptyRoot,     // extAccount doesn't have Root value
				CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},
	}

	var decodedIncarnations []uint64
	b.ResetTimer()
	for _, test := range accountCases {
		test := test
		encodedAccount := make([]byte, test.acc.EncodingLengthForStorage())
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				test.acc.Nonce = uint64(i)
				test.acc.Balance.SetUint64(uint64(i))
				test.acc.EncodeForStorage(encodedAccount)

				b.StartTimer()

				if _, err := DecodeIncarnationFromStorage(encodedAccount); err != nil {
					b.Fatal("can't decode the incarnation", err, encodedAccount)
				}

				decodedIncarnation, _ := DecodeIncarnationFromStorage(encodedAccount)

				b.StopTimer()
				decodedIncarnations = append(decodedIncarnations, decodedIncarnation)

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
				Root:     emptyRoot,     // extAccount doesn't have Root value
				CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(uint256.Int).SetUint64(1000),
				Root:     libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: libcommon.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
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
		Root:     emptyRoot,     // extAccount doesn't have Root value
		CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
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
		Root:     emptyRoot,     // extAccount doesn't have Root value
		CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
	}

	var isEmpty bool
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isEmpty = acc.IsEmptyRoot()
	}
	b.StopTimer()

	fmt.Fprint(io.Discard, isEmpty)
}
