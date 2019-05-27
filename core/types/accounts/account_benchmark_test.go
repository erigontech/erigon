package accounts

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/crypto"
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
				Balance:  *new(big.Int),
				Root:     emptyRoot,     // extAccount doesn't have Root value
				CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(big.Int).SetInt64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:          2,
				Balance:        *new(big.Int).SetInt64(1000),
				Root:           common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash:       common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
				HasStorageSize: true,
				StorageSize:    0,
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeEIP2027",
			acc: &Account{
				Nonce:          2,
				Balance:        *new(big.Int).SetInt64(1000),
				Root:           common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash:       common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
				HasStorageSize: true,
				StorageSize:    1000,
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

			fmt.Fprint(ioutil.Discard, lengths)
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
				Balance:  *new(big.Int),
				Root:     emptyRoot,     // extAccount doesn't have Root value
				CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(big.Int).SetInt64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:          2,
				Balance:        *new(big.Int).SetInt64(1000),
				Root:           common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash:       common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
				HasStorageSize: true,
				StorageSize:    0,
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeEIP2027",
			acc: &Account{
				Nonce:          2,
				Balance:        *new(big.Int).SetInt64(1000),
				Root:           common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash:       common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
				HasStorageSize: true,
				StorageSize:    1000,
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

			fmt.Fprint(ioutil.Discard, lengths)
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
				Balance:  *new(big.Int),
				Root:     emptyRoot,     // extAccount doesn't have Root value
				CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(big.Int).SetInt64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:          2,
				Balance:        *new(big.Int).SetInt64(1000),
				Root:           common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash:       common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
				HasStorageSize: true,
				StorageSize:    0,
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeEIP2027",
			acc: &Account{
				Nonce:          2,
				Balance:        *new(big.Int).SetInt64(1000),
				Root:           common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash:       common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
				HasStorageSize: true,
				StorageSize:    1000,
			},
		},
	}

	b.ResetTimer()
	for _, test := range accountCases {
		test := test

		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				encodedLen := test.acc.EncodingLengthForStorage()
				b.StartTimer()

				encodedAccount := pool.GetBuffer(encodedLen)
				test.acc.EncodeForStorage(encodedAccount.B)
				pool.PutBuffer(encodedAccount)
			}
		})
	}

	b.StopTimer()

	for _, test := range accountCases {
		fmt.Fprint(ioutil.Discard, test.acc)
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
				Balance:  *new(big.Int),
				Root:     emptyRoot,     // extAccount doesn't have Root value
				CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(big.Int).SetInt64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:          2,
				Balance:        *new(big.Int).SetInt64(1000),
				Root:           common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash:       common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
				HasStorageSize: true,
				StorageSize:    0,
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeEIP2027",
			acc: &Account{
				Nonce:          2,
				Balance:        *new(big.Int).SetInt64(1000),
				Root:           common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash:       common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
				HasStorageSize: true,
				StorageSize:    1000,
			},
		},
	}

	b.ResetTimer()
	for _, test := range accountCases {
		test := test
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				encodedLen := test.acc.EncodingLengthForHashing()
				b.StartTimer()

				encodedAccount := pool.GetBuffer(encodedLen)
				test.acc.EncodeForHashing(encodedAccount.B)
				pool.PutBuffer(encodedAccount)
			}
		})
	}

	b.StopTimer()

	for _, test := range accountCases {
		fmt.Fprint(ioutil.Discard, test.acc)
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
				Balance:  *new(big.Int),
				Root:     emptyRoot,     // extAccount doesn't have Root value
				CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(big.Int).SetInt64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:          2,
				Balance:        *new(big.Int).SetInt64(1000),
				Root:           common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash:       common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
				HasStorageSize: true,
				StorageSize:    0,
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeEIP2027",
			acc: &Account{
				Nonce:          2,
				Balance:        *new(big.Int).SetInt64(1000),
				Root:           common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash:       common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
				HasStorageSize: true,
				StorageSize:    1000,
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
				test.acc.Balance.SetInt64(int64(i))

				encodedAccount := pool.GetBuffer(test.acc.EncodingLengthForStorage())
				test.acc.EncodeForStorage(encodedAccount.B)

				b.StartTimer()

				var decodedAccount Account
				if err := decodedAccount.DecodeForStorage(encodedAccount.B); err != nil {
					b.Fatal("cant decode the account", err, encodedAccount)
				}

				b.StopTimer()
				decodedAccounts = append(decodedAccounts, decodedAccount)
				b.StartTimer()

				pool.PutBuffer(encodedAccount)
			}
		})
	}

	b.StopTimer()
	for _, acc := range decodedAccounts {
		fmt.Fprint(ioutil.Discard, acc)
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
				Balance:  *new(big.Int),
				Root:     emptyRoot,     // extAccount doesn't have Root value
				CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(big.Int).SetInt64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:          2,
				Balance:        *new(big.Int).SetInt64(1000),
				Root:           common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash:       common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
				HasStorageSize: true,
				StorageSize:    0,
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeEIP2027",
			acc: &Account{
				Nonce:          2,
				Balance:        *new(big.Int).SetInt64(1000),
				Root:           common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash:       common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
				HasStorageSize: true,
				StorageSize:    1000,
			},
		},
	}

	b.ResetTimer()
	for _, test := range accountCases {
		test := test
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if err := test.acc.EncodeRLP(ioutil.Discard); err != nil {
					b.Fatal("cant encode the account", err, test)
				}
			}
		})
	}
}

func BenchmarkRLPDecodingAccount(b *testing.B) {
	accountCases := []struct {
		name string
		acc  *Account
	}{
		{
			name: "EmptyAccount",
			acc: &Account{
				Nonce:    0,
				Balance:  *new(big.Int),
				Root:     emptyRoot,     // extAccount doesn't have Root value
				CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
			},
		},

		{
			name: "AccountEncodeWithCode",
			acc: &Account{
				Nonce:    2,
				Balance:  *new(big.Int).SetInt64(1000),
				Root:     common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash: common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeHack",
			acc: &Account{
				Nonce:          2,
				Balance:        *new(big.Int).SetInt64(1000),
				Root:           common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash:       common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
				HasStorageSize: true,
				StorageSize:    0,
			},
		},

		{
			name: "AccountEncodeWithCodeWithStorageSizeEIP2027",
			acc: &Account{
				Nonce:          2,
				Balance:        *new(big.Int).SetInt64(1000),
				Root:           common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
				CodeHash:       common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3})),
				HasStorageSize: true,
				StorageSize:    1000,
			},
		},
	}

	var decodedAccounts []Account
	b.ResetTimer()
	for _, test := range accountCases {
		test := test
		b.Run(fmt.Sprint(test.name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				bufBytes := make([]byte, 0, test.acc.encodingLength(false))
				buf := bytes.NewBuffer(bufBytes)
				if err := test.acc.EncodeRLP(buf); err != nil {
					b.Fatal("cant encode the account", err, test)
				}

				accBytes := buf.Bytes()

				b.StartTimer()

				var decodedAccount Account
				if err := decodedAccount.DecodeForHashing(accBytes); err != nil {
					b.Fatal("cant decode the account", err, test)
				}

				b.StopTimer()
				decodedAccounts = append(decodedAccounts, decodedAccount)
				b.StartTimer()
			}
		})
	}

	b.StopTimer()
	for _, acc := range decodedAccounts {
		fmt.Fprint(ioutil.Discard, acc)
	}
}

func BenchmarkIsEmptyCodeHash(b *testing.B) {
	acc := &Account{
		Nonce:    0,
		Balance:  *new(big.Int),
		Root:     emptyRoot,     // extAccount doesn't have Root value
		CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
	}

	var isEmpty bool
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isEmpty = acc.IsEmptyCodeHash()
	}
	b.StopTimer()

	fmt.Fprint(ioutil.Discard, isEmpty)
}

func BenchmarkIsEmptyRoot(b *testing.B) {
	acc := &Account{
		Nonce:    0,
		Balance:  *new(big.Int),
		Root:     emptyRoot,     // extAccount doesn't have Root value
		CodeHash: emptyCodeHash, // extAccount doesn't have CodeHash value
	}

	var isEmpty bool
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isEmpty = acc.IsEmptyRoot()
	}
	b.StopTimer()

	fmt.Fprint(ioutil.Discard, isEmpty)
}
