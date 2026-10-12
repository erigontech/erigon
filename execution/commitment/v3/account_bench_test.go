// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for
// more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package v3

import (
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest"
)

func v3Account(u *commitment.Update) *accounts.Account {
	a := new(accounts.Account)
	a.Nonce = u.Nonce
	a.Balance = u.Balance
	a.CodeHash = accounts.InternCodeHash(u.CodeHash)
	return a
}

func encodeAccountLeafV3(a *accounts.Account, storageRoot []byte) []byte {
	enc := accounts.SerialiseV3(a)
	if !isEmptyStorageRoot(storageRoot) {
		enc = append(enc, storageRoot...)
	}
	return enc
}

func BenchmarkZZAccountLeafFormats(b *testing.B) {
	rnd := rand.New(rand.NewSource(7))
	u, _ := sizeEOA(1, rnd)
	acc := v3Account(u)
	root := make([]byte, 32)
	rnd.Read(root)

	packed := encodeAccountLeaf(u, root, nil)
	v3enc := encodeAccountLeafV3(acc, root)
	buf := make([]byte, 0, 128)

	b.Run("encode/v3packed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			buf = encodeAccountLeaf(u, root, buf[:0])
		}
	})
	b.Run("encode/SerialiseV3", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = encodeAccountLeafV3(acc, root)
		}
	})
	b.Run("decode/v3packed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, _, _, _, err := decodeAccountLeaf(packed); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decode/DeserialiseV3", func(b *testing.B) {
		var a accounts.Account
		b.ReportAllocs()
		for range b.N {
			if err := accounts.DeserialiseV3(&a, v3enc); err != nil {
				b.Fatal(err)
			}
		}
	})
	_ = empty.RootHash
}

func BenchmarkZZContractDecode(b *testing.B) {
	rnd := rand.New(rand.NewSource(9))
	u, root := sizeContract(3, rnd)
	acc := v3Account(u)
	packed := encodeAccountLeaf(u, root, nil)
	v3enc := encodeAccountLeafV3(acc, root)
	b.Run("v3packed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, _, _, _, err := decodeAccountLeaf(packed); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("DeserialiseV3", func(b *testing.B) {
		var a accounts.Account
		b.ReportAllocs()
		for range b.N {
			if err := accounts.DeserialiseV3(&a, v3enc); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func sizeEOA(i int, rnd *rand.Rand) (*commitment.Update, []byte) {
	value, root := commitmenttest.SizedAccount(i, false, rnd)
	return testAccountUpdate(value), root
}

func sizeContract(i int, rnd *rand.Rand) (*commitment.Update, []byte) {
	value, root := commitmenttest.SizedAccount(i, true, rnd)
	return testAccountUpdate(value), root
}

func BenchmarkZZAccountLeafCodec(b *testing.B) {
	rnd := rand.New(rand.NewSource(7))
	u, _ := sizeEOA(1, rnd)
	packed := encodeAccountLeaf(u, empty.RootHash[:], nil)
	buf := make([]byte, 0, 128)

	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			buf = encodeAccountLeaf(u, empty.RootHash[:], buf[:0])
		}
	})
	b.Run("decode+consensusRLP", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			nonce, bal, ch, sr, err := decodeAccountLeaf(packed)
			if err != nil {
				b.Fatal(err)
			}
			buf = accountConsensusRLP(nonce, &bal, sr, ch, buf[:0])
		}
	})
	b.Run("consensusRLP_only", func(b *testing.B) {
		b.ReportAllocs()
		bal := u.Balance
		for range b.N {
			buf = accountConsensusRLP(u.Nonce, &bal, empty.RootHash[:], empty.CodeHash[:], buf[:0])
		}
	})
}
