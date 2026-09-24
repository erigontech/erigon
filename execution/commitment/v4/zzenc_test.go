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

package v4

import (
	"context"
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
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

func TestZZAccountLeafFormats(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	const n = 200000
	const contractShare = 12

	var v4Bytes, v3Bytes, v3NoRootBytes int
	var eoaN, ctrN int
	for i := range n {
		var u *commitment.Update
		var root []byte
		if i%100 < contractShare {
			u, root = sizeContract(i, rnd)
			ctrN++
		} else {
			u, root = sizeEOA(i, rnd)
			eoaN++
		}
		acc := v3Account(u)
		v4Bytes += len(encodeAccountLeaf(u, root, nil))
		v3Bytes += len(encodeAccountLeafV3(acc, root))
		v3NoRootBytes += len(accounts.SerialiseV3(acc))
	}
	t.Logf("n=%d (EOA %d / contract %d)", n, eoaN, ctrN)
	t.Logf("  v4 packed          %9d B  avg %6.3f B/leaf", v4Bytes, float64(v4Bytes)/n)
	t.Logf("  SerialiseV3+root   %9d B  avg %6.3f B/leaf  (%+.2f B/leaf vs v4)",
		v3Bytes, float64(v3Bytes)/n, float64(v3Bytes-v4Bytes)/n)
	t.Logf("  SerialiseV3 alone  %9d B  avg %6.3f B/leaf  (no storage root)",
		v3NoRootBytes, float64(v3NoRootBytes)/n)
}

func TestZZAccountLeafFormatsEOAOnly(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	const n = 200000
	var v4Bytes, v3Bytes int
	for range n {
		u, _ := sizeEOA(0, rnd)
		acc := v3Account(u)
		v4Bytes += len(encodeAccountLeaf(u, empty.RootHash[:], nil))
		v3Bytes += len(encodeAccountLeafV3(acc, empty.RootHash[:]))
	}
	t.Logf("EOA-only n=%d", n)
	t.Logf("  v4 packed        avg %6.3f B/leaf", float64(v4Bytes)/n)
	t.Logf("  SerialiseV3      avg %6.3f B/leaf  (%+.2f B/leaf)",
		float64(v3Bytes)/n, float64(v3Bytes-v4Bytes)/n)
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

	b.Run("encode/v4packed", func(b *testing.B) {
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
	b.Run("decode/v4packed", func(b *testing.B) {
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
	b.Run("v4packed", func(b *testing.B) {
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

func contextBackground() context.Context { return context.Background() }

func TestZZRecordByteBudget(t *testing.T) {
	ctxb := contextBackground()
	for _, shape := range []string{"accounts", "storage"} {
		entries := benchEntries(shape, 100000)
		c := newParityContext()
		tr := &Trie{}
		tr.ResetContext(c)
		u := benchUpdates2(t, entries)
		if _, err := tr.Process(ctxb, u, "", nil, commitmentWarmup()); err != nil {
			t.Fatal(err)
		}
		var keyB, valB, recs int
		for k, v := range c.branches {
			keyB += len(k)
			valB += len(v)
			recs++
		}
		nLeaves := 100000
		t.Logf("%s/100k: %d records, keys %d B, values %d B, total %d B (%.1f B/record)",
			shape, recs, keyB, valB, keyB+valB, float64(keyB+valB)/float64(recs))
		t.Logf("   account-leaf format delta at +1.77 B/leaf = %+d B (%+.2f%% of total)",
			int(1.77*float64(nLeaves)), 100*1.77*float64(nLeaves)/float64(keyB+valB))
		tr.Release()
	}
}
