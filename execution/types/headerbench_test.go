// Copyright 2026 The Erigon Authors
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

package types

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/execution/rlp"
)

// Testdata format: repeated [u32 LE length][rlp bytes]. Produced by the
// extract_headers helper described in PERFBENCH.md.
func loadHeadersRLP(tb testing.TB, name string) [][]byte {
	tb.Helper()
	path := filepath.Join("testdata", name)
	raw, err := os.ReadFile(path)
	if err != nil {
		tb.Fatalf("read %s: %v", path, err)
	}
	var out [][]byte
	for len(raw) >= 4 {
		n := binary.LittleEndian.Uint32(raw[:4])
		raw = raw[4:]
		if uint32(len(raw)) < n {
			tb.Fatalf("truncated record in %s", path)
		}
		out = append(out, raw[:n])
		raw = raw[n:]
	}
	if len(out) == 0 {
		tb.Fatalf("no headers in %s", path)
	}
	return out
}

func BenchmarkDecodeHeader_PreLondon(b *testing.B) {
	headers := loadHeadersRLP(b, "headers-mainnet-00099.rlp")
	hRLP := headers[0]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var h Header
		if err := rlp.DecodeBytes(hRLP, &h); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeHeader_London(b *testing.B) {
	headers := loadHeadersRLP(b, "headers-mainnet-01894.rlp")
	hRLP := headers[0]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var h Header
		if err := rlp.DecodeBytes(hRLP, &h); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeHeader_Loop_PreLondon(b *testing.B) {
	headers := loadHeadersRLP(b, "headers-mainnet-00099.rlp")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, hRLP := range headers {
			var h Header
			if err := rlp.DecodeBytes(hRLP, &h); err != nil {
				b.Fatal(err)
			}
			_ = h.ParentHash
		}
	}
}

func BenchmarkDecodeHeader_Loop_London(b *testing.B) {
	headers := loadHeadersRLP(b, "headers-mainnet-01894.rlp")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, hRLP := range headers {
			var h Header
			if err := rlp.DecodeBytes(hRLP, &h); err != nil {
				b.Fatal(err)
			}
			_ = h.ParentHash
		}
	}
}

func BenchmarkEncodeHeader_London(b *testing.B) {
	headers := loadHeadersRLP(b, "headers-mainnet-01894.rlp")
	var h Header
	if err := rlp.DecodeBytes(headers[0], &h); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out, err := rlp.EncodeToBytes(&h)
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}

func BenchmarkHeaderHash_Mutable(b *testing.B) {
	headers := loadHeadersRLP(b, "headers-mainnet-01894.rlp")
	var h Header
	if err := rlp.DecodeBytes(headers[0], &h); err != nil {
		b.Fatal(err)
	}
	h.mutable = true
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = h.Hash()
	}
}

func BenchmarkHeaderHash_Memoized(b *testing.B) {
	headers := loadHeadersRLP(b, "headers-mainnet-01894.rlp")
	var h Header
	if err := rlp.DecodeBytes(headers[0], &h); err != nil {
		b.Fatal(err)
	}
	h.mutable = false
	_ = h.Hash() // warm the cache
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = h.Hash()
	}
}

func BenchmarkDecodeHeader_PreLondon_TypesAPI(b *testing.B) {
	headers := loadHeadersRLP(b, "headers-mainnet-00099.rlp")
	hRLP := headers[0]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var h Header
		if err := DecodeHeader(hRLP, &h); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeHeader_London_TypesAPI(b *testing.B) {
	headers := loadHeadersRLP(b, "headers-mainnet-01894.rlp")
	hRLP := headers[0]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var h Header
		if err := DecodeHeader(hRLP, &h); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeHeader_Loop_PreLondon_Hoisted(b *testing.B) {
	headers := loadHeadersRLP(b, "headers-mainnet-00099.rlp")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var h Header
		for _, hRLP := range headers {
			if err := DecodeHeader(hRLP, &h); err != nil {
				b.Fatal(err)
			}
			_ = h.ParentHash
		}
	}
}

func BenchmarkDecodeHeader_Loop_London_Hoisted(b *testing.B) {
	headers := loadHeadersRLP(b, "headers-mainnet-01894.rlp")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var h Header
		for _, hRLP := range headers {
			if err := DecodeHeader(hRLP, &h); err != nil {
				b.Fatal(err)
			}
			_ = h.ParentHash
		}
	}
}
