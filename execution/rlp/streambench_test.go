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

package rlp

import (
	"bytes"
	"testing"
)

// encodeStringRLP wraps a byte payload in a minimal RLP string envelope.
// Used by the Stream-level benchmarks to construct inputs of known shape.
func encodeStringRLP(payload []byte) []byte {
	if len(payload) == 1 && payload[0] < 0x80 {
		return []byte{payload[0]}
	}
	if len(payload) < 56 {
		return append([]byte{0x80 + byte(len(payload))}, payload...)
	}
	// long-string encoding
	lenBytes := []byte{}
	n := len(payload)
	for n > 0 {
		lenBytes = append([]byte{byte(n & 0xff)}, lenBytes...)
		n >>= 8
	}
	hdr := append([]byte{0xb7 + byte(len(lenBytes))}, lenBytes...)
	return append(hdr, payload...)
}

// BenchmarkStreamBytes_64B measures the cost of Stream.Bytes() for a
// medium-small string. The current implementation allocates a fresh
// []byte every call (decode.go:705).
func BenchmarkStreamBytes_64B(b *testing.B) {
	payload := bytes.Repeat([]byte{0xab}, 64)
	encoded := encodeStringRLP(payload)
	stream, done := NewStreamFromPool(bytes.NewReader(encoded), uint64(len(encoded)))
	defer done()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stream.Reset(bytes.NewReader(encoded), uint64(len(encoded)))
		out, err := stream.Bytes()
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}

// BenchmarkStreamBytes_4KB measures Stream.Bytes() for a larger payload.
// Realistic for Header.Extra or transaction calldata.
func BenchmarkStreamBytes_4KB(b *testing.B) {
	payload := bytes.Repeat([]byte{0xab}, 4096)
	encoded := encodeStringRLP(payload)
	stream, done := NewStreamFromPool(bytes.NewReader(encoded), uint64(len(encoded)))
	defer done()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stream.Reset(bytes.NewReader(encoded), uint64(len(encoded)))
		out, err := stream.Bytes()
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}

// BenchmarkStreamReadBytes_64B measures Stream.ReadBytes() — the existing
// no-alloc variant that decodes into a caller-supplied buffer. Compare
// against BenchmarkStreamBytes_64B to confirm the pattern that callers
// passing a destination buffer pay zero per-call allocs.
func BenchmarkStreamReadBytes_64B(b *testing.B) {
	payload := bytes.Repeat([]byte{0xab}, 64)
	encoded := encodeStringRLP(payload)
	stream, done := NewStreamFromPool(bytes.NewReader(encoded), uint64(len(encoded)))
	defer done()
	dst := make([]byte, 64)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stream.Reset(bytes.NewReader(encoded), uint64(len(encoded)))
		if err := stream.ReadBytes(dst); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeBytes_VsStreamDecode compares the two reflective entry
// points: the package-level DecodeBytes wrapper (one extra Reset() + pool
// checkout) vs. a hand-managed Stream.Decode loop.
//
// Decodes a tiny payload (10-byte string) so we measure the entry-point
// overhead, not the payload decode.
func BenchmarkDecodeBytes_Tiny(b *testing.B) {
	payload := []byte("hello rlp!")
	encoded := encodeStringRLP(payload)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var dst []byte
		if err := DecodeBytes(encoded, &dst); err != nil {
			b.Fatal(err)
		}
		_ = dst
	}
}
