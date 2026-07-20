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

func encodeStringRLP(payload []byte) []byte {
	if len(payload) == 1 && payload[0] < 0x80 {
		return []byte{payload[0]}
	}
	if len(payload) < 56 {
		return append([]byte{0x80 + byte(len(payload))}, payload...)
	}
	lenBytes := []byte{}
	n := len(payload)
	for n > 0 {
		lenBytes = append([]byte{byte(n & 0xff)}, lenBytes...)
		n >>= 8
	}
	hdr := append([]byte{0xb7 + byte(len(lenBytes))}, lenBytes...)
	return append(hdr, payload...)
}

func BenchmarkStreamBytes_64B(b *testing.B) {
	payload := bytes.Repeat([]byte{0xab}, 64)
	encoded := encodeStringRLP(payload)
	stream := NewStreamFromPool(bytes.NewReader(encoded), uint64(len(encoded)))
	defer PutStream(stream)
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

func BenchmarkStreamBytes_4KB(b *testing.B) {
	payload := bytes.Repeat([]byte{0xab}, 4096)
	encoded := encodeStringRLP(payload)
	stream := NewStreamFromPool(bytes.NewReader(encoded), uint64(len(encoded)))
	defer PutStream(stream)
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

func BenchmarkStreamReadBytes_64B(b *testing.B) {
	payload := bytes.Repeat([]byte{0xab}, 64)
	encoded := encodeStringRLP(payload)
	stream := NewStreamFromPool(bytes.NewReader(encoded), uint64(len(encoded)))
	defer PutStream(stream)
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

type benchCap struct {
	Name    string
	Version uint
}

type benchHandshake struct {
	Version    uint64
	Name       string
	Caps       []benchCap
	ListenPort uint64
	ID         []byte
}

func BenchmarkDecodeStringFields(b *testing.B) {
	encoded, err := EncodeToBytes(&benchHandshake{
		Version:    5,
		Name:       "erigon/v3.6.0-dev/linux-amd64/go1.25.0",
		Caps:       []benchCap{{"eth", 68}, {"eth", 69}, {"snap", 1}, {"wit", 0}},
		ListenPort: 30303,
		ID:         bytes.Repeat([]byte{0xab}, 64),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var dst benchHandshake
		if err := DecodeBytes(encoded, &dst); err != nil {
			b.Fatal(err)
		}
		_ = dst
	}
}
