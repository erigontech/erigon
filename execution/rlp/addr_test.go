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

	"github.com/erigontech/erigon/common"
)

func TestStreamAddr(t *testing.T) {
	want := common.HexToAddress("0xdeadbeef00112233445566778899aabbccddeeff")
	enc, err := EncodeToBytes(want)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("bytes-stream", func(t *testing.T) {
		s := NewBytesStream(enc)
		defer PutStream(s)
		got, err := s.Addr()
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("got %x, want %x", got, want)
		}
	})

	t.Run("reader-stream", func(t *testing.T) {
		s := NewStream(bytes.NewReader(enc), uint64(len(enc)))
		got, err := s.Addr()
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("got %x, want %x", got, want)
		}
	})

	t.Run("bad-inputs", func(t *testing.T) {
		for _, in := range []string{
			"C0", // empty list
			"01", // single byte
			"80", // empty string
			"93deadbeef00112233445566778899aabbccddee",     // 19 bytes
			"95deadbeef00112233445566778899aabbccddeeff00", // 21 bytes
		} {
			s := NewBytesStream(unhex(in))
			_, err := s.Addr()
			PutStream(s)
			if err == nil {
				t.Fatalf("expected error for input %s", in)
			}
		}
	})

	t.Run("no-allocs", func(t *testing.T) {
		rdr := bytes.NewReader(enc)
		s := NewStream(rdr, uint64(len(enc)))
		defer PutStream(s)
		got := testing.AllocsPerRun(100, func() {
			rdr.Reset(enc)
			s.Reset(rdr, uint64(len(enc)))
			a, err := s.Addr()
			if err != nil || a != want {
				t.Fatalf("a=%x err=%v", a, err)
			}
		})
		if got != 0 {
			t.Fatalf("Addr allocated %v times/op, want 0", got)
		}
	})
}
