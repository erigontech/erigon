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

import "testing"

type decodeAllocProbe struct {
	A uint64
	B uint64
	C uint64
}

// TestDecodeBytesAllocs guards the allocation cost of the pooled-Stream decode
// path. NewBytesStream keeps the input slice in the Stream's inline sliceRdr,
// so the borrow/return cycle must not allocate and the input must not escape.
// DecodeBytes then adds only the single allocation inherent to reflect-based
// decoding; a regression here (e.g. reintroducing an escaping reader) would
// push the counts above the asserted bounds.
func TestDecodeBytesAllocs(t *testing.T) {
	enc, err := EncodeToBytes(&decodeAllocProbe{A: 1, B: 2, C: 3})
	if err != nil {
		t.Fatal(err)
	}

	var out decodeAllocProbe
	if err := DecodeBytes(enc, &out); err != nil {
		t.Fatal(err)
	}
	if out != (decodeAllocProbe{A: 1, B: 2, C: 3}) {
		t.Fatalf("unexpected decode result: %+v", out)
	}

	if got := testing.AllocsPerRun(1000, func() {
		PutStream(NewBytesStream(enc))
	}); got != 0 {
		t.Fatalf("NewBytesStream/PutStream allocated %v times/op, want 0 (input must not escape)", got)
	}

	if got := testing.AllocsPerRun(1000, func() {
		out = decodeAllocProbe{}
		_ = DecodeBytes(enc, &out)
	}); got > 1 {
		t.Fatalf("DecodeBytes allocated %v times/op, want <= 1", got)
	}
}
