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

package v4

import (
	"bytes"
	"errors"
	"testing"
)

type leafFixture struct {
	suffix []byte
	value  []byte
}

func recordFixture(flags byte, depth int, child, leaf, ext, emb uint16, self []byte, extAt map[int][]byte, embAt map[int][]byte, leafAt map[int]leafFixture) []byte {
	if flags&hdrIsLeafRoot != 0 {
		return append([]byte{flags}, append(make([]byte, 32), 0)...)
	}
	rec := []byte{flags}
	if flags&hdrHasSelfExt != 0 {
		rec = append(rec, self...)
	}
	mask := make([]byte, 4)
	mask[0] = byte(child >> 8)
	mask[1] = byte(child)
	mask[2] = byte(leaf >> 8)
	mask[3] = byte(leaf)
	rec = append(rec, mask...)
	if flags&hdrHasChildExt != 0 {
		rec = append(rec, byte(ext>>8), byte(ext))
	}
	if flags&hdrHasEmb != 0 {
		rec = append(rec, byte(emb>>8), byte(emb))
	}
	tree := child &^ leaf &^ emb
	for nib := range 16 {
		if tree&(uint16(1)<<nib) == 0 {
			continue
		}
		for range 32 {
			rec = append(rec, byte(nib))
		}
	}
	for nib := range 16 {
		if ext&(uint16(1)<<nib) != 0 {
			rec = append(rec, extAt[nib]...)
		}
	}
	for nib := range 16 {
		if emb&(uint16(1)<<nib) != 0 {
			value := embAt[nib]
			rec = append(rec, byte(len(value)))
			rec = append(rec, value...)
		}
	}
	for nib := range 16 {
		if leaf&(uint16(1)<<nib) != 0 {
			entry := leafAt[nib]
			rec = append(rec, entry.suffix...)
			rec = append(rec, byte(len(entry.value)))
			rec = append(rec, entry.value...)
		}
	}
	return rec
}

func extFixture(path []byte) []byte {
	return append([]byte{byte(len(path))}, packPath(path, nil)...)
}

func TestRecordDecodeHeaderForms(t *testing.T) {
	suffix := packPath(bytes.Repeat([]byte{1}, 60), nil)
	tests := []struct {
		name  string
		flags byte
		data  []byte
		depth int
	}{
		{
			name:  "branch",
			flags: 0,
			data:  recordFixture(0, 3, 0x0003, 0, 0, 0, nil, nil, nil, nil),
			depth: 3,
		},
		{
			name:  "child extension",
			flags: hdrHasChildExt,
			data: recordFixture(hdrHasChildExt, 3, 0x0001, 0, 0x0001, 0, nil,
				map[int][]byte{0: extFixture([]byte{1, 2})}, nil, nil),
			depth: 3,
		},
		{
			name:  "embedded child",
			flags: hdrHasEmb,
			data: recordFixture(hdrHasEmb, 3, 0x0001, 0, 0, 0x0001, nil, nil,
				map[int][]byte{0: {0xaa, 0xbb}}, nil),
			depth: 3,
		},
		{
			name:  "leaf child",
			flags: 0,
			data: recordFixture(0, 3, 0x0001, 0x0001, 0, 0, nil, nil, nil,
				map[int]leafFixture{0: {suffix: suffix, value: []byte{0x42}}}),
			depth: 3,
		},
		{
			name:  "self extension",
			flags: hdrHasSelfExt,
			data: recordFixture(hdrHasSelfExt, 0, 0x0001, 0, 0, 0,
				extFixture([]byte{3, 4, 5}), nil, nil, nil),
			depth: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Validate(tt.data, tt.depth); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			r := NewRecord(tt.data, tt.depth)
			if r.data[0]&hdrFormatMask != recordFormat {
				t.Fatalf("format nibble: %#x", r.data[0]&hdrFormatMask)
			}
		})
	}
}

func TestRecordDecodeHeaderBitCombinations(t *testing.T) {
	for combination := range 16 {
		flags := byte(combination << 4)
		depth := 3
		if flags&(hdrHasSelfExt|hdrIsLeafRoot) != 0 {
			depth = 0
		}
		if flags&hdrIsLeafRoot != 0 {
			data := append([]byte{flags}, make([]byte, 32)...)
			data = append(data, 0)
			wantErr := combination != 8
			t.Run(string(rune('a'+combination)), func(t *testing.T) {
				err := Validate(data, depth)
				if (err != nil) != wantErr {
					t.Fatalf("Validate error = %v, want error %t", err, wantErr)
				}
			})
			continue
		}

		child := uint16(0)
		ext := uint16(0)
		emb := uint16(0)
		leaf := uint16(0)
		extAt := make(map[int][]byte)
		embAt := make(map[int][]byte)
		leafAt := make(map[int]leafFixture)
		if flags&hdrHasChildExt != 0 {
			child |= 1
			ext |= 1
			extAt[0] = extFixture([]byte{1})
		}
		if flags&hdrHasEmb != 0 {
			child |= 2
			emb |= 2
			embAt[1] = []byte{0xaa}
		}
		if child == 0 {
			child = 1
		}
		self := []byte(nil)
		if flags&hdrHasSelfExt != 0 {
			self = extFixture([]byte{1})
		}
		data := recordFixture(flags, depth, child, leaf, ext, emb, self, extAt, embAt, leafAt)
		wantErr := flags&hdrHasSelfExt != 0 && bitsCount(child) != 1
		t.Run(string(rune('a'+combination)), func(t *testing.T) {
			err := Validate(data, depth)
			if (err != nil) != wantErr {
				t.Fatalf("Validate error = %v, want error %t", err, wantErr)
			}
		})
	}
}

func bitsCount(mask uint16) int {
	count := 0
	for mask != 0 {
		mask &= mask - 1
		count++
	}
	return count
}

func TestRecordDecodeAccessors(t *testing.T) {
	depth := 3
	suffix := packPath(bytes.Repeat([]byte{1}, 60), nil)
	extension := extFixture([]byte{6, 7, 8})
	embedded := []byte{0xc1, 0x01}
	data := recordFixture(hdrHasChildExt|hdrHasEmb, depth, 0x000f, 0x000a, 0x0001, 0x0004, nil,
		map[int][]byte{0: extension}, map[int][]byte{2: embedded}, map[int]leafFixture{
			1: {suffix: suffix, value: []byte{0x11, 0x12}},
			3: {suffix: suffix, value: []byte{0x31}},
		})
	if err := Validate(data, depth); err != nil {
		t.Fatal(err)
	}
	r := NewRecord(data, depth)
	if r.ChildMask() != 0x000f || r.LeafMask() != 0x000a || r.ExtMask() != 0x0001 || r.EmbMask() != 0x0004 {
		t.Fatalf("masks: child=%x leaf=%x ext=%x emb=%x", r.ChildMask(), r.LeafMask(), r.ExtMask(), r.EmbMask())
	}
	if got := r.SlotAt(0); len(got) != 32 || got[0] != 0 {
		t.Fatalf("slot 0: %x", got)
	}
	if r.SlotAt(1) != nil || r.SlotAt(2) != nil || r.SlotAt(3) != nil {
		t.Fatalf("unexpected non-tree slot")
	}
	if got := r.ExtAt(0); !bytes.Equal(got, extension) {
		t.Fatalf("extension: %x", got)
	}
	if got := r.EmbAt(2); !bytes.Equal(got, embedded) {
		t.Fatalf("embedded: %x", got)
	}
	gotSuffix, gotValue := r.LeafAt(1)
	if !bytes.Equal(gotSuffix, suffix) || !bytes.Equal(gotValue, []byte{0x11, 0x12}) {
		t.Fatalf("leaf 1: suffix=%x value=%x", gotSuffix, gotValue)
	}
	if gotSuffix, gotValue := r.LeafAt(3); !bytes.Equal(gotSuffix, suffix) || !bytes.Equal(gotValue, []byte{0x31}) {
		t.Fatalf("leaf 3: suffix=%x value=%x", gotSuffix, gotValue)
	}

	root := recordFixture(hdrHasSelfExt, 0, 1, 0, 0, 0, extFixture([]byte{1, 2}), nil, nil, nil)
	if got := NewRecord(root, 0).SelfExt(); !bytes.Equal(got, extFixture([]byte{1, 2})) {
		t.Fatalf("self extension: %x", got)
	}
}

func TestRecordDecodeLeafRoot(t *testing.T) {
	key := bytes.Repeat([]byte{0xab}, 32)
	value := []byte{1, 2, 3}
	data := append([]byte{hdrIsLeafRoot}, key...)
	data = append(data, byte(len(value)))
	data = append(data, value...)
	if err := Validate(data, 0); err != nil {
		t.Fatal(err)
	}
	gotKey, gotValue := NewRecord(data, 0).LeafRootBody()
	if !bytes.Equal(gotKey, key) || !bytes.Equal(gotValue, value) {
		t.Fatalf("leaf root: key=%x value=%x", gotKey, gotValue)
	}
}

func TestRecordDecodeNoAllocations(t *testing.T) {
	data := recordFixture(hdrHasChildExt|hdrHasEmb, 3, 0x000f, 0x000a, 0x0001, 0x0004, nil,
		map[int][]byte{0: extFixture([]byte{6, 7})}, map[int][]byte{2: {0xaa}}, map[int]leafFixture{
			1: {suffix: packPath(bytes.Repeat([]byte{1}, 60), nil), value: []byte{1}},
		})
	r := NewRecord(data, 3)
	if allocations := testing.AllocsPerRun(100, func() {
		_ = r.ChildMask()
		_ = r.LeafMask()
		_ = r.ExtMask()
		_ = r.EmbMask()
		_ = r.SlotAt(0)
		_ = r.ExtAt(0)
		_ = r.EmbAt(2)
		_, _ = r.LeafAt(1)
	}); allocations != 0 {
		t.Fatalf("record accessors allocate: %f", allocations)
	}
}

func TestRecordDecodeRejectsMalformedRecords(t *testing.T) {
	valid := recordFixture(0, 3, 1, 0, 0, 0, nil, nil, nil, nil)
	tests := []struct {
		name  string
		data  []byte
		depth int
		want  error
	}{
		{name: "truncated body", data: valid[:len(valid)-1], depth: 3, want: ErrRecordTruncated},
		{name: "leaf outside child mask", data: recordFixture(0, 3, 1, 2, 0, 0, nil, nil, nil, nil), depth: 3, want: ErrRecordMasks},
		{name: "extension overlaps leaf", data: recordFixture(hdrHasChildExt, 3, 1, 1, 1, 0, nil, map[int][]byte{0: extFixture([]byte{1})}, nil, nil), depth: 3, want: ErrRecordMasks},
		{name: "leaf root and self extension", data: append([]byte{hdrIsLeafRoot | hdrHasSelfExt}, make([]byte, 33)...), depth: 0, want: ErrRecordRoot},
		{name: "self extension has two children", data: recordFixture(hdrHasSelfExt, 0, 3, 0, 0, 0, extFixture([]byte{1}), nil, nil, nil), depth: 0, want: ErrRecordRoot},
		{name: "short extension trailer", data: recordFixture(hdrHasChildExt, 3, 1, 0, 1, 0, nil, map[int][]byte{0: {3, 0x12}}, nil, nil), depth: 3, want: ErrRecordTruncated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Validate(tt.data, tt.depth); !errors.Is(err, tt.want) {
				t.Fatalf("Validate error = %v, want %v", err, tt.want)
			}
		})
	}
}
