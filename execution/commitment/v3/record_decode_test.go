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

package v3

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/stretchr/testify/require"
)

func TestRecordCodec(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E24/RecordDecodeHeaderForms", func(t *testing.T) {
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
					data:  recordFixture(0, 3, 0x0003, 0, 0, nil, nil, nil),
					depth: 3,
				},
				{
					name:  "child extension",
					flags: hdrHasChildExt,
					data: recordFixture(hdrHasChildExt, 3, 0x0001, 0, 0x0001, nil,
						map[int][]byte{0: extFixture([]byte{1, 2})}, nil),
					depth: 3,
				},
				{
					name:  "leaf child",
					flags: 0,
					data: recordFixture(0, 3, 0x0001, 0x0001, 0, nil, nil,
						map[int]leafFixture{0: {suffix: suffix, value: []byte{0x42}}}),
					depth: 3,
				},
				{
					name:  "self extension",
					flags: hdrHasSelfExt,
					data: recordFixture(hdrHasSelfExt, 0, 0x0001, 0, 0,
						extFixture([]byte{3, 4, 5}), nil, nil),
					depth: 0,
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					require.NoError(t, Validate(tt.data, tt.depth))
					r := Record{data: tt.data, depth: tt.depth}
					require.Equal(t, byte(recordFormat), r.data[0]&hdrFormatMask)
				})
			}
		}},
		{"E24-E25/RecordDecodeHeaderBitCombinations", func(t *testing.T) {
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
						require.Equal(t, wantErr, err != nil)
					})
					continue
				}

				child := uint16(1)
				ext := uint16(0)
				extAt := make(map[int][]byte)
				if flags&hdrHasChildExt != 0 {
					ext |= 1
					extAt[0] = extFixture([]byte{1})
				}
				self := []byte(nil)
				if flags&hdrHasSelfExt != 0 {
					self = extFixture([]byte{1})
				}
				data := recordFixture(flags, depth, child, 0, ext, self, extAt, nil)
				wantErr := flags&hdrHasEmb != 0
				t.Run(string(rune('a'+combination)), func(t *testing.T) {
					err := Validate(data, depth)
					require.Equal(t, wantErr, err != nil)
				})
			}
		}},
		{"E26/RecordDecodeAccessors", func(t *testing.T) {
			depth := 3
			suffix := packPath(bytes.Repeat([]byte{1}, 60), nil)
			extension := extFixture([]byte{6, 7, 8})
			second := extFixture([]byte{9})
			data := recordFixture(hdrHasChildExt, depth, 0x000f, 0x000a, 0x0005, nil,
				map[int][]byte{0: extension, 2: second}, map[int]leafFixture{
					1: {suffix: suffix, value: []byte{0x11, 0x12}},
					3: {suffix: suffix, value: []byte{0x31}},
				})
			require.NoError(t, Validate(data, depth))
			r := Record{data: data, depth: depth}
			l := r.layout()
			require.Equal(t, uint16(0x000f), l.child)
			require.Equal(t, uint16(0x000a), l.leaf)
			require.Equal(t, uint16(0x0005), l.ext)
			for _, nib := range []int{0, 2} {
				require.Len(t, r.slotAt(l, nib), 32)
				require.Equal(t, byte(nib), r.slotAt(l, nib)[0])
			}
			for _, nib := range []int{1, 3} {
				require.Nil(t, r.slotAt(l, nib))
			}
			require.Equal(t, extension, r.extAt(l, 0))
			require.Equal(t, second, r.extAt(l, 2))
			require.Nil(t, r.extAt(l, 1))
			for _, tc := range []struct {
				nib   int
				value []byte
			}{{1, []byte{0x11, 0x12}}, {3, []byte{0x31}}} {
				gotSuffix, gotValue := r.leafAt(l, tc.nib)
				require.Equal(t, suffix, gotSuffix)
				require.Equal(t, tc.value, gotValue)
			}

			root := recordFixture(hdrHasSelfExt, 0, 1, 0, 0, extFixture([]byte{1, 2}), nil, nil)
			require.Equal(t, extFixture([]byte{1, 2}), (Record{data: root, depth: 0}).SelfExt())
		}},
		{"E28/RecordDecodeLeafRoot", func(t *testing.T) {
			key := bytes.Repeat([]byte{0xab}, 32)
			value := []byte{1, 2, 3}
			data := append([]byte{hdrIsLeafRoot}, key...)
			data = append(data, byte(len(value)))
			data = append(data, value...)
			require.NoError(t, Validate(data, 0))
			gotKey, gotValue := data[1:33], data[34:]
			require.Equal(t, key, gotKey)
			require.Equal(t, value, gotValue)
		}},
		{"E27/RecordDecodeNoAllocations", func(t *testing.T) {
			data := recordFixture(hdrHasChildExt, 3, 0x000f, 0x000a, 0x0001, nil,
				map[int][]byte{0: extFixture([]byte{6, 7})}, map[int]leafFixture{
					1: {suffix: packPath(bytes.Repeat([]byte{1}, 60), nil), value: []byte{1}},
					3: {suffix: packPath(bytes.Repeat([]byte{1}, 60), nil), value: []byte{3}},
				})
			r := Record{data: data, depth: 3}
			require.Zero(t, testing.AllocsPerRun(100, func() {
				l := r.layout()
				_ = r.slotAt(l, 0)
				_ = r.extAt(l, 0)
				_, _ = r.leafAt(l, 1)
			}))
		}},
		{"E25/RecordDecodeRejectsMalformedRecords", func(t *testing.T) {
			valid := recordFixture(0, 3, 1, 0, 0, nil, nil, nil)
			tests := []struct {
				name  string
				data  []byte
				depth int
				want  error
			}{
				{name: "truncated body", data: valid[:len(valid)-1], depth: 3, want: ErrRecordTruncated},
				{name: "leaf outside child mask", data: recordFixture(0, 3, 1, 2, 0, nil, nil, nil), depth: 3, want: ErrRecordMasks},
				{name: "extension overlaps leaf", data: recordFixture(hdrHasChildExt, 3, 1, 1, 1, nil, map[int][]byte{0: extFixture([]byte{1})}, nil), depth: 3, want: ErrRecordMasks},
				{name: "leaf root and self extension", data: append([]byte{hdrIsLeafRoot | hdrHasSelfExt}, make([]byte, 33)...), depth: 0, want: ErrRecordRoot},
				{name: "self extension has two children", data: recordFixture(hdrHasSelfExt, 0, 3, 0, 0, extFixture([]byte{1}), nil, nil), depth: 0, want: ErrRecordRoot},
				{name: "short extension trailer", data: recordFixture(hdrHasChildExt, 3, 1, 0, 1, nil, map[int][]byte{0: {3, 0x12}}, nil), depth: 3, want: ErrRecordTruncated},
				{name: "embedded flag", data: recordFixture(hdrHasEmb, 3, 1, 0, 0, nil, nil, nil), depth: 3, want: ErrRecordFormat},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					require.ErrorIs(t, Validate(tt.data, tt.depth), tt.want)
				})
			}
		}},
		{"E26/EncodeRecordRoundTrip", func(t *testing.T) {
			suffix := packPath(bytes.Repeat([]byte{1}, 60), nil)
			hash := bytes.Repeat([]byte{0xab}, 32)
			tests := []struct {
				name  string
				depth int
				path  []byte
				spec  commitmenttest.RecordSpec
				check func(*testing.T, Record)
			}{
				{
					name:  "branch with leaf",
					depth: 3,
					spec:  commitmenttest.RecordSpec{ChildMask: 3, LeafMask: 2, Hashes: [16][]byte{0: hash}, Leaves: [16]commitmenttest.Leaf{1: {Suffix: suffix, Value: []byte{0x42}}}},
					check: func(t *testing.T, r Record) {
						l := r.layout()
						require.Equal(t, uint16(3), l.child)
						require.Equal(t, uint16(2), l.leaf)
						require.Equal(t, hash, r.slotAt(l, 0))
						gotSuffix, gotValue := r.leafAt(l, 1)
						require.Equal(t, suffix, gotSuffix)
						require.Equal(t, []byte{0x42}, gotValue)
					},
				},
				{
					name:  "child extension",
					depth: 3,
					spec:  commitmenttest.RecordSpec{ChildMask: 1 << 4, Hashes: [16][]byte{4: hash}, Extensions: [16][]byte{4: {3, 0x23, 0x40}}},
					check: func(t *testing.T, r Record) {
						l := r.layout()
						require.Equal(t, uint16(1<<4), l.ext)
						require.Equal(t, []byte{3, 0x23, 0x40}, r.extAt(l, 4))
						require.Equal(t, hash, r.slotAt(l, 4))
					},
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					n := materializeNode(tt.path, planeAccount, &tt.spec)
					data := encodeRecord(n, tt.depth, bytes.Repeat([]byte{0xff}, 8))
					require.NoError(t, Validate(data, tt.depth))
					tt.check(t, Record{data: data, depth: tt.depth})
				})
			}
		}},
		{"E28/EncodeRecordRootForms", func(t *testing.T) {
			hash := bytes.Repeat([]byte{0x11}, 32)
			tests := []struct {
				name  string
				path  []byte
				spec  commitmenttest.RecordSpec
				check func(*testing.T, []byte)
			}{
				{
					name: "leaf root",
					spec: commitmenttest.RecordSpec{ChildMask: 1 << 4, LeafMask: 1 << 4, Leaves: [16]commitmenttest.Leaf{4: {Suffix: packPath(bytes.Repeat([]byte{2}, 63), nil), Value: []byte{1, 2}}}},
					check: func(t *testing.T, data []byte) {
						require.Equal(t, byte(hdrIsLeafRoot), data[0])
						require.NoError(t, Validate(data, 0))
						key, value := data[1:33], data[34:]
						require.Equal(t, packPath(append([]byte{4}, bytes.Repeat([]byte{2}, 63)...), nil), key)
						require.Equal(t, []byte{1, 2}, value)
					},
				},
				{
					name: "extension root",
					path: []byte{1, 2},
					spec: commitmenttest.RecordSpec{ChildMask: 1 << 3, Hashes: [16][]byte{3: hash}},
					check: func(t *testing.T, data []byte) {
						require.Equal(t, byte(hdrHasSelfExt), data[0])
						require.NoError(t, Validate(data, 0))
						r := Record{data: data, depth: 0}
						require.Equal(t, []byte{2, 0x12}, r.SelfExt())
						require.Equal(t, hash, r.slotAt(r.layout(), 3))
					},
				},
				{
					name: "branch root",
					spec: commitmenttest.RecordSpec{ChildMask: 1<<3 | 1<<9, Hashes: [16][]byte{3: hash, 9: bytes.Repeat([]byte{0x22}, 32)}},
					check: func(t *testing.T, data []byte) {
						require.Equal(t, byte(recordFormat), data[0])
						require.NoError(t, Validate(data, 0))
						require.Equal(t, uint16(1<<3|1<<9), Record{data: data, depth: 0}.layout().child)
					},
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					tt.check(t, encodeRecord(materializeNode(tt.path, planeAccount, &tt.spec), 0, nil))
				})
			}
		}},
		{"E28/EncodeRecordTombstone", func(t *testing.T) {
			dst := []byte{1, 2, 3}
			require.Empty(t, encodeRecord(fork(nil), 7, dst))
		}},
		{"E27-E28/EncodeRecordReusesDestination", func(t *testing.T) {
			n := fork(nil)
			n.setStoredChild(2, bytes.Repeat([]byte{0x33}, 32), nil)
			dst := make([]byte, 0, 256)
			data := encodeRecord(n, 7, dst)
			require.NoError(t, Validate(data, 7))
			data = encodeRecord(fork(nil), 7, data)
			require.Empty(t, data)
		}},
		{"E28/EncodeRecordRejectsUnrepresentableRoots", func(t *testing.T) {
			require.Panics(t, func() {
				n := fork(nil)
				n.setStoredChild(1, bytes.Repeat([]byte{1}, 32), nil)
				encodeRecord(n, 0, nil)
			})
			require.Panics(t, func() {
				n := fork([]byte{1})
				n.setStoredChild(1, bytes.Repeat([]byte{1}, 32), []byte{2})
				encodeRecord(n, 0, nil)
			})
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

type leafFixture struct {
	suffix []byte
	value  []byte
}

func recordFixture(flags byte, depth int, child, leaf, ext uint16, self []byte, extAt map[int][]byte, leafAt map[int]leafFixture) []byte {
	spec := commitmenttest.RecordSpec{Flags: flags, ChildMask: child, LeafMask: leaf, ExtensionMask: ext, SelfExtension: self}
	for nib, path := range extAt {
		spec.Extensions[nib] = path
	}
	for nib, entry := range leafAt {
		spec.Leaves[nib] = commitmenttest.Leaf{Suffix: entry.suffix, Value: entry.value}
	}
	return commitmenttest.Records([]commitmenttest.RecordSpec{spec})[0].Data
}

func extFixture(path []byte) []byte {
	return append([]byte{byte(len(path))}, packPath(path, nil)...)
}
