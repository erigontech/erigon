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

package nibbles

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodeKeyV3Vectors(t *testing.T) {
	tests := []struct {
		name string
		path []byte
		want []byte
	}{
		{name: "empty", path: nil, want: []byte{0x00}},
		{name: "one_nibble", path: []byte{0xa}, want: []byte{0xfa}},
		{name: "two_nibbles", path: []byte{0x2, 0xf}, want: []byte{0x2f, 0x00}},
		{name: "three_nibbles", path: []byte{0x2, 0xf, 0xb}, want: []byte{0x2f, 0xfb}},
		{name: "four_nibbles", path: []byte{0x2, 0xf, 0xb, 0x3}, want: []byte{0x2f, 0xb3, 0x00}},
		{name: "127_nibbles", path: repeatByte(0xa, 127), want: append(repeatByte(0xaa, 63), 0xfa)},
		{name: "128_nibbles", path: repeatByte(0xa, 128), want: append(repeatByte(0xaa, 64), 0x00)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EncodeKeyV3(tt.path); !bytes.Equal(got, tt.want) {
				t.Fatalf("EncodeKeyV3(%x) = %x, want %x", tt.path, got, tt.want)
			}
		})
	}
}

func TestDecodeKeyV3RoundTrip(t *testing.T) {
	for depth := 0; depth <= MaxPathNibbles; depth++ {
		path := make([]byte, depth)
		for i := range path {
			path[i] = byte((i*7 + depth*3) & 0x0f)
		}

		key := EncodeKeyV3(path)
		got, err := DecodeKeyV3(key)
		if err != nil {
			t.Fatalf("depth %d: DecodeKeyV3(%x) returned error: %v", depth, key, err)
		}
		if !bytes.Equal(got, path) {
			t.Fatalf("depth %d: DecodeKeyV3(%x) = %x, want %x", depth, key, got, path)
		}
		if want := depth/2 + 1; len(key) != want {
			t.Fatalf("depth %d: encoded length = %d, want %d", depth, len(key), want)
		}
	}
}

func TestDecodeKeyV3Errors(t *testing.T) {
	tooLong := make([]byte, MaxPathNibbles/2+2)
	oddTooLong := append(make([]byte, MaxPathNibbles/2), 0xf0)
	tests := []struct {
		name string
		key  []byte
		want error
	}{
		{name: "empty", key: nil, want: ErrV3KeyLength},
		{name: "too_long", key: tooLong, want: ErrV3KeyLength},
		{name: "odd_depth_over_max", key: oddTooLong, want: ErrV3KeyLength},
		{name: "illegal_terminal", key: []byte{0x12, 0x01}, want: ErrV3KeyTerminal},
		{name: "child_terminal", key: []byte{0x12, 0x80}, want: ErrV3KeyTerminal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeKeyV3(tt.key)
			if !errors.Is(err, tt.want) {
				t.Fatalf("DecodeKeyV3(%x) error = %v, want %v", tt.key, err, tt.want)
			}
			if got != nil {
				t.Fatalf("DecodeKeyV3(%x) returned %x with error, want nil", tt.key, got)
			}
		})
	}
}

func TestChildKeyV3(t *testing.T) {
	for depth := 0; depth <= MaxPathNibbles; depth++ {
		path := make([]byte, depth)
		for i := range path {
			path[i] = byte((i + 5) & 0x0f)
		}
		nodeKey := EncodeKeyV3(path)

		for nibble := range 16 {
			childKey := ChildKeyV3(nodeKey, byte(nibble))
			if !IsChildKeyV3(childKey) {
				t.Fatalf("depth %d nibble %x: %x is not a V3 child key", depth, nibble, childKey)
			}
			if !IsChildKeyForNodeV3(nodeKey, childKey) {
				t.Fatalf("depth %d nibble %x: %x is not a child of %x", depth, nibble, childKey, nodeKey)
			}
			if !IsChildKeyAtDepthV3(childKey, depth) {
				t.Fatalf("depth %d nibble %x: %x has the wrong child-key length", depth, nibble, childKey)
			}
			if got := ChildNibbleV3(childKey); got != byte(nibble) {
				t.Fatalf("depth %d: ChildNibbleV3(%x) = %x, want %x", depth, childKey, got, nibble)
			}
			if want := ChildKeyLenForDepth(depth); len(childKey) != want {
				t.Fatalf("depth %d: child key length = %d, want %d", depth, len(childKey), want)
			}
		}
	}
}

func TestChildRangeBoundsV3(t *testing.T) {
	for _, path := range [][]byte{nil, {0x1}, {0x1, 0x2}, {0x1, 0x2, 0xf}, repeatByte(0xa, 128)} {
		nodeKey := EncodeKeyV3(path)
		lo, hi := ChildRangeBoundsV3(nodeKey)
		if want := ChildKeyV3(nodeKey, 0); !bytes.Equal(lo, want) {
			t.Fatalf("path %x: lower bound = %x, want %x", path, lo, want)
		}
		wantHi := append(append([]byte(nil), nodeKey...), 0x90)
		if !bytes.Equal(hi, wantHi) {
			t.Fatalf("path %x: upper bound = %x, want %x", path, hi, wantHi)
		}
		for nibble := range 16 {
			key := ChildKeyV3(nodeKey, byte(nibble))
			if bytes.Compare(lo, key) > 0 || bytes.Compare(key, hi) >= 0 {
				t.Fatalf("path %x nibble %x: child key %x is outside [%x, %x)", path, nibble, key, lo, hi)
			}
		}
	}
}

func TestV3TerminalClasses(t *testing.T) {
	for terminal := 0; terminal <= 0xff; terminal++ {
		key := []byte{0x00, byte(terminal)}
		_, nodeErr := DecodeKeyV3(key)
		isNode := nodeErr == nil
		isChild := IsChildKeyV3(key)

		switch {
		case terminal == 0 || terminal >= 0xf0:
			if !isNode || isChild {
				t.Fatalf("terminal 0x%02x: node=%t child=%t, want node only", terminal, isNode, isChild)
			}
		case terminal >= 0x80 && terminal <= 0x8f:
			if isNode || !isChild {
				t.Fatalf("terminal 0x%02x: node=%t child=%t, want child only", terminal, isNode, isChild)
			}
		default:
			if isNode || isChild {
				t.Fatalf("terminal 0x%02x: node=%t child=%t, want neither", terminal, isNode, isChild)
			}
		}
	}
}

func TestV3ChildRangeRejectsIntrudingDescendants(t *testing.T) {
	tests := []struct {
		name       string
		path       []byte
		descendant []byte
	}{
		{
			name:       "even_parent_subtree_0_0_8",
			path:       []byte{0x1, 0x2},
			descendant: []byte{0x1, 0x2, 0x0, 0x0, 0x8, 0x0},
		},
		{
			name:       "odd_parent_subtree_15_8_8",
			path:       []byte{0x1, 0x2, 0xf},
			descendant: []byte{0x1, 0x2, 0xf, 0xf, 0x8, 0x8},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodeKey := EncodeKeyV3(tt.path)
			intruder := ChildKeyV3(EncodeKeyV3(tt.descendant), 0)
			lo, hi := ChildRangeBoundsV3(nodeKey)
			if bytes.Compare(intruder, lo) < 0 || bytes.Compare(intruder, hi) >= 0 {
				t.Fatalf("intruding key %x is not inside [%x, %x)", intruder, lo, hi)
			}
			if !IsChildKeyV3(intruder) {
				t.Fatalf("intruding key %x should have a valid child-record suffix", intruder)
			}
			if IsChildKeyForNodeV3(nodeKey, intruder) {
				t.Fatalf("intruding key %x passed the exact direct-child predicate for %x", intruder, nodeKey)
			}
			if IsChildKeyAtDepthV3(intruder, len(tt.path)) {
				t.Fatalf("intruding key %x passed the exact depth predicate", intruder)
			}
		})
	}
}

func TestEncodeKeyV3LengthComparedWithV2(t *testing.T) {
	for depth := 0; depth <= MaxPathNibbles; depth++ {
		path := make([]byte, depth)
		keyV2 := EncodeKeyV2(path)
		keyV3 := EncodeKeyV3(path)
		want := len(keyV2)
		if depth&1 == 1 {
			want--
		}
		if len(keyV3) != want {
			t.Fatalf("depth %d: V3 key length = %d, V2 length = %d, want V3 length %d", depth, len(keyV3), len(keyV2), want)
		}
	}
}

func TestEncodeKeyV3Panics(t *testing.T) {
	tests := []struct {
		name string
		path []byte
	}{
		{name: "invalid_nibble", path: []byte{0x10}},
		{name: "overlong", path: make([]byte, MaxPathNibbles+1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("EncodeKeyV3(%x) did not panic", tt.path)
				}
			}()
			_ = EncodeKeyV3(tt.path)
		})
	}
}

func FuzzEncodeDecodeKeyV3(f *testing.F) {
	for _, depth := range []int{0, 1, 2, 3, 63, 64, 127, 128} {
		f.Add(depth, byte(0xa))
	}

	f.Fuzz(func(t *testing.T, depth int, value byte) {
		depth %= MaxPathNibbles + 1
		if depth < 0 {
			depth = -depth
		}
		path := make([]byte, depth)
		for i := range path {
			path[i] = byte(int(value)+i) & 0x0f
		}
		key := EncodeKeyV3(path)
		got, err := DecodeKeyV3(key)
		if err != nil {
			t.Fatalf("DecodeKeyV3(%x) returned error: %v", key, err)
		}
		if !bytes.Equal(got, path) {
			t.Fatalf("DecodeKeyV3(%x) = %x, want %x", key, got, path)
		}
	})
}
